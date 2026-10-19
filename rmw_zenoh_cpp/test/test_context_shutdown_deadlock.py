# Copyright 2026 rmw_zenoh contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Exercise the real shutdown and graph callback bodies with a blocking subscriber.

The mock undeclare waits for its graph callback, just as Zenoh does. Compiling
the production method bodies keeps the lock ordering under test without needing
a router or relying on thread timing to reproduce the race.
"""
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SOURCE = Path(__file__).resolve().parents[1] / 'src/detail/rmw_context_impl_s.cpp'
if len(sys.argv) == 2:
    SOURCE = Path(sys.argv.pop())


def method(source, signature):
    start = source.index(signature)
    opening = source.index('{', start)
    depth = 1
    end = opening + 1
    while depth:
        depth += (source[end] == '{') - (source[end] == '}')
        end += 1
    return source[start:end]


HARNESS = r'''
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
using rmw_ret_t = int;
constexpr int RMW_RET_OK = 0, RMW_RET_ERROR = 1, Z_OK = 0;
#define RMW_ZENOH_LOG_ERROR_NAMED(...)
#define RMW_ZENOH_LOG_WARN_NAMED(...)
int rmw_trigger_guard_condition(void *) {return RMW_RET_OK;}
struct CallbackState {
  std::promise<void> undeclaring, completed;
  std::shared_future<void> callback_done = completed.get_future().share();
  int undeclare_count = 0;
};
namespace zenoh {
using ZResult = int;
enum class SampleKind {Z_SAMPLE_KIND_PUT, Z_SAMPLE_KIND_DELETE};
struct Sample {SampleKind get_kind() const {return SampleKind::Z_SAMPLE_KIND_PUT;}};
template<class T> struct Subscriber {
  explicit Subscriber(CallbackState * value) : state(value) {}
  CallbackState * state;
  void undeclare(ZResult * result) && {
    ++state->undeclare_count;
    state->undeclaring.set_value();
    state->callback_done.wait();
    *result = Z_OK;
  }
};
}
struct GraphCache {
  void parse_put(const std::string &) {}
  void parse_del(const std::string &) {}
};
struct Data {
  std::recursive_mutex mutex_;
  bool is_shutdown_ = false;
  std::shared_ptr<int> session_ = std::make_shared<int>(1);
  std::optional<zenoh::Subscriber<void>> graph_subscriber_;
  std::shared_ptr<GraphCache> graph_cache_ = std::make_shared<GraphCache>();
  std::unique_ptr<int> graph_guard_condition_ = std::make_unique<int>(1);
  @SHUTDOWN@
  @CALLBACK@
};
int main() {
  CallbackState state;
  Data data;
  data.graph_subscriber_.emplace(&state);
  std::thread callback([&] {
    state.undeclaring.get_future().wait();
    data.update_graph_cache(zenoh::Sample{}, "graph_key");
    state.completed.set_value();
  });
  if (data.shutdown() != RMW_RET_OK) {return 1;}
  callback.join();
  if (data.shutdown() != RMW_RET_OK) {return 2;}
  return state.undeclare_count == 1 && data.is_shutdown_ && !data.session_ ? 0 : 3;
}
'''


class ContextShutdownTest(unittest.TestCase):
    def test_graph_callback_can_finish_during_undeclare(self):
        source = SOURCE.read_text()
        program = HARNESS.replace('@SHUTDOWN@', method(source, 'rmw_ret_t shutdown()'))
        program = program.replace('@CALLBACK@', method(source, 'void update_graph_cache('))
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'test.cpp'
            path.write_text(program)
            binary = Path(tmp) / 'test'
            subprocess.run(['c++', '-std=c++17', '-pthread', str(path), '-o', str(binary)],
                           check=True, capture_output=True, timeout=10)
            result = subprocess.run([str(binary)], capture_output=True, timeout=3)
            self.assertEqual(result.returncode, 0, result.stderr.decode())


if __name__ == '__main__':
    unittest.main()
