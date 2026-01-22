# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_subdirectory(src)

get_source(nes-single-node-worker SINGLE_NODE_WORKER_SOURCE_FILES)

add_library(nes-single-node-worker-lib STATIC ${SINGLE_NODE_WORKER_SOURCE_FILES})

find_package(etcd-cpp-api CONFIG REQUIRED)

target_include_directories(nes-single-node-worker-lib
        PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        PRIVATE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/private>
)

target_link_libraries(nes-single-node-worker-lib
        PUBLIC nes-logical-operators nes-single-node-worker-interface nes-node-engine
        PRIVATE etcd-cpp-api
)

add_tests_if_enabled(tests)