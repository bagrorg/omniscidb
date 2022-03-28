/*
 * Copyright 2022 OmniSci, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "DataMgr/MemoryLevel.h"
#include "QueryEngine/Dispatchers/RRExecutionPolicy.h"

namespace policy {
SchedulingAssignment RoundRobinExecutionPolicy::scheduleSingleFragment(
    const Fragmenter_Namespace::FragmentInfo& fragment,
    size_t frag_id,
    size_t frag_num) const {
  auto device_type = frag_id % 2 ? ExecutorDeviceType::GPU : ExecutorDeviceType::CPU;
  auto memory_level = device_type == ExecutorDeviceType::GPU ? Data_Namespace::GPU_LEVEL
                                                             : Data_Namespace::CPU_LEVEL;
  unsigned device_id = fragment.deviceIds[static_cast<unsigned>(memory_level)];
  return {device_type, device_id};
}
std::vector<SchedulingAssignment> RoundRobinExecutionPolicy::scheduleAll(
    const TableFragments* fragments) const {
  CHECK(false);
}
}  // namespace policy