/*
    Copyright (c) 2022 Intel Corporation
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
        http://www.apache.org/licenses/LICENSE-2.0
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/


#include "DummyCostModel.h"

#include "QueryEngine/Dispatchers/DefaultExecutionPolicy.h"

namespace CostModel {

DummyCostModel::DummyCostModel(std::unique_ptr<Connector> _connector, std::unique_ptr<ExtrapolationModel> _extrapolation) 
    : CostModel(std::move(_connector), std::move(_extrapolation)) {};


std::unique_ptr<policy::ExecutionPolicy> DummyCostModel::predict(size_t sizeInBytes) {
    return std::make_unique<policy::FragmentIDAssignmentExecutionPolicy>(ExecutorDeviceType::CPU);
}

}