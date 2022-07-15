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

#pragma once

#include <memory>

#include "DataSources/DataSource.h"
#include "ExtrapolationModels/ExtrapolationModel.h"
#include "Measurements.h"

#include "QueryEngine/Dispatchers/ExecutionPolicy.h"
#include "QueryEngine/CompilationOptions.h"

namespace CostModel {

using TemplatePredictions = std::unordered_map<AnalyticalTemplate, TimePrediction>;
using DevicePredictions = std::unordered_map<ExecutorDeviceType, TemplatePredictions>;

class CostModel {
public:
    CostModel(std::unique_ptr<DataSource> _dataSource, std::unique_ptr<ExtrapolationModel> _extrapolation);
    virtual ~CostModel() = default;

    void calibrate();
    virtual std::unique_ptr<policy::ExecutionPolicy> predict(size_t sizeInBytes) = 0;

protected:
    std::unique_ptr<DataSource> dataSource;
    std::unique_ptr<ExtrapolationModel> extrapolation;

    // TODO: naming
    DeviceMeasurements dm;
    DevicePredictions dp;

    std::vector<AnalyticalTemplate> templates = {
        GroupBy,
        Join,
        Scan,
        Reduce
    };

    std::vector<ExecutorDeviceType> devices = {
        ExecutorDeviceType::CPU,
        ExecutorDeviceType::GPU
    };
};


}
