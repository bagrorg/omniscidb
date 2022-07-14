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

#include "CostModel.h"

namespace CostModel {


CostModel::CostModel(std::unique_ptr<DataSource> _dataSource, std::unique_ptr<ExtrapolationModel> _extrapolation) 
        : dataSource(std::move(_dataSource)), extrapolation(std::move(_extrapolation)) {}


void CostModel::calibrate() {
    dp.clear();
    dm = dataSource->getMeasurements(templates);

    for (const auto &dmEntry: dm) {
        ExecutorDeviceType device = dmEntry.first;

        for (const auto &templateMeasurement: dmEntry.second) {
            AnalyticalTemplate templ = templateMeasurement.first;
            TimePrediction prediction = extrapolation->getExtrapolatedData(templateMeasurement.second);

            // TODO: is ok?
            dp[device][templ] = std::move(prediction);
        }
    }
}

}

