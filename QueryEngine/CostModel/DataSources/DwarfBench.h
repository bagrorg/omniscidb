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

#include <string>

#include "DataSource.h"

namespace CostModel {

// COnfig
// TODO: somwhere else
const std::string DWARF_BENCH_PATH = "/dwarf_bench";
const std::string sizeHeader = "buf_size_bytes";
const std::string timeHeader = "total_time";


class DwarfBench : public DataSource {
public: 
    DwarfBench() = default;

    std::unordered_map<AnalyticalTemplate, Measurement> getMeasurements(const std::vector<AnalyticalTemplate> &templates) override;

private:
    void runSpecifiedDwarf(const std::string &templateName, const boost::filesystem::path &reportFile);
    Measurement readReportFile(const boost::filesystem::path &reportFile);
};

}