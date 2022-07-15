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

#include "DwarfBench.h"

#include <boost/filesystem.hpp>
#include <boost/algorithm/string.hpp>

#include <iostream>
#include <fstream>

namespace CostModel {

DeviceMeasurements DwarfBench::getMeasurements(const std::vector<ExecutorDeviceType> &devices, const std::vector<AnalyticalTemplate> &templates) {
    DeviceMeasurements dm;
    boost::filesystem::path dwarf_path = DWARF_BENCH_PATH;

    if (!boost::filesystem::exists(dwarf_path /  "results")) {
        boost::filesystem::create_directory(dwarf_path / "results");
    }

    for (AnalyticalTemplate templ: templates) {
        for (ExecutorDeviceType device: devices) {
            std::string deviceName = deviceToDwarfString(device);
            std::string templateName = templatetoString(templ);
            boost::filesystem::path reportFile = "report_" + templateName + ".csv";

            runSpecifiedDwarf(templateName, deviceName, reportFile);
            dm[device][templ] = parser.parseMeasurement(reportFile);
        }
    }

    return dm;
}

// TODO: more crossplatform and check errors
void DwarfBench::runSpecifiedDwarf(const std::string &templateName, const std::string &deviceName, const boost::filesystem::path &reportFile) {
    std::string scriptPath = DWARF_BENCH_PATH + "/scripts/" + "benchmark_" + templateName + ".sh";
    std::string executeLine = scriptPath + " " + reportFile.string() + " " + deviceName + " > /dev/null";
    system(executeLine.c_str());
}

Measurement DwarfBench::DwarfCsvParser::parseMeasurement(const boost::filesystem::path &csv) {
    line.clear();
    entries.clear();

    std::ifstream in(csv);
    if (!in.good())
        throw DwarfBenchException("No such report file: " + csv.string());

    Measurement ms;
    CsvColumnIndexes indexes = parseHeader(in);

    while(std::getline(in, line)) {
        entries.clear();
        boost::split(entries, line, ",");
        ms.emplace_back(entries.at(indexes.sizeIndex), entries.at(indexes.timeIndex));
    }

    std::sort(ms.begin(), ms.end(), MeasurementOrder());

    return ms;
}

size_t DwarfBench::DwarfCsvParser::getCsvColumnIndex(const std::vector<std::string> &header, const std::string &columnName) {
    auto iter = std::find(header.begin(), header.end(), columnName);

    if (iter == header.end())
        throw DwarfBenchException("No such column: " + columnName);

    return iter - header.begin();
}

DwarfBench::DwarfCsvParser::CsvColumnIndexes DwarfBench::DwarfCsvParser::parseHeader(std::ifstream &in) {
    in.seekg(0);
    
    std::getline(in, line);
    boost::split(entries, line, ",");

    CsvColumnIndexes indexes = {
        .timeIndex = getCsvColumnIndex(entries, timeHeader),
        .sizeIndex = getCsvColumnIndex(entries, sizeHeader)
    };

    return indexes;
}

std::string deviceToDwarfString(ExecutorDeviceType device) {
    return device == ExecutorDeviceType::CPU ? "cpu" : "gpu";
}

}