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

std::vector<Measurement> DwarfBench::DwarfCsvParser::parseMeasurement(const boost::filesystem::path &csv) {
    line.clear();
    entries.clear();

    std::ifstream in(csv);
    if (!in.good())
        throw DwarfBenchException("No such report file: " + csv.string());

    std::vector<Measurement> ms;
    CsvColumnIndexes indexes = parseHeader(in);

    while(std::getline(in, line)) {
        entries.clear();
        boost::split(entries, line, boost::is_any_of(","));

        ms.push_back(parseLine(indexes));
    }

    std::sort(ms.begin(), ms.end());

    return ms;
}

Measurement DwarfBench::DwarfCsvParser::parseLine(const CsvColumnIndexes &indexes) {
    entries.clear();
    boost::split(entries, line, boost::is_any_of(","));

    Measurement m = {
        .bytes = std::stoull(entries.at(indexes.sizeIndex)),
        .milliseconds = std::stoull(entries.at(indexes.timeIndex))
    };

    return m;
}

size_t DwarfBench::DwarfCsvParser::getCsvColumnIndex(const std::string &columnName) {
    auto iter = std::find(entries.begin(), entries.end(), columnName);

    if (iter == entries.end())
        throw DwarfBenchException("No such column: " + columnName);

    return iter - entries.begin();
}

DwarfBench::DwarfCsvParser::CsvColumnIndexes DwarfBench::DwarfCsvParser::parseHeader(std::ifstream &in) {
    in.seekg(0);
    
    std::getline(in, line);
    boost::split(entries, line, boost::is_any_of(","));

    CsvColumnIndexes indexes = {
        .timeIndex = getCsvColumnIndex(timeHeader),
        .sizeIndex = getCsvColumnIndex(sizeHeader)
    };

    return indexes;
}

std::string deviceToDwarfString(ExecutorDeviceType device) {
    return device == ExecutorDeviceType::CPU ? "cpu" : "gpu";
}

}