/*
 * Copyright 2020 OmniSci, Inc.
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

#pragma once

#include <arrow/api.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/statistics.h>
#include <parquet/types.h>

#include "Catalog/ColumnDescriptor.h"
#include "DataMgr/ChunkMetadata.h"
#include "Shared/mapd_shared_mutex.h"

namespace foreign_storage {

using UniqueReaderPtr = std::unique_ptr<parquet::arrow::FileReader>;
using ReaderPtr = parquet::arrow::FileReader*;

/*
  Splits up a set of items to be processed into multiple partitions, with the intention
  that each thread will process a separate part.
 */
// TODO(Misiu): Change this to return a list of Views/Ranges when we support c++20.
template <typename T>
auto partition_for_threads(const std::set<T>& items, size_t max_threads) {
  const size_t items_per_thread = (items.size() + (max_threads - 1)) / max_threads;
  std::list<std::set<T>> items_by_thread;
  auto i = 0U;
  for (auto item : items) {
    if (i++ % items_per_thread == 0) {
      items_by_thread.emplace_back(std::set<T>{});
    }
    items_by_thread.back().emplace(item);
  }
  return items_by_thread;
}

/*
  Splits up a vector of items to be processed into multiple partitions, with the intention
  that each thread will process a separate part.
 */
// TODO: refactor partition_for_threads to use Requires when we support c++20
template <typename T>
auto partition_for_threads(const std::vector<T>& items, size_t max_threads) {
  const size_t items_per_thread = (items.size() + (max_threads - 1)) / max_threads;
  std::list<std::vector<T>> items_by_thread;
  auto i = 0U;
  for (auto item : items) {
    if (i++ % items_per_thread == 0) {
      items_by_thread.emplace_back(std::vector<T>{});
    }
    items_by_thread.back().emplace_back(item);
  }
  return items_by_thread;
}

struct RowGroupInterval {
  std::string file_path;
  int start_index{-1}, end_index{-1};
};

struct RowGroupMetadata {
  std::string file_path;
  int row_group_index;
  std::list<std::shared_ptr<ChunkMetadata>> column_chunk_metadata;
};

UniqueReaderPtr open_parquet_table(const std::string& file_path,
                                   std::shared_ptr<arrow::fs::FileSystem>& file_system);

std::pair<int, int> get_parquet_table_size(const ReaderPtr& reader);

const parquet::ColumnDescriptor* get_column_descriptor(
    const parquet::arrow::FileReader* reader,
    const int logical_column_index);

void validate_equal_column_descriptor(
    const parquet::ColumnDescriptor* reference_descriptor,
    const parquet::ColumnDescriptor* new_descriptor,
    const std::string& reference_file_path,
    const std::string& new_file_path);

std::unique_ptr<ColumnDescriptor> get_sub_type_column_descriptor(
    const ColumnDescriptor* column);

std::shared_ptr<parquet::Statistics> validate_and_get_column_metadata_statistics(
    const parquet::ColumnChunkMetaData* column_metadata);

// A cache for parquet FileReaders which locks access for parallel use.
class FileReaderMap {
 public:
  const ReaderPtr getOrInsert(const std::string& path,
                              std::shared_ptr<arrow::fs::FileSystem>& file_system) {
    mapd_unique_lock<std::mutex> cache_lock(mutex_);
    if (map_.count(path) < 1 || !(map_.at(path))) {
      map_[path] = open_parquet_table(path, file_system);
    }
    return map_.at(path).get();
  }

  const ReaderPtr insert(const std::string& path,
                         std::shared_ptr<arrow::fs::FileSystem>& file_system) {
    mapd_unique_lock<std::mutex> cache_lock(mutex_);
    map_[path] = open_parquet_table(path, file_system);
    return map_.at(path).get();
  }

  void initializeIfEmpty(const std::string& path) {
    mapd_unique_lock<std::mutex> cache_lock(mutex_);
    if (map_.count(path) < 1) {
      map_.emplace(path, UniqueReaderPtr());
    }
  }

  void clear() {
    mapd_unique_lock<std::mutex> cache_lock(mutex_);
    map_.clear();
  }

 private:
  mutable std::mutex mutex_;
  std::map<const std::string, UniqueReaderPtr> map_;
};
}  // namespace foreign_storage
