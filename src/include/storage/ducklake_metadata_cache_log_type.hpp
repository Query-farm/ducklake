//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_metadata_cache_log_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/logging/log_type.hpp"

namespace duckdb {

//! Structured log type for DuckLake metadata cache events (Query 3 / Query 4
//! pull-through caches). Emits hit, miss, and eviction events with the cache
//! name and a stable string form of the key.
class DuckLakeMetadataCacheLogType : public LogType {
public:
	static constexpr const char *NAME = "DuckLakeMetadataCache";
	static constexpr LogLevel LEVEL = LogLevel::LOG_TRACE;

	DuckLakeMetadataCacheLogType();

	static LogicalType GetLogType();
	static string ConstructLogMessage(const string &catalog_name, const string &cache, const string &event,
	                                  const string &key);
};

} // namespace duckdb
