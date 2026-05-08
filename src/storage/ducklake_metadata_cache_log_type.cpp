#include "storage/ducklake_metadata_cache_log_type.hpp"

#include "duckdb/common/types/value.hpp"

namespace duckdb {

constexpr LogLevel DuckLakeMetadataCacheLogType::LEVEL;

DuckLakeMetadataCacheLogType::DuckLakeMetadataCacheLogType() : LogType(NAME, LEVEL, GetLogType()) {
}

LogicalType DuckLakeMetadataCacheLogType::GetLogType() {
	child_list_t<LogicalType> child_list = {
	    {"catalog", LogicalType::VARCHAR},
	    {"cache", LogicalType::VARCHAR},
	    {"event", LogicalType::VARCHAR},
	    {"key", LogicalType::VARCHAR},
	};
	return LogicalType::STRUCT(child_list);
}

string DuckLakeMetadataCacheLogType::ConstructLogMessage(const string &catalog_name, const string &cache,
                                                         const string &event, const string &key) {
	child_list_t<Value> child_list = {
	    {"catalog", Value(catalog_name)},
	    {"cache", Value(cache)},
	    {"event", Value(event)},
	    {"key", Value(key)},
	};
	return Value::STRUCT(std::move(child_list)).ToString();
}

} // namespace duckdb
