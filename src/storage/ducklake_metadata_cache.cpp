#include "storage/ducklake_metadata_cache.hpp"
#include "storage/ducklake_inlined_data.hpp"
#include "storage/ducklake_metadata_cache_log_type.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

static const char *EventName(DuckLakeMetadataCache::SchemaVersionLRU::Event e) {
	switch (e) {
	case DuckLakeMetadataCache::SchemaVersionLRU::Event::HIT:
		return "hit";
	case DuckLakeMetadataCache::SchemaVersionLRU::Event::MISS:
		return "miss";
	case DuckLakeMetadataCache::SchemaVersionLRU::Event::EVICTION:
		return "eviction";
	}
	return "unknown";
}

static idx_t SizeOfInlinedData(const shared_ptr<DuckLakeInlinedData> &data) {
	if (!data || !data->data) {
		return 1;
	}
	idx_t bytes = data->data->SizeInBytes();
	bytes += data->row_ids.size() * sizeof(int64_t);
	return bytes == 0 ? 1 : bytes;
}

DuckLakeMetadataCache::DuckLakeMetadataCache(idx_t schema_version_max_entries, idx_t inlined_data_max_bytes)
    : last_cleanup_generation(0),
      schema_versions(SchemaVersionLRU::ByCountTag {}, schema_version_max_entries),
      inlined_data(InlinedDataLRU::ByBytesTag {}, inlined_data_max_bytes, SizeOfInlinedData) {
}

void DuckLakeMetadataCache::EnableLogging(DatabaseInstance &db, string catalog_name) {
	auto &db_ref = db;
	auto catalog = std::move(catalog_name);

	schema_versions.SetEventCallback(
	    [&db_ref, catalog](SchemaVersionLRU::Event event, const SchemaVersionKey &key) {
		    DUCKDB_LOG(db_ref, DuckLakeMetadataCacheLogType, catalog, string("schema_version"),
		               string(EventName(event)),
		               StringUtil::Format("table=%llu,version=%llu", key.table_id.index, key.schema_version));
	    });

	inlined_data.SetEventCallback([&db_ref, catalog](InlinedDataLRU::Event event, const InlinedDataKey &key) {
		// Map InlinedDataLRU::Event to the same string names as the schema-version cache.
		const char *name = "unknown";
		switch (event) {
		case InlinedDataLRU::Event::HIT:
			name = "hit";
			break;
		case InlinedDataLRU::Event::MISS:
			name = "miss";
			break;
		case InlinedDataLRU::Event::EVICTION:
			name = "eviction";
			break;
		}
		DUCKDB_LOG(db_ref, DuckLakeMetadataCacheLogType, catalog, string("inlined_data"), string(name),
		           StringUtil::Format("table=%s,snapshot=%llu", key.table_name, key.snapshot_id));
	});
}

void DuckLakeMetadataCache::InvalidateInlinedDataForTable(const string &table_name) {
	inlined_data.EraseIf([&table_name](const InlinedDataKey &k) { return k.table_name == table_name; });
}

void DuckLakeMetadataCache::OnCleanupGenerationAdvance(uint64_t new_generation, idx_t min_live_snapshot_id) {
	auto previous = last_cleanup_generation.load();
	if (new_generation <= previous) {
		return;
	}
	if (!last_cleanup_generation.compare_exchange_strong(previous, new_generation)) {
		// another thread observed the same advance and is/has swept; do nothing
		return;
	}
	inlined_data.EraseIf(
	    [min_live_snapshot_id](const InlinedDataKey &k) { return k.snapshot_id < min_live_snapshot_id; });
}

} // namespace duckdb
