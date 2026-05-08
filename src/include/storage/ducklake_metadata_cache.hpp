//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_metadata_cache.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/ducklake_lru.hpp"
#include "common/index.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class DatabaseInstance;
struct DuckLakeInlinedData;

//! Cache for catalog metadata reads keyed on immutable facts. Pull-through:
//! callers invoke GetOrLoad* with a loader; on miss the loader runs and the
//! result is stored.
//!
//! Two caches:
//!   - schema_versions: (TableIndex, schema_version) -> begin_snapshot.
//!     Immutable historic fact. Count-bounded LRU.
//!   - inlined_data: (table_name, snapshot_id, projection) ->
//!     shared_ptr<DuckLakeInlinedData>. Snapshot-pinned, immutable for any
//!     sealed snapshot. Byte-bounded LRU. Swept on cleanup-watermark advance.
class DuckLakeMetadataCache {
public:
	struct SchemaVersionKey {
		TableIndex table_id;
		idx_t schema_version;

		bool operator==(const SchemaVersionKey &other) const {
			return table_id == other.table_id && schema_version == other.schema_version;
		}
	};

	struct SchemaVersionKeyHash {
		size_t operator()(const SchemaVersionKey &k) const noexcept {
			return std::hash<idx_t>()(k.table_id.index) ^ (std::hash<idx_t>()(k.schema_version) << 1);
		}
	};

	struct InlinedDataKey {
		string table_name;
		idx_t snapshot_id;
		vector<string> columns;

		bool operator==(const InlinedDataKey &other) const {
			return snapshot_id == other.snapshot_id && table_name == other.table_name && columns == other.columns;
		}
	};

	struct InlinedDataKeyHash {
		size_t operator()(const InlinedDataKey &k) const noexcept {
			size_t h = std::hash<string>()(k.table_name);
			h ^= std::hash<idx_t>()(k.snapshot_id) + 0x9e3779b9 + (h << 6) + (h >> 2);
			for (const auto &col : k.columns) {
				h ^= std::hash<string>()(col) + 0x9e3779b9 + (h << 6) + (h >> 2);
			}
			return h;
		}
	};

	using SchemaVersionLRU = DuckLakeLRU<SchemaVersionKey, idx_t, SchemaVersionKeyHash>;
	using InlinedDataLRU =
	    DuckLakeLRU<InlinedDataKey, shared_ptr<DuckLakeInlinedData>, InlinedDataKeyHash>;

public:
	DuckLakeMetadataCache(idx_t schema_version_max_entries, idx_t inlined_data_max_bytes);

	//! Wire up logging. Once called, every hit/miss/eviction emits a
	//! DuckLakeMetadataCacheLogType event against the given database instance.
	//! Safe to call once after construction; subsequent events use the latest
	//! db pointer / catalog name.
	void EnableLogging(DatabaseInstance &db, string catalog_name);

	//! Pull-through lookup for Query 3.
	template <class Loader>
	idx_t GetOrLoadSchemaVersion(TableIndex table_id, idx_t schema_version, Loader loader) {
		return schema_versions.GetOrLoad({table_id, schema_version}, loader);
	}

	//! Pull-through lookup for Query 4.
	template <class Loader>
	shared_ptr<DuckLakeInlinedData> GetOrLoadInlinedData(const string &table_name, idx_t snapshot_id,
	                                                     const vector<string> &columns, Loader loader) {
		return inlined_data.GetOrLoad({table_name, snapshot_id, columns}, loader);
	}

	//! Called at transaction-start when the catalog server reports a higher
	//! cleanup_generation than we last observed. Sweeps the inlined-data
	//! cache, evicting entries whose snapshot is below the new watermark. The
	//! schema-versions cache is intentionally not swept — its values are
	//! immutable historic facts and stale entries surface as downstream errors
	//! rather than silent corruption.
	void OnCleanupGenerationAdvance(uint64_t new_generation, idx_t min_live_snapshot_id);

	//! Invalidate every cached inlined-data result for a given inlined-table
	//! name. Must be called whenever the inlined-data table is mutated
	//! (e.g. flushed deletions, in-place rewrites) so that subsequent reads
	//! within the same transaction see the new state.
	void InvalidateInlinedDataForTable(const string &table_name);

	uint64_t LastSeenCleanupGeneration() const {
		return last_cleanup_generation.load();
	}

	SchemaVersionLRU::Stats GetSchemaVersionStats() const {
		return schema_versions.GetStats();
	}
	InlinedDataLRU::Stats GetInlinedDataStats() const {
		return inlined_data.GetStats();
	}

private:
	atomic<uint64_t> last_cleanup_generation;
	SchemaVersionLRU schema_versions;
	InlinedDataLRU inlined_data;
};

} // namespace duckdb
