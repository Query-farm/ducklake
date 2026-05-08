#include "metadata_manager/vgi_metadata_manager.hpp"
#include "common/ducklake_util.hpp"
#include "duckdb/main/database.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_transaction.hpp"

namespace duckdb {

VgiMetadataManager::VgiMetadataManager(DuckLakeTransaction &transaction) : DuckLakeMetadataManager(transaction) {
}

bool VgiMetadataManager::TypeIsNativelySupported(const LogicalType &type) {
	// SQLite-backed DO storage. Mirror SQLiteMetadataManager.
	switch (type.id()) {
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::MAP:
	case LogicalTypeId::LIST:
	// SQLite converts IEEE 754 NaN to NULL when storing double values,
	// so FLOAT/DOUBLE must be stored as VARCHAR to preserve NaN through the round-trip
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::VARIANT:
	// SQLite has no first-class temporal types — declaring a column as
	// DATE/TIME/TIMESTAMP gives it NUMERIC affinity, and CAST(string AS
	// TIMESTAMP) parses the leading digits and returns a number (e.g.
	// 'CAST('2024-01-01 09:00:00' AS TIMESTAMP)' → 2024). Force these to
	// be stored as VARCHAR text so the original string value round-trips
	// through the DO unchanged; the worker re-parses on read.
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_NS:
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_TZ:
		return false;
	default:
		return true;
	}
}

bool VgiMetadataManager::SupportsInlining(const LogicalType &type) {
	if (type.id() == LogicalTypeId::VARIANT) {
		return false;
	}
	return DuckLakeMetadataManager::SupportsInlining(type);
}

string VgiMetadataManager::GetColumnTypeInternal(const LogicalType &column_type) {
	switch (column_type.id()) {
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::VARIANT:
	// Same reason as TypeIsNativelySupported above: declare temporal
	// columns with VARCHAR (TEXT affinity) in the DO's SQLite so values
	// round-trip as strings and don't get NUMERIC-coerced by `CAST(...)`.
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_NS:
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_TZ:
		return "VARCHAR";
	default:
		return column_type.ToString();
	}
}

void VgiMetadataManager::InitializeDuckLake(bool, DuckLakeEncryption) {
	// The VGI worker owns bootstrap. Re-attach the VGI catalog with the
	// `data_path` option to auto-bootstrap, or call
	// `<catalog>.main.catalog_create('<path>')` before attaching DuckLake.
	throw InvalidInputException(
	    "DuckLake metadata tables are not present in the VGI-backed catalog. "
	    "Bootstrap is owned by the VGI ATTACH — re-ATTACH the VGI catalog with "
	    "the `data_path` option set, or call `<catalog>.main.catalog_create('<path>')` "
	    "manually before attaching DuckLake.");
}

unique_ptr<QueryResult> VgiMetadataManager::Execute(DuckLakeSnapshot snapshot, string &query) {
	auto &commit_info = transaction.GetCommitInfo();

	// Snapshot / commit-info substitutions (same set as the base path).
	query = StringUtil::Replace(query, "{SNAPSHOT_ID}", to_string(snapshot.snapshot_id));
	query = StringUtil::Replace(query, "{SCHEMA_VERSION}", to_string(snapshot.schema_version));
	query = StringUtil::Replace(query, "{NEXT_CATALOG_ID}", to_string(snapshot.next_catalog_id));
	query = StringUtil::Replace(query, "{NEXT_FILE_ID}", to_string(snapshot.next_file_id));
	query = StringUtil::Replace(query, "{AUTHOR}", commit_info.author.ToSQLString());
	query = StringUtil::Replace(query, "{COMMIT_MESSAGE}", commit_info.commit_message.ToSQLString());
	query = StringUtil::Replace(query, "{COMMIT_EXTRA_INFO}", commit_info.commit_extra_info.ToSQLString());

	auto &connection = transaction.GetConnection();
	auto &ducklake_catalog = transaction.GetCatalog();
	auto vgi_catalog_identifier = DuckLakeUtil::SQLIdentifierToString(ducklake_catalog.MetadataDatabaseName());
	auto data_path = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.DataPath());

	// The DO's SQLite has only the `main` schema, so all metadata-catalog
	// references resolve to plain `main.<table>`. Substitute every variant of
	// `{METADATA_*}` with the DO-side schema; do not introduce DuckDB-side
	// catalog/database qualifiers because the SQL is shipped as a literal to
	// the DO and resolved by SQLite there.
	query = StringUtil::Replace(query, "{METADATA_CATALOG_NAME_LITERAL}", "'main'");
	query = StringUtil::Replace(query, "{METADATA_CATALOG_NAME_IDENTIFIER}", "main");
	query = StringUtil::Replace(query, "{METADATA_SCHEMA_NAME_LITERAL}", "'main'");
	query = StringUtil::Replace(query, "{METADATA_CATALOG}", "main");
	query = StringUtil::Replace(query, "{METADATA_SCHEMA_ESCAPED}", "main");
	query = StringUtil::Replace(query, "{DATA_PATH}", data_path);

	// Forward the entire batch as a single HTTP POST to the DO via the worker.
	return connection.Query(StringUtil::Format("CALL %s.main.ducklake_do_execute(%s)", vgi_catalog_identifier,
	                                            SQLString(query)));
}

} // namespace duckdb
