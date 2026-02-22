from dataclasses import dataclass
from typing import Any, Protocol, Tuple

from eeql.vocabulary import data_types as dty


@dataclass
class ColumnMeta:
    name: str
    native_type: Any


class SqlMetadataAdapter(Protocol):
    def get_columns(self, conn: Any, sql: str) -> list[ColumnMeta]: ...

    def to_eeql_type(self, native_type: Any): ...


class SnowflakeSqlMetadataAdapter:
    def get_columns(self, conn: Any, sql: str) -> list[ColumnMeta]:
        cur = conn.cursor()
        metadata_query = f"with query as ({sql}) select * from query where 1=0"
        cur.execute(metadata_query)
        column_metadata = cur.description
        cur.close()
        return [ColumnMeta(name=cm.name, native_type=cm.type_code) for cm in column_metadata]

    def to_eeql_type(self, native_type: Any):
        snowflake_data_type_map = {
            0: dty.TypeInteger(),
            1: dty.TypeFloat(),
            2: dty.TypeString(),
            3: dty.TypeDate(),
            # 5: "variant",
            6: dty.TypeTimestamp(),  # timestampltz
            7: dty.TypeTimestamp(),  # timestamptz
            8: dty.TypeTimestamp(),  # timestampntz
            # 9: "object",
            # 10: "array",
            # 11: "binary",
            12: dty.TypeTime(),
            13: dty.TypeBoolean(),
        }
        if native_type not in snowflake_data_type_map:
            raise ValueError(f"Unsupported Snowflake type code `{native_type}` in Event.from_sql()")
        return snowflake_data_type_map[native_type]


class DuckDbSqlMetadataAdapter:
    def get_columns(self, conn: Any, sql: str) -> list[ColumnMeta]:
        metadata_query = f"with query as ({sql}) select * from query where 1=0"
        relation = conn.query(metadata_query)
        return [
            ColumnMeta(name=column_name, native_type=dtype)
            for column_name, dtype in zip(relation.columns, relation.dtypes)
        ]

    def to_eeql_type(self, native_type: Any):
        dt = str(native_type).upper()
        if dt in {
            "TINYINT",
            "SMALLINT",
            "INTEGER",
            "BIGINT",
            "HUGEINT",
            "UTINYINT",
            "USMALLINT",
            "UINTEGER",
            "UBIGINT",
        }:
            return dty.TypeInteger()
        if dt.startswith("DECIMAL") or dt.startswith("NUMERIC") or dt in {"REAL", "FLOAT", "DOUBLE"}:
            return dty.TypeFloat()
        if dt in {"VARCHAR", "CHAR", "TEXT", "UUID"}:
            return dty.TypeString()
        if dt == "DATE":
            return dty.TypeDate()
        if dt.startswith("TIME"):
            return dty.TypeTime()
        if dt.startswith("TIMESTAMP"):
            return dty.TypeTimestamp()
        if dt == "BOOLEAN":
            return dty.TypeBoolean()
        raise ValueError(f"Unsupported DuckDB data type `{native_type}` in Event.from_sql()")


class PostgresSqlMetadataAdapter:
    def get_columns(self, conn: Any, sql: str) -> list[ColumnMeta]:
        cur = conn.cursor()
        metadata_query = f"with query as ({sql}) select * from query where 1=0"
        cur.execute(metadata_query)
        column_metadata = cur.description
        normalized_columns: list[ColumnMeta] = []
        for cm in column_metadata:
            if hasattr(cm, "name"):
                column_name = cm.name
            elif isinstance(cm, tuple) and len(cm) > 0:
                column_name = cm[0]
            else:
                raise ValueError(
                    f"Unsupported Postgres column metadata entry `{cm}` in Event.from_sql()"
                )

            if hasattr(cm, "type_code"):
                native_type = cm.type_code
            elif isinstance(cm, tuple) and len(cm) > 1:
                native_type = cm[1]
            else:
                native_type = None

            normalized_columns.append(ColumnMeta(name=column_name, native_type=native_type))

        type_oids = sorted(
            {
                int(column.native_type)
                for column in normalized_columns
                if isinstance(column.native_type, int) and column.native_type > 0
            }
        )
        oid_to_name = {}
        if type_oids:
            oid_csv = ",".join(str(oid) for oid in type_oids)
            cur.execute(f"select oid, typname from pg_type where oid in ({oid_csv})")
            oid_to_name = dict(cur.fetchall())
        cur.close()
        return [
            ColumnMeta(
                name=column.name,
                native_type=oid_to_name.get(column.native_type, column.native_type),
            )
            for column in normalized_columns
        ]

    def to_eeql_type(self, native_type: Any):
        postgres_oid_map = {
            16: "bool",
            20: "int8",
            21: "int2",
            23: "int4",
            25: "text",
            114: "json",
            700: "float4",
            701: "float8",
            1042: "bpchar",
            1043: "varchar",
            1082: "date",
            1083: "time",
            1114: "timestamp",
            1184: "timestamptz",
            1266: "timetz",
            1700: "numeric",
            2950: "uuid",
            3802: "jsonb",
        }
        if isinstance(native_type, int):
            native_type = postgres_oid_map.get(native_type, native_type)
        dt = str(native_type).lower()
        if dt in {"int2", "int4", "int8"}:
            return dty.TypeInteger()
        if "int8" in dt or "int16" in dt or "int32" in dt or "int64" in dt:
            return dty.TypeInteger()
        if dt in {"numeric", "decimal", "float4", "float8"}:
            return dty.TypeFloat()
        if "numeric" in dt or "decimal" in dt or "float" in dt or "double" in dt:
            return dty.TypeFloat()
        if dt in {"text", "varchar", "bpchar", "uuid", "json", "jsonb"}:
            return dty.TypeString()
        if "string" in dt or "text" in dt or "json" in dt or "uuid" in dt:
            return dty.TypeString()
        if dt == "date":
            return dty.TypeDate()
        if "date" in dt and "timestamp" not in dt:
            return dty.TypeDate()
        if dt in {"time", "timetz"}:
            return dty.TypeTime()
        if "time" in dt and "timestamp" not in dt:
            return dty.TypeTime()
        if dt in {"timestamp", "timestamptz"}:
            return dty.TypeTimestamp()
        if "timestamp" in dt:
            return dty.TypeTimestamp()
        if dt == "bool":
            return dty.TypeBoolean()
        if "bool" in dt:
            return dty.TypeBoolean()
        raise ValueError(f"Unsupported Postgres data type `{native_type}` in Event.from_sql()")


class BigQueryClientSqlMetadataAdapter:
    def get_columns(self, conn: Any, sql: str) -> list[ColumnMeta]:
        metadata_query = f"with query as ({sql}) select * from query where 1=0"
        query_job = conn.query(metadata_query)
        column_metadata = query_job.result().schema
        return [ColumnMeta(name=field.name, native_type=field.field_type) for field in column_metadata]

    def to_eeql_type(self, native_type: Any):
        dt = str(native_type).upper().split(".")[-1]
        if dt == "INT64":
            return dty.TypeInteger()
        if dt in {"FLOAT64", "NUMERIC", "BIGNUMERIC"}:
            return dty.TypeFloat()
        if dt in {"STRING", "BYTES", "JSON", "GEOGRAPHY"}:
            return dty.TypeString()
        if dt == "DATE":
            return dty.TypeDate()
        if dt == "TIME":
            return dty.TypeTime()
        if dt in {"TIMESTAMP", "DATETIME"}:
            return dty.TypeTimestamp()
        if dt in {"BOOL", "BOOLEAN"}:
            return dty.TypeBoolean()
        raise ValueError(f"Unsupported BigQuery data type `{native_type}` in Event.from_sql()")


class BigQueryDbApiSqlMetadataAdapter:
    def get_columns(self, conn: Any, sql: str) -> list[ColumnMeta]:
        cur = conn.cursor()
        metadata_query = f"with query as ({sql}) select * from query where 1=0"
        cur.execute(metadata_query)
        column_metadata = cur.description
        cur.close()
        return [ColumnMeta(name=cm.name, native_type=cm.type_code) for cm in column_metadata]

    def to_eeql_type(self, native_type: Any):
        dt = str(native_type).upper().split(".")[-1]
        if dt == "INT64":
            return dty.TypeInteger()
        if dt in {"FLOAT64", "NUMERIC", "BIGNUMERIC"}:
            return dty.TypeFloat()
        if dt in {"STRING", "BYTES", "JSON", "GEOGRAPHY"}:
            return dty.TypeString()
        if dt == "DATE":
            return dty.TypeDate()
        if dt == "TIME":
            return dty.TypeTime()
        if dt in {"TIMESTAMP", "DATETIME"}:
            return dty.TypeTimestamp()
        if dt in {"BOOL", "BOOLEAN"}:
            return dty.TypeBoolean()
        raise ValueError(f"Unsupported BigQuery data type `{native_type}` in Event.from_sql()")


def infer_sql_backend(conn: Any) -> Tuple[str, str]:
    connection_class = conn.__class__
    module_name = (getattr(connection_class, "__module__", "") or "").lower()
    class_name = (getattr(connection_class, "__name__", "") or "").lower()

    if module_name.startswith("google.cloud.bigquery.dbapi"):
        return ("bigquery", "dbapi")
    if module_name.startswith("google.cloud.bigquery.client") or (
        "google.cloud.bigquery" in module_name and class_name == "client"
    ):
        return ("bigquery", "client")
    if module_name.startswith("snowflake.connector"):
        return ("snowflake", "dbapi")
    if "duckdb" in module_name or class_name == "duckdbpyconnection":
        return ("duckdb", "native")
    if module_name.startswith("psycopg") or module_name.startswith("psycopg2"):
        return ("postgres", "dbapi")
    if module_name.startswith("adbc_driver_postgresql"):
        return ("postgres", "adbc")
    if module_name.startswith("adbc_driver_manager.dbapi"):
        try:
            info = conn.adbc_get_info()
            driver = str(info.get("driver_name", "")).lower()
            vendor = str(info.get("vendor_name", "")).lower()
            if "postgres" in driver or "postgresql" in driver or "postgres" in vendor:
                return ("postgres", "adbc")
        except Exception:
            pass

        connection_repr = repr(conn).lower()
        if "postgres" in connection_repr or "postgresql" in connection_repr:
            return ("postgres", "adbc")
        raise ValueError(
            "ADBC DBAPI connection detected, but backend could not be inferred. "
            "Use adbc_driver_postgresql.dbapi.connect(...) for Postgres support."
        )

    raise ValueError(
        f"Unsupported SQL connection type `{connection_class}` passed to Event.from_sql()."
    )


def get_sql_metadata_adapter(conn: Any) -> SqlMetadataAdapter:
    warehouse, interface = infer_sql_backend(conn)

    if warehouse == "snowflake":
        return SnowflakeSqlMetadataAdapter()
    if warehouse == "duckdb":
        return DuckDbSqlMetadataAdapter()
    if warehouse == "postgres":
        return PostgresSqlMetadataAdapter()
    if warehouse == "bigquery" and interface == "client":
        return BigQueryClientSqlMetadataAdapter()
    if warehouse == "bigquery" and interface == "dbapi":
        return BigQueryDbApiSqlMetadataAdapter()

    raise ValueError(
        f"Unsupported SQL backend combination `{warehouse}` / `{interface}` in Event.from_sql()"
    )


def get_columns_from_sql(conn: Any, sql: str) -> dict[str, dict[str, Any]]:
    adapter = get_sql_metadata_adapter(conn)
    columns = adapter.get_columns(conn, sql)
    return {
        column.name.lower(): {"data_type": adapter.to_eeql_type(column.native_type)}
        for column in columns
    }
