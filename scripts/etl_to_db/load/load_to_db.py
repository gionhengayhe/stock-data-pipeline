from collections.abc import Iterable

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

from scripts.common.config import DATA_ROOT, postgres_config
from scripts.common.files import dated_file, read_json


def _processed_rows(dataset: str, execution_date) -> list[dict]:
    path = dated_file(
        DATA_ROOT / "processed" / dataset,
        dataset,
        execution_date,
        ".json",
    )
    return read_json(path)


def _upsert(
    cursor,
    *,
    table: str,
    rows: Iterable[dict],
    columns: list[str],
    conflict_columns: list[str],
    update_columns: list[str] | None = None,
) -> int:
    values = [tuple(row.get(column) for column in columns) for row in rows]
    if not values:
        print(f"No rows to load into {table}")
        return 0

    statement = sql.SQL("INSERT INTO {} ({}) VALUES %s ON CONFLICT ({}) ").format(
        sql.Identifier(table),
        sql.SQL(", ").join(map(sql.Identifier, columns)),
        sql.SQL(", ").join(map(sql.Identifier, conflict_columns)),
    )
    if update_columns:
        assignments = [
            sql.SQL("{} = EXCLUDED.{}").format(
                sql.Identifier(column), sql.Identifier(column)
            )
            for column in update_columns
        ]
        assignments.append(sql.SQL("updated_time = CURRENT_TIMESTAMP"))
        target_values = sql.SQL(", ").join(
            sql.SQL("{}.{}").format(sql.Identifier(table), sql.Identifier(column))
            for column in update_columns
        )
        incoming_values = sql.SQL(", ").join(
            sql.SQL("EXCLUDED.{}").format(sql.Identifier(column))
            for column in update_columns
        )
        statement += sql.SQL("DO UPDATE SET {} WHERE ROW({}) IS DISTINCT FROM ROW({})").format(
            sql.SQL(", ").join(assignments), target_values, incoming_values
        )
    else:
        statement += sql.SQL("DO NOTHING")

    execute_values(cursor, statement.as_string(cursor.connection), values, page_size=1000)
    print(f"Loaded {len(values)} source rows into {table}")
    return len(values)


def _key_map(cursor, table: str, key_columns: list[str]) -> dict:
    query = sql.SQL("SELECT {} FROM {}").format(
        sql.SQL(", ").join(
            [sql.Identifier("id"), *map(sql.Identifier, key_columns)]
        ),
        sql.Identifier(table),
    )
    cursor.execute(query)
    rows = cursor.fetchall()
    if len(key_columns) == 1:
        return {row[1]: row[0] for row in rows}
    return {tuple(row[1:]): row[0] for row in rows}


def load_to_db(**context) -> None:
    execution_date = context["execution_date"]
    with psycopg2.connect(**postgres_config()) as conn:
        with conn.cursor() as cursor:
            regions = _processed_rows("regions", execution_date)
            _upsert(
                cursor,
                table="regions",
                rows=regions,
                columns=["region", "local_open", "local_close"],
                conflict_columns=["region"],
                update_columns=["local_open", "local_close"],
            )

            region_ids = _key_map(cursor, "regions", ["region"])
            exchanges = []
            for row in _processed_rows("exchanges", execution_date):
                region_id = region_ids.get(row.get("region"))
                if region_id is None:
                    print(f"Skipping exchange with unknown region: {row}")
                    continue
                exchanges.append({"name": row.get("name"), "region_id": region_id})
            _upsert(
                cursor,
                table="exchanges",
                rows=exchanges,
                columns=["name", "region_id"],
                conflict_columns=["name"],
                update_columns=["region_id"],
            )

            _upsert(
                cursor,
                table="industries",
                rows=_processed_rows("industries", execution_date),
                columns=["industry", "sector"],
                conflict_columns=["industry", "sector"],
            )
            _upsert(
                cursor,
                table="sic_industries",
                rows=_processed_rows("sic_industries", execution_date),
                columns=["sic_industry", "sic_sector"],
                conflict_columns=["sic_industry", "sic_sector"],
            )

            exchange_ids = _key_map(cursor, "exchanges", ["name"])
            industry_ids = _key_map(cursor, "industries", ["industry", "sector"])
            sic_ids = _key_map(
                cursor, "sic_industries", ["sic_industry", "sic_sector"]
            )
            companies = []
            for row in _processed_rows("companies", execution_date):
                exchange_id = exchange_ids.get(row.get("exchange"))
                if exchange_id is None:
                    print(f"Skipping company with unknown exchange: {row}")
                    continue
                companies.append(
                    {
                        "exchange_id": exchange_id,
                        "industry_id": industry_ids.get(
                            (row.get("industry"), row.get("sector"))
                        ),
                        "sic_id": sic_ids.get(
                            (row.get("sic_industry"), row.get("sic_sector"))
                        ),
                        **{
                            column: row.get(column)
                            for column in [
                                "name",
                                "ticker",
                                "is_delisted",
                                "category",
                                "currency",
                                "location",
                            ]
                        },
                    }
                )
            company_columns = [
                "exchange_id",
                "industry_id",
                "sic_id",
                "name",
                "ticker",
                "is_delisted",
                "category",
                "currency",
                "location",
            ]
            _upsert(
                cursor,
                table="companies",
                rows=companies,
                columns=company_columns,
                conflict_columns=["ticker", "is_delisted"],
                update_columns=[
                    column
                    for column in company_columns
                    if column not in {"ticker", "is_delisted"}
                ],
            )
