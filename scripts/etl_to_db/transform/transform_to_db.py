import polars as pl

from scripts.common.config import DATA_ROOT
from scripts.common.files import dated_file, read_json, write_json_atomic


def _clean(
    rows: list[dict], required: list[str], unique_subset: list[str] | None = None
) -> pl.DataFrame:
    frame = pl.DataFrame(rows)
    if frame.is_empty():
        return frame
    return frame.drop_nulls(subset=required).unique(
        subset=unique_subset,
        keep="last",
        maintain_order=True,
    )


def _write(frame: pl.DataFrame, dataset: str, execution_date) -> None:
    path = dated_file(
        DATA_ROOT / "processed" / dataset,
        dataset,
        execution_date,
        ".json",
    )
    write_json_atomic(frame.to_dicts(), path)
    print(f"Wrote {frame.height} {dataset} rows to {path}")


def transform_to_db(**context) -> None:
    execution_date = context["execution_date"]
    companies = read_json(
        dated_file(
            DATA_ROOT / "raw" / "companies",
            "crawl_companies",
            execution_date,
            ".json",
        )
    )
    markets = read_json(
        dated_file(
            DATA_ROOT / "raw" / "markets",
            "crawl_markets",
            execution_date,
            ".json",
        )
    )

    regions = _clean(
        [
            {
                "region": row.get("region"),
                "local_open": row.get("local_open"),
                "local_close": row.get("local_close"),
            }
            for row in markets
        ],
        ["region", "local_open", "local_close"],
    )
    _write(regions, "regions", execution_date)

    industries = _clean(
        [
            {"industry": row.get("industry"), "sector": row.get("sector")}
            for row in companies
        ],
        ["industry", "sector"],
    )
    _write(industries, "industries", execution_date)

    sic_industries = _clean(
        [
            {
                "sic_industry": row.get("sicIndustry"),
                "sic_sector": row.get("sicSector"),
            }
            for row in companies
        ],
        ["sic_industry", "sic_sector"],
    )
    _write(sic_industries, "sic_industries", execution_date)

    exchanges = _clean(
        [
            {"name": exchange.strip(), "region": row.get("region")}
            for row in markets
            for exchange in row.get("primary_exchanges", "").split(",")
            if exchange.strip()
        ],
        ["name", "region"],
    )
    _write(exchanges, "exchanges", execution_date)

    company_rows = [
        {
            "name": row.get("name"),
            "ticker": row.get("ticker"),
            "is_delisted": row.get("isDelisted"),
            "category": row.get("category"),
            "currency": row.get("currency"),
            "location": row.get("location"),
            "industry": row.get("industry"),
            "sector": row.get("sector"),
            "exchange": row.get("exchange"),
            "sic_industry": row.get("sicIndustry"),
            "sic_sector": row.get("sicSector"),
        }
        for row in companies
    ]
    company_frame = _clean(
        company_rows,
        ["name", "ticker", "is_delisted", "exchange"],
        unique_subset=["ticker", "is_delisted"],
    ).filter(
        pl.col("exchange").is_in(["NASDAQ", "NYSE"])
        & (pl.col("currency") == "USD")
    )
    _write(company_frame, "companies", execution_date)
