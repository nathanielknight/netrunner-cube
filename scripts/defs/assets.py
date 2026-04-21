import json
import sqlite3
import urllib.request
from pathlib import Path

import dagster as dg


DATA_DIR = Path(__file__).parent.parent / "data"
if not DATA_DIR.exists():
    DATA_DIR.mkdir()
else:
    assert DATA_DIR.is_dir(), f"{DATA_DIR} isn't a directory"


CARD_PATH = DATA_DIR / "raw_card_data.json"
DB_PATH = DATA_DIR / "cards.sqlite3"

NETRUNNERDB_API_RUL = "https://api-preview.netrunnerdb.com/api/v3/public/cards"


@dg.asset
def raw_card_json(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    context.log.info(f"Fetching cards from {NETRUNNERDB_API_RUL}")

    with urllib.request.urlopen(NETRUNNERDB_API_RUL) as resp:
        data = json.loads(resp.read())

    with CARD_PATH.open("w") as f:
        json.dump(data, f, indent=2)

    record_count = len(data.get("data", []))
    context.log.info(f"Wrote {record_count} cards to {CARD_PATH}")
    return dg.MaterializeResult(
        metadata={
            "record_count": dg.MetadataValue.int(record_count),
            "path": dg.MetadataValue.path(CARD_PATH),
        }
    )


@dg.asset(deps=[raw_card_json])
def raw_card_db(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    context.log.info("Loading raw card data into database")

    dbpath = DATA_DIR / "cards.db"

    context.log.debug(f"Connecting to {dbpath}")
    with sqlite3.connect(dbpath) as cxn:
        context.log.debug("setting up db")
        cxn.execute("DROP TABLE IF EXISTS raw")
        cxn.execute("CREATE TABLE raw (card TEXT NOT NULL)")

        context.log.debug("loading cards")
        event = context.instance.get_latest_materialization_event(dg.AssetKey("raw_card_json"))
        assert event and event.asset_materialization, "raw_card_json has not been materialized"
        card_path = Path(event.asset_materialization.metadata["path"].value)
        with card_path.open() as inf:
            cards = json.load(inf)["data"]

        cxn.executemany(
            "INSERT INTO raw(card) VALUES (?)",
            ((json.dumps(c),) for c in cards),
        )

    return dg.MaterializeResult(
        metadata={"record_count": len(cards), "dbpath": DB_PATH}
    )
