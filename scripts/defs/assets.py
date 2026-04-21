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


DB_PATH = DATA_DIR / "cards.sqlite3"

NETRUNNERDB_API_RUL = "https://api-preview.netrunnerdb.com/api/v3/public/cards"


@dg.asset
def raw_card_json(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    CARD_PATH = DATA_DIR / "raw_card_data.json"

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

    context.log.debug(f"Connecting to {DB_PATH}")
    with sqlite3.connect(DB_PATH) as cxn:
        context.log.debug("setting up db")
        cxn.execute("DROP TABLE IF EXISTS raw")
        cxn.execute("CREATE TABLE raw (card TEXT NOT NULL)")

        context.log.debug("loading cards")
        event = context.instance.get_latest_materialization_event(
            dg.AssetKey("raw_card_json")
        )
        assert event and event.asset_materialization, (
            "raw_card_json has not been materialized"
        )
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


@dg.asset(deps=[raw_card_db])
def card_table(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    ...
    event = context.instance.get_latest_materialization_event(dg.AssetKey("raw_card_db"))
    assert event and event.asset_materialization, "raw_card_db hasn't been materialized"
    db_path = Path(event.asset_materialization.metadata["dbpath"].value)

    schemapath = Path(__file__).parent / "cards.sql"
    with schemapath.open() as inf:
        schema = inf.read()
    with sqlite3.connect(db_path) as cxn:
        cxn.execute("DROP TABLE IF EXISTS cards")
        cxn.execute(schema)

        for raw in cxn.execute("SELECT card FROM raw").fetchall():
            c = json.loads(raw[0])
            card = {
                "id": c["id"],
                "set_names": c["attributes"]["card_set_names"],
                "title": c["attributes"]["title"],
                "stripped_title": c["attributes"]["stripped_title"],
                "side_id": c["attributes"]["side_id"],
                "faction_id": c["attributes"]["faction_id"],
                "influence_cost": c["attributes"]["influence_cost"],
                "card_type_id": c["attributes"]["card_type_id"],
                "card_subtype_ids": c["attributes"]["card_subtype_ids"],
                "cost": c["attributes"]["cost"],
                "trash_cost": c["attributes"]["trash_cost"],
                "advancement_requirement": c["attributes"]["advancement_requirement"],
                "agenda_points": c["attributes"]["agenda_points"],
                "strength": c["attributes"]["strength"],
                "memory_cost": c["attributes"]["memory_cost"],
            }
            flds = sorted(card.keys())
            command = f"""
                INSERT INTO cards
                ({", ".join(flds)})
                VALUES
                ({", ".join("?" for _ in flds)})
            """

            def asinput(c):
                if type(c) is list:
                    return ', '.join(c)
                return c
            cxn.execute(command, [asinput(card[f]) for f in flds])

    return dg.MaterializeResult(metadata={"tablename": "cards"})
