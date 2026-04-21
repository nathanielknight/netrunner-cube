import json
import sqlite3
import time
import urllib.request
from pathlib import Path

import dagster as dg


DATA_DIR = Path(__file__).parent.parent / "data"
if not DATA_DIR.exists():
    DATA_DIR.mkdir()
else:
    assert DATA_DIR.is_dir(), f"{DATA_DIR} isn't a directory"


DB_PATH = DATA_DIR / "cards.sqlite3"

NETRUNNERDB_API_URL = "https://api-preview.netrunnerdb.com/api/v3/public/cards"

# Delay between paginated requests so we don't hammer the API.
REQUEST_DELAY_SECONDS = 0.25


def _fetch_page(url: str) -> dict:
    with urllib.request.urlopen(url) as resp:
        return json.loads(resp.read())


@dg.asset
def raw_card_db(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    context.log.info(f"Fetching cards from {NETRUNNERDB_API_URL}")

    context.log.debug(f"Connecting to {DB_PATH}")
    with sqlite3.connect(DB_PATH) as cxn:
        context.log.debug("setting up db")
        cxn.execute("DROP TABLE IF EXISTS raw")
        cxn.execute("CREATE TABLE raw (card TEXT NOT NULL)")

        total_count: int | None = None
        loaded = 0
        page_num = 0
        next_url: str | None = NETRUNNERDB_API_URL

        while next_url is not None:
            page_num += 1
            context.log.info(f"Fetching page {page_num}: {next_url}")
            page = _fetch_page(next_url)

            if total_count is None:
                metadata = page.get("metadata") or page.get("meta") or {}
                for key in ("total_count", "total", "totalCount", "count"):
                    if key in metadata:
                        total_count = int(metadata[key])
                        context.log.info(
                            f"API reports {total_count} total cards"
                        )
                        break

            cards = page.get("data", [])
            cxn.executemany(
                "INSERT INTO raw(card) VALUES (?)",
                ((json.dumps(c),) for c in cards),
            )
            loaded += len(cards)
            context.log.info(
                f"Inserted {len(cards)} cards from page {page_num} "
                f"({loaded} total)"
            )

            next_url = (page.get("links") or {}).get("next")
            if next_url:
                time.sleep(REQUEST_DELAY_SECONDS)

        if total_count is not None:
            assert loaded == total_count, (
                f"Expected {total_count} cards from API but loaded {loaded}"
            )
        else:
            context.log.warning(
                "API response did not include a total count; "
                "skipping completeness check"
            )

    metadata: dict[str, object] = {
        "record_count": dg.MetadataValue.int(loaded),
        "dbpath": DB_PATH,
        "pages_fetched": dg.MetadataValue.int(page_num),
    }
    if total_count is not None:
        metadata["api_total_count"] = dg.MetadataValue.int(total_count)

    return dg.MaterializeResult(metadata=metadata)


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
