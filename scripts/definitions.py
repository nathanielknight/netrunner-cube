from .defs.assets import raw_card_json, raw_card_db

import dagster as dg

defs = dg.Definitions(
    assets=[raw_card_json, raw_card_db],
)


