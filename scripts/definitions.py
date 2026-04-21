from .defs.assets import *

import dagster as dg

defs = dg.Definitions(
    assets=[raw_card_json, raw_card_db, card_table],
)


