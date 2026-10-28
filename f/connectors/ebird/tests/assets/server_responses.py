"""Mock eBird API responses for testing."""

import json
from pathlib import Path

_ASSETS = Path(__file__).parent

RECENT_OBSERVATIONS = json.loads((_ASSETS / "recent_observations.json").read_text())
NOTABLE_OBSERVATIONS = json.loads((_ASSETS / "notable_observations.json").read_text())

RECENT_COUNT = 56
NOTABLE_COUNT = 3

PRIMARY_SPECIES_CODE = "hoocro1"
PRIMARY_SUB_ID = "S63619695"
PRIMARY_FEATURE_ID = f"{PRIMARY_SUB_ID}_{PRIMARY_SPECIES_CODE}"
PUBLIC_LOCATION_FEATURE_ID = "S63615092_eutspa"
HYBRID_FEATURE_ID = "S63158588_y00764"

NOTABLE_OBS_ID = "OBS527233428"
NOTABLE_SECOND_HERON_ID = "OBS527186633"
NOTABLE_SPARROW_ID = "OBS527201666"
