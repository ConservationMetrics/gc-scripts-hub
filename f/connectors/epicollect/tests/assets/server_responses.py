"""Mock EpiCollect5 API responses for testing.

Structures use the minimum fields the production code reads from each response.
"""

PROJECT_SLUG = "biocultural-monitoring"
FORM_REF = "e452333f782446c9879b094c945e9e0e_6a05d7183234e"
PROJECT_REF = "e452333f782446c9879b094c945e9e0e"
FORM_NAME = "Biocultural monitoring survey"

# Input refs for the three media types + location/barcode
_AUDIO_REF = f"{FORM_REF}_6a0662ace5349"
_PHOTO_REF = f"{FORM_REF}_6a0662b3e534a"
_VIDEO_REF = f"{FORM_REF}_6a0662b8e534b"
_LOCATION_REF = f"{FORM_REF}_6a0662bee534c"
_BARCODE_REF = f"{FORM_REF}_6a0662c3e534d"

# Media filenames used in entry 1
AUDIO_FILENAME = "a9498e4f-70f0-4811-924e-f37b996b9623_1778803517.mp4"
PHOTO_FILENAME = "a9498e4f-70f0-4811-924e-f37b996b9623_1778803534.jpg"
VIDEO_FILENAME = "a9498e4f-70f0-4811-924e-f37b996b9623_1778803536.mp4"

# UUID of the primary (full-data) test entry
PRIMARY_UUID = "a9498e4f-70f0-4811-924e-f37b996b9623"


def token_response() -> dict:
    return {
        "token_type": "Bearer",
        "expires_in": 7200,
        "access_token": "test_access_token_abc123",
    }


def project_metadata() -> dict:
    """Minimal project definition — only the fields the script uses.

    - ``meta.project_mapping[0]`` (is_default) drives media field detection
    - ``data.project.forms[0].name`` → dataset_name
    - ``data.project.forms[0].inputs`` → ref/type pairs for media detection
    - ``data.project.logo_url`` non-empty → logo download is attempted
    """
    return {
        "meta": {
            "project_mapping": [
                {
                    "name": "EC5_AUTO",
                    "is_default": True,
                    "map_index": 0,
                    "forms": {
                        FORM_REF: {
                            _AUDIO_REF: {"map_to": "1_Record_audio"},
                            _PHOTO_REF: {"map_to": "2_Take_photo"},
                            _VIDEO_REF: {"map_to": "3_Record_video"},
                            _LOCATION_REF: {"map_to": "4_Record_location"},
                            _BARCODE_REF: {"map_to": "5_Scan_barcode"},
                        }
                    },
                },
            ],
        },
        "data": {
            "project": {
                "slug": PROJECT_SLUG,
                "logo_url": PROJECT_REF,  # non-empty → logo download attempted
                "forms": [
                    {
                        "name": FORM_NAME,
                        "inputs": [
                            {"ref": _AUDIO_REF, "type": "audio"},
                            {"ref": _PHOTO_REF, "type": "photo"},
                            {"ref": _VIDEO_REF, "type": "video"},
                            {"ref": _LOCATION_REF, "type": "location"},
                            {"ref": _BARCODE_REF, "type": "barcode"},
                        ],
                    }
                ],
            }
        },
    }


# ---------------------------------------------------------------------------
# Public-project fixture — photo field is a full URL, not a bare filename
# ---------------------------------------------------------------------------

PUBLIC_PROJECT_SLUG = "ec5-public-demo"
PUBLIC_PHOTO_FILENAME = "abc123def4-0000-0000-0000-000000000001_1493304122.jpg"
PUBLIC_ENTRY_UUID = "ee000000-0000-0000-0000-000000000001"
_PUBLIC_FORM_REF = "aabbccdd00001111222233334444555_pub_form_ref"
_PUBLIC_PHOTO_REF = f"{_PUBLIC_FORM_REF}_photo_ref"
_PUBLIC_TEXT_REF = f"{_PUBLIC_FORM_REF}_text_ref"


def public_project_metadata() -> dict:
    """Minimal metadata for a public project whose photo field is a full URL."""
    return {
        "meta": {
            "project_mapping": [
                {
                    "name": "EC5_AUTO",
                    "is_default": True,
                    "map_index": 0,
                    "forms": {
                        _PUBLIC_FORM_REF: {
                            _PUBLIC_TEXT_REF: {"map_to": "1_Name"},
                            _PUBLIC_PHOTO_REF: {"map_to": "2_Photo"},
                        }
                    },
                }
            ],
        },
        "data": {
            "project": {
                "slug": PUBLIC_PROJECT_SLUG,
                "logo_url": "",
                "forms": [
                    {
                        "name": "Public Demo Form",
                        "inputs": [
                            {"ref": _PUBLIC_TEXT_REF, "type": "text"},
                            {"ref": _PUBLIC_PHOTO_REF, "type": "photo"},
                        ],
                    }
                ],
            }
        },
    }


def public_entries_page() -> dict:
    """Single entry whose photo value is a full media URL (public project behaviour)."""
    photo_url = (
        f"https://five.epicollect.net/api/media/{PUBLIC_PROJECT_SLUG}"
        f"?type=photo&format=entry_original&name={PUBLIC_PHOTO_FILENAME}"
    )
    return {
        "meta": {
            "total": 1,
            "per_page": 250,
            "current_page": 1,
            "last_page": 1,
            "from": 1,
            "to": 1,
            "newest": "2026-05-15T00:00:00.000Z",
            "oldest": "2026-05-15T00:00:00.000Z",
        },
        "data": {
            "id": PUBLIC_PROJECT_SLUG,
            "type": "entries",
            "entries": [
                {
                    "ec5_uuid": PUBLIC_ENTRY_UUID,
                    "created_at": "2026-05-15T00:00:00.000Z",
                    "uploaded_at": "2026-05-15T00:00:05.000Z",
                    "created_by": "collector@example.com",
                    "title": "Test Person",
                    "1_Name": "Test Person",
                    "2_Photo": photo_url,
                }
            ],
            "mapping": {"map_name": "EC5_AUTO", "map_index": 0},
        },
        "links": {"self": "", "first": "", "prev": None, "next": None, "last": ""},
    }


def _build_entries_page(
    entries: list[dict], current_page: int, last_page: int, per_page: int
) -> dict:
    total = last_page * per_page if last_page > 1 else len(entries)
    base_url = (
        f"https://five.epicollect.net/api/export/entries/{PROJECT_SLUG}"
        f"?form_ref={FORM_REF}&per_page={per_page}&sort_by=created_at&sort_order=ASC&page="
    )
    return {
        "meta": {
            "total": total,
            "per_page": per_page,
            "current_page": current_page,
            "last_page": last_page,
            "from": (current_page - 1) * per_page + 1,
            "to": (current_page - 1) * per_page + len(entries),
            "newest": "2026-05-15T02:00:00.000Z",
            "oldest": "2026-05-15T00:06:27.000Z",
        },
        "data": {
            "id": PROJECT_SLUG,
            "type": "entries",
            "entries": entries,
            "mapping": {"map_name": "EC5_AUTO", "map_index": 0},
        },
        "links": {
            "self": f"{base_url}{current_page}",
            "first": f"{base_url}1",
            "prev": f"{base_url}{current_page - 1}" if current_page > 1 else None,
            "next": f"{base_url}{current_page + 1}"
            if current_page < last_page
            else None,
            "last": f"{base_url}{last_page}",
        },
    }


def _all_entries() -> list[dict]:
    """Four entries exercising all field types and edge cases.

    Entry 1 (PRIMARY_UUID): all 3 media attachments, valid GPS, radio answer,
        multi-select checkbox array, date, text, barcode.
    Entry 2: no GPS fix (empty-string lat/lon), no media, empty optional fields.
    Entry 3: valid GPS, unicode surveyor name, checkbox array, no media. Used as
        first entry of page 2 in the paginated fixture.
    Entry 4: no GPS, all optional fields blank. Second entry of page 2.
    """
    return [
        {
            "ec5_uuid": PRIMARY_UUID,
            "created_at": "2026-05-15T00:06:27.000Z",
            "uploaded_at": "2026-05-15T00:06:34.000Z",
            "created_by": "collector@example.com",
            "title": "Temperate oak stand",
            "1_Record_audio": AUDIO_FILENAME,
            "2_Take_photo": PHOTO_FILENAME,
            "3_Record_video": VIDEO_FILENAME,
            "4_Record_location": {
                "latitude": 4.711,
                "longitude": -74.072,
                "accuracy": 30,
                "UTM_Northing": 520631,
                "UTM_Easting": 610234,
                "UTM_Zone": "18N",
            },
            "5_Scan_barcode": "027917270609",
            "6_Survey_date": "15/05/2026",
            "7_Surveyor_name": "Field Collector",
            "8_Tree_species": "Oak",
            "9_Observed_features": ["Epiphytes", "NestingBirds"],
        },
        {
            # No GPS fix — latitude/longitude are empty strings, as the real API returns
            "ec5_uuid": "b1234567-0000-0000-0000-000000000001",
            "created_at": "2026-05-15T01:00:00.000Z",
            "uploaded_at": "2026-05-15T01:00:10.000Z",
            "created_by": "collector@example.com",
            "title": "No GPS fix entry",
            "1_Record_audio": "",
            "2_Take_photo": "",
            "3_Record_video": "",
            "4_Record_location": {
                "latitude": "",
                "longitude": "",
                "accuracy": "",
                "UTM_Northing": "",
                "UTM_Easting": "",
                "UTM_Zone": "",
            },
            "5_Scan_barcode": "123456789",
            "6_Survey_date": "15/05/2026",
            "7_Surveyor_name": "Another Collector",
            "8_Tree_species": "",
            "9_Observed_features": [],
        },
        {
            # Unicode name; valid GPS; multi-select; no media
            "ec5_uuid": "c2345678-0000-0000-0000-000000000002",
            "created_at": "2026-05-15T02:00:00.000Z",
            "uploaded_at": "2026-05-15T02:00:10.000Z",
            "created_by": "colector@ejemplo.com",
            "title": "Bosque tropical húmedo",
            "1_Record_audio": "",
            "2_Take_photo": "",
            "3_Record_video": "",
            "4_Record_location": {
                "latitude": -3.465,
                "longitude": -62.216,
                "accuracy": 10,
                "UTM_Northing": 9616782,
                "UTM_Easting": 613891,
                "UTM_Zone": "20S",
            },
            "5_Scan_barcode": "",
            "6_Survey_date": "15/05/2026",
            "7_Surveyor_name": "José María",
            "8_Tree_species": "Pine",
            "9_Observed_features": ["Fungi"],
        },
        {
            # Minimal entry — all optional fields blank, used as page-2 entry in paginated fixture
            "ec5_uuid": "d3456789-0000-0000-0000-000000000003",
            "created_at": "2026-05-15T03:00:00.000Z",
            "uploaded_at": "2026-05-15T03:00:10.000Z",
            "created_by": "collector@example.com",
            "title": "Minimal entry",
            "1_Record_audio": "",
            "2_Take_photo": "",
            "3_Record_video": "",
            "4_Record_location": {
                "latitude": "",
                "longitude": "",
                "accuracy": "",
                "UTM_Northing": "",
                "UTM_Easting": "",
                "UTM_Zone": "",
            },
            "5_Scan_barcode": "",
            "6_Survey_date": "",
            "7_Surveyor_name": "Minimal",
            "8_Tree_species": "",
            "9_Observed_features": [],
        },
    ]


def entries_page(page: int = 1, per_page: int = 250) -> dict:
    """Single-page response returning the first three entries."""
    return _build_entries_page(
        _all_entries()[:3], current_page=1, last_page=1, per_page=per_page
    )


def entries_empty(page: int = 1, per_page: int = 250) -> dict:
    """Single-page response with zero entries."""
    return _build_entries_page([], current_page=1, last_page=1, per_page=per_page)


def entries_paginated(page: int = 1, per_page: int = 2) -> dict:
    """Two-page response — 2 entries per page, 4 entries total."""
    all_e = _all_entries()
    start = (page - 1) * per_page
    page_entries = all_e[start : start + per_page]
    last_page = (len(all_e) + per_page - 1) // per_page
    return _build_entries_page(
        page_entries, current_page=page, last_page=last_page, per_page=per_page
    )
