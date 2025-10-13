def arcgis_token():
    return {
        "token": "token_value",
        "expires": 1741109789251,
        "ssl": True,
    }


def arcgis_features():
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "id": 1,
                "geometry": {
                    "type": "Point",
                    "coordinates": [-73.965355, 40.782865],
                },
                "properties": {
                    "objectid": 1,
                    "globalid": "12345678-1234-5678-1234-567812345678",
                    "CreationDate": 1741017108116,
                    "Creator": "arcgis_account",
                    "EditDate": 1741017108116,
                    "Editor": "arcgis_account",
                    "what_is_your_name": "Community mapper",
                    "what_is_your_community": "Springfield",
                    "what_is_your_community_other": None,
                    "what_is_the_date_and_time": 1741017060000,
                    "did_you_like_this_survey": 7,
                },
            }
        ],
    }


def arcgis_attachments():
    return {
        "attachmentInfos": [
            {
                "id": 1,
                "globalId": "ab12cd34-56ef-78gh-90ij-klmn12345678",
                "parentGlobalId": "12345678-1234-5678-1234-567812345678",
                "name": "springfield_photo.png",
                "contentType": "image/png",
                "size": 3632,
                "keywords": "add_a_photo",
                "exifInfo": None,
            },
            {
                "id": 2,
                "globalId": "mnop5678-qrst-uvwx-yzab-cdef98765432",
                "parentGlobalId": "12345678-1234-5678-1234-567812345678",
                "name": "springfield_audio.mp4",
                "contentType": "audio/webm;codecs=opus",
                "size": 920,
                "keywords": "add_an_audio",
                "exifInfo": None,
            },
        ]
    }


def arcgis_metadata_anonymous():
    """Metadata response for anonymous feature server"""
    return {
        "currentVersion": 11.1,
        "serviceDescription": "Test Anonymous Feature Layer",
        "layers": [
            {
                "id": 0,
                "name": "Test Anonymous Layer",
                "type": "Feature Layer",
                "geometryType": "esriGeometryPoint",
            }
        ],
        "tables": [],
        "spatialReference": {"wkid": 102100, "latestWkid": 3857},
    }


def arcgis_features_anonymous():
    """Features response for anonymous feature server (returns raw ArcGIS format, not GeoJSON)"""
    return {
        "objectIdFieldName": "OBJECTID",
        "globalIdFieldName": "globalid",
        "geometryType": "esriGeometryPoint",
        "spatialReference": {"wkid": 102100, "latestWkid": 3857},
        "fields": [
            {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID"},
            {"name": "globalid", "type": "esriFieldTypeGlobalID", "alias": "GlobalID"},
            {
                "name": "what_is_your_name",
                "type": "esriFieldTypeString",
                "alias": "What is your name?",
            },
            {
                "name": "what_is_the_date_and_time",
                "type": "esriFieldTypeDate",
                "alias": "What is the date and time?",
            },
        ],
        "features": [
            {
                "attributes": {
                    "OBJECTID": 1,
                    "globalid": "12345678-1234-5678-1234-567812345678",
                    "what_is_your_name": "Community mapper",
                    "what_is_the_date_and_time": 1741017060000,
                },
                "geometry": {
                    "x": -8228661.123,
                    "y": 4972614.456,
                    "spatialReference": {"wkid": 102100, "latestWkid": 3857},
                },
            }
        ],
    }
