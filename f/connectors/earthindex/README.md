# `earthindex_pull`: Fetch Features from Earth Index API

[Earth Index](https://earthindex.ai) is an AI-powered search tool developed by non-profit [Earth Genome](https://earthgenome.org) to identify environmental features (e.g., illegal mining, deforestation, solar farms) using satellite imagery and machine learning. It acts as a search engine for the planet, allowing users to find specific objects in minutes by analyzing "genetic signatures" or "embeddings" from satellite data. 

This script fetches features that are detected in a project from the Earth Index API.


## Endpoints

The request header must include an access token in the format: 

    Authorized: Bearer <token>.

### `GET /v1/projects/<project_id>`

```json
{
  "id": "a1b2c3d4-e5f6-g7h8-i9j0-k1l2m3n4o5p6",
  "title": "Springfield mapping",
  "description": "Mapping special places in Springfield",
  "targetLabel": "",
  "layers": [
    {
      "id": "12345678-aaaa-bbbb-cccc-000000001111",
      "projectId": "a1b2c3d4-e5f6-g7h8-i9j0-k1l2m3n4o5p6",
      "snapshotId": "76923014-1337-2024-4242-987654321000",
      "selectedImagery": "satellite",
      "additionalMetadata": {
        "confidenceThreshold": 0.9407004614878623
      },
      "numResults": 500,
      "placeLabels": true,
      "points": null,
      "timePeriods": [
        {
          "startTime": "2024-01-01T00:00:00Z",
          "endTime": "2025-01-01T00:00:00Z"
        }
      ],
      "createdAt": "2026-03-18T18:44:30.105967Z",
      "updatedAt": "2026-03-18T18:48:06.430431Z",
      "public": true
    }
  ],
  "reference_layers": null,
  "geometry": {
    "type": "Polygon",
    "coordinates": [
      [
        [
          -77.36311765959915,
          38.711849825335435
        ],
        [
          -77.08730800803413,
          38.711849825335435
        ],
        [
          -77.08730800803413,
          38.90231389323236
        ],
        [
          -77.36311765959915,
          38.90231389323236
        ],
        [
          -77.36311765959915,
          38.711849825335435
        ]
      ]
    ]
  },
  "additionalMetadata": {
    "geoId": "",
    "geoName": ""
  },
  "createdAt": "2026-03-18T18:44:29.899787Z",
  "updatedAt": "2026-03-18T18:52:45.238099Z",
  "pointCount": 0,
  "public": true
}
```

### `GET /v1/projects/<project_id>/layers`

```json
[
  {
    "id": "12345678-aaaa-bbbb-cccc-000000001111",
    "projectId": "22223333-4444-5555-6666-777788889999",
    "snapshotId": "76923014-1337-2024-4242-987654321000",
    "selectedImagery": "satellite",
    "additionalMetadata": {
      "confidenceThreshold": 0.9407004614878623
    },
    "numResults": 500,
    "placeLabels": true,
    "points": null,
    "timePeriods": [
      {
        "startTime": "2024-01-01T00:00:00Z",
        "endTime": "2025-01-01T00:00:00Z"
      }
    ],
    "createdAt": "2026-03-18T18:44:30.105967Z",
    "updatedAt": "2026-03-18T18:48:06.430431Z",
    "public": false
  }
]

```

### `GET /v1/projects/<project_id>/layers/<layer_id>/points`

```json
{ 
    "features": [
        {
            "id": "dqchu0s1dg6",
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    ...
                ]
            },
            "properties": {
                "label": "prediction",
                "score": 0.9192034508686123
            }
        },
        ...
    ],
    ...
}

## 📚 Reference

* API Guide: https://api.earthindex.dev/docs/index.html