SAMPLE_PROJECT = {
    "id": "a1b2c3d4-e5f6-g7h8-i9j0-k1l2m3n4o5p6",
    "title": "Springfield mapping",
    "description": "Mapping special places in Springfield",
    "targetLabel": "",
    "layers": [
        {
            "id": "f1af7315-45e0-4eb6-9d1a-5fba73be2804",
            "projectId": "a1b2c3d4-e5f6-g7h8-i9j0-k1l2m3n4o5p6",
            "snapshotId": "76923014-1337-2024-4242-987654321000",
            "selectedImagery": "satellite",
            "additionalMetadata": {"confidenceThreshold": 0.9407004614878623},
            "numResults": 500,
            "placeLabels": True,
            "points": None,
            "timePeriods": [
                {
                    "startTime": "2024-01-01T00:00:00Z",
                    "endTime": "2025-01-01T00:00:00Z",
                }
            ],
            "createdAt": "2026-03-18T18:44:30.105967Z",
            "updatedAt": "2026-03-18T18:48:06.430431Z",
            "public": True,
        }
    ],
    "reference_layers": None,
    "geometry": {
        "type": "Polygon",
        "coordinates": [
            [
                [-77.36311765959915, 38.711849825335435],
                [-77.08730800803413, 38.711849825335435],
                [-77.08730800803413, 38.90231389323236],
                [-77.36311765959915, 38.90231389323236],
                [-77.36311765959915, 38.711849825335435],
            ]
        ],
    },
    "additionalMetadata": {"geoId": "", "geoName": ""},
    "createdAt": "2026-03-18T18:44:29.899787Z",
    "updatedAt": "2026-03-18T18:52:45.238099Z",
    "pointCount": 0,
    "public": True,
}

SAMPLE_LAYERS = [
    {
        "id": "f1af7315-45e0-4eb6-9d1a-5fba73be2804",
        "projectId": "a1b2c3d4-e5f6-g7h8-i9j0-k1l2m3n4o5p6",
        "snapshotId": "76923014-1337-2024-4242-987654321000",
        "selectedImagery": "satellite",
        "additionalMetadata": {"confidenceThreshold": 0.9407004614878623},
        "numResults": 500,
        "placeLabels": True,
        "points": None,
        "timePeriods": [
            {
                "startTime": "2024-01-01T00:00:00Z",
                "endTime": "2025-01-01T00:00:00Z",
            }
        ],
        "createdAt": "2026-03-18T18:44:30.105967Z",
        "updatedAt": "2026-03-18T18:48:06.430431Z",
        "public": False,
    }
]

SAMPLE_POINTS = {
    "features": [
        {
            "id": "dqchu0s1dg6",
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-77.16419898560378, 38.80529560190361],
                        [-77.16051027204263, 38.80529560190361],
                        [-77.16051027204263, 38.80817017471015],
                        [-77.16419898560378, 38.80817017471015],
                        [-77.16419898560378, 38.80529560190361],
                    ]
                ],
            },
            "properties": {"label": "prediction", "score": 0.9192034508686123},
        },
        {
            "id": "dqchurbsjcr",
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-77.1578839450482, 38.845539621195186],
                        [-77.1541931515276, 38.845539621195186],
                        [-77.1541931515276, 38.84841419400173],
                        [-77.1578839450482, 38.84841419400173],
                        [-77.1578839450482, 38.845539621195186],
                    ]
                ],
            },
            "properties": {"label": "prediction", "score": 0.9387758293762569},
        },
        {
            "id": "dqcj0nwmwxw",
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-77.33672516899342, 38.882909067680224],
                        [-77.33303244468607, 38.882909067680224],
                        [-77.33303244468607, 38.885783640486764],
                        [-77.33672516899342, 38.885783640486764],
                        [-77.33672516899342, 38.882909067680224],
                    ]
                ],
            },
            "properties": {"label": "prediction", "score": 0.9387120504676387},
        },
        {
            "id": "dqch8bmnh1t",
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-77.30558912077335, 38.76073972340222],
                        [-77.30190263583674, 38.76073972340222],
                        [-77.30190263583674, 38.76361429620876],
                        [-77.30558912077335, 38.76361429620876],
                        [-77.30558912077335, 38.76073972340222],
                    ]
                ],
            },
            "properties": {"label": "positive", "score": 0.94728232531232},
        },
        {
            "id": "dqch8cnz5jr",
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-77.30322580645162, 38.76505158261203],
                        [-77.29953917050692, 38.76505158261203],
                        [-77.29953917050692, 38.76792615541857],
                        [-77.30322580645162, 38.76792615541857],
                        [-77.30322580645162, 38.76505158261203],
                    ]
                ],
            },
            "properties": {"label": "positive", "score": 0.9523669567999482},
        },
        {
            "id": "dqch90ey6vm",
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-77.2963729084318, 38.76217700980549],
                        [-77.2926864234952, 38.76217700980549],
                        [-77.2926864234952, 38.76505158261203],
                        [-77.2963729084318, 38.76505158261203],
                        [-77.2963729084318, 38.76217700980549],
                    ]
                ],
            },
            "properties": {"label": "positive", "score": 0.9487162716015689},
        },
        {
            "id": "dqchfy3cg3f",
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-77.22219944234871, 38.836915902775566],
                        [-77.21850910283747, 38.836915902775566],
                        [-77.21850910283747, 38.839790475582106],
                        [-77.22219944234871, 38.839790475582106],
                        [-77.22219944234871, 38.836915902775566],
                    ]
                ],
            },
            "properties": {"label": "negative", "score": 0.94182509518789},
        },
    ],
    "layerId": "f1af7315-45e0-4eb6-9d1a-5fba73be2804",
    "type": "FeatureCollection",
}
