SAMPLE_OBSERVATIONS = [
    {
        "docId": "doc_id_1",
        "createdAt": "2024-10-14T20:18:14.206Z",
        "updatedAt": "2024-10-14T20:18:14.206Z",
        "deleted": False,
        "lat": -33.8688,
        "lon": 151.2093,
        "tags": {
            "notes": "Rapid",
            "type": "water",
            "status": "active",
            "created_at": "village",
        },
    },
    {
        "docId": "doc_id_2",
        "createdAt": "2024-10-15T21:19:15.207Z",
        "updatedAt": "2024-10-15T21:19:15.207Z",
        "deleted": False,
        "lat": 48.8566,
        "lon": 2.3522,
        "attachments": [
            {
                "url": "http://comapeo.example.org/projects/forest_expedition/attachments/drive_discovery_doc_id_2/photo/capybara.jpg"
            }
        ],
        "tags": {
            "notes": "Capybara",
            "type": "animal",
            "animal-type": "capybara",
        },
    },
    {
        "docId": "doc_id_3",
        "createdAt": "2024-10-16T22:20:16.208Z",
        "updatedAt": "2024-10-16T22:20:16.208Z",
        "deleted": False,
        "lat": 35.6895,
        "lon": 139.6917,
        "tags": {
            "notes": "Former village site",
            "type": "location",
            "status": "historical",
        },
    },
]


def comapeo_projects(uri):
    return {
        "data": [
            {
                "projectId": "forest_expedition",
                "name": "Forest Expedition",
            },
            {
                "projectId": "river_mapping",
                "name": "River Mapping",
            },
        ]
    }


def comapeo_project_observations(uri, project_id):
    # Update attachment URLs to use the provided URI and project_id
    # New route format: /projects/:projectPublicId/attachments/:driveDiscoveryId/:type/:name
    observations = []
    for obs in SAMPLE_OBSERVATIONS:
        obs_copy = obs.copy()
        if "attachments" in obs_copy:
            for attachment in obs_copy["attachments"]:
                # Use a mock driveDiscoveryId for test data
                drive_discovery_id = f"drive_discovery_{obs_copy['docId']}"
                attachment["url"] = (
                    f"{uri}/projects/{project_id}/attachments/{drive_discovery_id}/photo/capybara.jpg"
                )
        observations.append(obs_copy)

    return {"data": observations}


def comapeo_alerts():
    return {
        "data": [
            {
                "detectionDateStart": "2024-11-03T04:20:69Z",
                "detectionDateEnd": "2024-11-04T04:20:69Z",
                "sourceId": "abc123",
                "metadata": {"foo": "bar"},
                "geometry": {"type": "Point", "coordinates": [12, 34]},
            }
        ]
    }
