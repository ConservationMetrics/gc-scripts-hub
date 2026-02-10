# Local Contexts: Pull Labels

This script fetches [Traditional Knowledge (TK) Labels](https://localcontexts.org/labels/traditional-knowledge-labels/) and [Biocultural (BC) Labels](https://localcontexts.org/labels/biocultural-labels/) applied to a Local Contexts project from the Local Contexts Hub API and writes them to a PostgreSQL database. The script also downloads the label assets to a specified directory on the volume mount.

## How to set up a Local Contexts project with TK and BC labels

**For more information, see**: https://localcontexts.org/support/getting-started-on-the-hub/

1. Create a Local Contexts Hub user profile
2. Create a Local Contexts Hub community account
3. Get your account [confirmed](https://localcontexts.org/support/getting-started-on-the-hub/#confirmation-step)
4. Customize the labels that you want to use
5. Have another editor or admin user approve your labels
6. Create a Local Contexts Hub project
  - You probably want to select "Collection" as the project type, and set visibility to "Private" to keep the labels private to your community.
7. Apply the TK and BC labels to the project
8. Get the **project ID**, and an **API key** for your community account

## Endpoints

### `GET /projects/{project_id}`

```json
{
  "data": [
    {
    "unique_id": "1d220c8c-1a9b-449c-a3d8-2baf48491336",
    ...
    "project_page": "https://sandbox.localcontextshub.org/projects/1d220c8c-1a9b-449c-a3d8-2baf48491336",
    "title": "Guardian Connector LC labels",
    "description": "Local Contexts labels for Guardian Connector",
    "project_type": "Collection",
    ...
    "date_added": "2026-02-09T20:35:29.519523Z",
    "date_modified": "2026-02-09T20:47:35.279601Z",
    ...
    "bc_labels": [
        {
            "unique_id": "73d43de7-172c-450b-808a-12c32c252a8d",
            "name": "BC Non-Commercial (BC NC)",
            "label_text": "This Label is being used to indicate that these biocultural materials and/or data have been designated as being available for non-commercial use.",
            "img_url": "https://localcontexts.org/wp-content/uploads/2025/04/bc-non-commercial.png",
            ...
        }
    ],
    "tk_labels": [
        {
            "unique_id": "583aeb2b-cad8-48e8-ae0c-9fe9d1d64a24",
            ...
        }
    ]
    }
  ]
}
```

## 📚 Reference

* API Guide: https://localcontexts.org/support/api-guide/v2/
* Swagger UI: https://sandbox.localcontextshub.org/api/v2/docs