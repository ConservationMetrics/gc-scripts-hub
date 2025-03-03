# `arcgis_feature_layer`: Fetch Feature Layer from ArcGIS REST API

The feature layer URL can be found on the item details page of your layer on ArcGIS Online:

![Screenshot of a feature layer item page](arcgis.jpg)

This script uses the [ArcGIS REST API Query Feature Service / Layer](https://developers.arcgis.com/rest/services-reference/enterprise/query-feature-service-layer/) endpoint.

Note: we have opted not to use the [ArcGIS API for Python](https://developers.arcgis.com/python/latest/) library because it requires installing `libkrb5-dev` as a system-level dependency. Workers in Windmill can [preinstall binaries](https://www.windmill.dev/docs/advanced/preinstall_binaries), but it requires modifying the Windmill `docker-compose.yml`, which is too heavy-handed an approach for this simple fetch script.