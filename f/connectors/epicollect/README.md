# EpiCollect5: Fetch Survey Entries

[**EpiCollect5**](https://five.epicollect.net/) is a mobile & web application for free and easy data collection developed by the CGPS Team at the Oxford University Big Data Institute. It provides web and mobile applications for building forms and freely hosted project websites. Data — including GPS and media — are collected across multiple devices and centralised on a single server for viewing via maps, tables, and charts.

## `epicollect_pull.py`

This script fetches project metadata and survey entries from the [EpiCollect5 API](https://developers.epicollect.net/). Project metadata and a project logo are saved to disk. Survey entries are written to a PostgreSQL table. Media attachments (photo, audio, video) are downloaded and saved to a specified directory.

## EpiCollect5 project parameters

EpiCollect5 uses OAuth2 client credentials. Create a project App from your project's **Apps** page to obtain a **Client ID** and **Client Secret**. The script exchanges these for a Bearer token (valid 2 hours) before each run.

The **project slug** appears in the project URL: `https://five.epicollect.net/project/{slug}`. It can also be found on the project's **API** tab in the web application.

> [!NOTE]
> Since the Client ID and Secret are project-specific, they are not bundled together in a single resource as there is no reusability of credentials across projects. Hence, for this script, we don't take advantage of Windmill resource types.

## 📚 Reference

* [EpiCollect5 User Guide](https://docs.epicollect.net/)
* [EpiCollect5 API Documentation](https://developers.epicollect.net/)
