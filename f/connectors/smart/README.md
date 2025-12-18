# SMART

The SMART (Spatial Monitoring And Reporting Tool) platform consists of a set of software and analysis tools designed to help conservationists manage and protect wildlife and wild places. SMART can help standardize and streamline data collection, analysis, and reporting, making it easier for key information to get from the field to decision-makers.

For more information on SMART, see the [SMART website](https://www.smartconservationsoftware.org/) and [Technical Manuals](https://smartconservationtools.org/en-us/Resources/SMART-Manuals).

## SMART Desktop

SMART Desktop is a desktop application that serves as the central database for information recorded on animals, illegal activities, and conservation actions.

### `smart_patrols.py`

This script imports patrol XML data exported from SMART Desktop, extracting observations with full context (patrol, leg, day, waypoint metadata) and saving them to a PostgreSQL database. Each observation includes inherited contextual information such as patrol team, transport type, waypoint coordinates, and observation attributes.

The XML file is also saved to the project folder for reference and can be used to re-import the data later.

> [!TIP]
> You can export patrol data from SMART Desktop by going to the "Patrols" menu and right clicking on a patrol and selecting "Export".

## SMART Connect

SMART Connect allows SMART users to store and manage data on a web connected database.

**TODO: Add SMART Connect scripts**: see [`gc-programs/smart/smart-connect`](https://github.com/ConservationMetrics/gc-programs/tree/main/smart/smart-connect) for more information on how to set up a SMART Connect server.


## 📚 Reference

* SMART Connect API documentation: https://app.assembla.com/spaces/smart-cs/wiki/SMART_Connect_API_Documentation

In the SMART Connect Manual, it is noted:

> Connect API
> 
> The SMART Connect API is useful for other application developers who want to interact
> with SMART Connect directly from their own software. Other systems can be setup to
> Run queries and reports and process that data automatically into their own analysis
> etc. The other systems can also be built to send new data observations to be stored in
> SMART Connect as well.
> 
> It’s recommended to discuss any plans of additional software system with the SMART
> Partnership Technical Council so they can help your collaboration be as smooth and
> efficient as possible.