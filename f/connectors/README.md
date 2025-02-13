# Connectors

This directory contains a collection of lightweight and flexible scripts 
and flows designed to enable seamless integration of multiple data sources.

These connectors can fetch data from various upstream sources, such as 
KoboToolbox, CoMapeo, or Google Cloud Platform. Data from these sources 
is ingested in a consistent way. The scripts ensure that the data is cleansed, 
validated, and formatted appropriately before being transferred to its target 
destination, such as a SQL database, file storage, communication services, or 
other handling methods.

Using Windmill, the scripts can be scheduled to run at defined intervals, ensuring 
timely data processing.

Windmill flows are created using the [flow editor UI](https://www.windmill.dev/docs/flows/flow_editor). 
Once created, you can run `wmill sync pull` to retrieve the flow code, 
which is composed of a YAML config file. The flow will be saved in 
a `«flowname».flow` directory here.