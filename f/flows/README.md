# Flows

This directory contains a collection of Windmill flows, which are step-by-step automated processes that runs tasks in order, making sure each step happens at the right time and with the right data.

Windmill flows are created using the [flow editor UI](https://www.windmill.dev/docs/flows/flow_editor). Once created, you can run `wmill sync pull` to retrieve the flow code, which is composed of a YAML config file. The flow will be saved in a `«flowname».flow` directory here.