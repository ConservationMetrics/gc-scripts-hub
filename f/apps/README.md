# Apps

This directory contains a collection of Windmill apps, which are customized UIs to interact with data sources and trigger scripts or flows to run based on inputs.

Windmill apps are created using the [app editor UI](https://www.windmill.dev/docs/apps/app_editor). Once created, you can run `wmill sync pull` to retrieve the app code, which is composed of a YAML config file and any inline scripts that you created for the app. The app will be saved in a `«appname».app` directory here.