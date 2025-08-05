# Guardian Connector Scripts Hub

This repository contains scripts, flows and apps to help communities guard and manage their land.

The code is intended to run on [Windmill](https://www.windmill.dev/), a platform that
can turn scripts into workflows and UIs.  It empowers semi-technical users to access, edit and schedule code 
to run on a given interval without being overwhelmed by the usual barriers to entry (git, IDE, local environments,
secrets managements, etc).

🌱 [Read a blog post about Windmill is being used in this repository for supporting Indigenous communities](https://www.windmill.dev/blog/conservation-metrics-case-study)


## Available scripts, flows, and apps

Some of the tools available in the Guardian Connector Scripts Hub are:

* Connector scripts to ingest data from data collection or annotation tools such as KoboToolbox, ODK, CoMapeo, ArcGIS, Global Forest Watch, Timelapse, and Locus Map, 
  and store this data (tabular and media attachments) in a data lake. 
* A flow to download and store GeoJSON and GeoTIFF change detection alerts, post these to a CoMapeo Archive Server 
  API, and send a message to WhatsApp recipients via Twilio.
* Scripts to export data from a database into a specific format (e.g., GeoJSON).
* An app to import and transform datasets from a variety of file formats and sources into a PostgreSQL database.

![Available scripts, flows, and apps in gc-scripts-hub](gc-scripts-hub.jpg)
_A Windmill Workspace populated with some of the tools in this repository._

## Deploying the code to Windmill workspaces

[Install the Windmill CLI](https://www.windmill.dev/docs/advanced/cli), and
[set it up to talk to your deployed Workspace](https://www.windmill.dev/docs/advanced/cli/workspace-management).

Then push the code in this Git repo to your workspace:

    wmill sync push --skip-variables

Folders named `./tests/` are excluded by `wmill.yml` from syncing to Windmill —
because otherwise Windmill tries to make the tests (as with ALL python files) into bona-fide Windmill scripts.

This repo also provides a shell script to batch push changes to a number of workspaces at once. To use this, set the `WORKSPACES` environment variable with a list of workspace names. You can do this directly in the command line:

       WORKSPACES=gc-windmill,gc-testing-server bin/push.sh

   Alternatively, use a subshell to load a WORKSPACES variable from a `.env` file without affecting your current shell environment:

       (set -a; source .env; set +a; bin/push.sh)

## Development

In Windmill, scripts can be written in Python, TypeScript, Go, and a number of other languages. Flows and apps can 
be built through the Windmill UI.

The `f/` directory is designated for storing code in a workspace folder, and will be used when synchronizing the contents 
of this repository with a server.

Within the `f/` directory, we store code in directories that represent a specific set of tasks. For example, the 
`f/connectors/` directory contains scripts for data ETL and pipelining tasks.

Note that Windmill also designates a `u/` directory for storing code per user on a workspace. We are not using this 
convention in this repository. See [Windmill's local development guide](https://www.windmill.dev/docs/advanced/local_development) 
for more information on these directories and how they are synchronized with a server.

For information on developing scripts, see the [Windmill Scripts quickstart](https://www.windmill.dev/docs/getting_started/scripts_quickstart).

You may develop within Windmill's code editor, or locally.  Developing locally has the advantage
of being able to run tests.

If you developed on the server, sync your remote changes into Git version control once done:

    wmill sync pull --skip-variables  # optionally add --raw to clobber your local repo
    # TODO: git add, commit, etc


## Running Tests

You can run tests using tox:

    tox

The virtual env for each script under test is defined according to the
`«scriptname».script.lock` file that Windmill creates.  This means that you'll need to have
Windmill create it, either by running the script being tested _at least once in Windmill itself_, or using the CLI:

    wmill script generate-metadata

For more about how Windmill chooses the package dependencies to go in these
metadata/lock files, read https://www.windmill.dev/docs/advanced/imports#imports-in-python

It also means that `tox` creates different environments for each "folder" of scripts.
To run tests for only one folder, specify the folder as an `-e «environment»` CLI arg:

    tox -e alerts

Note that the versions of package dependencies must be the same for scripts across a tox environment,
or you will get an error about conflicting dependencies.

# Running Windmill

[You may get a free cloud workspace, or self-host.](https://www.windmill.dev/docs/getting_started/how_to_use_windmill)

We recommend self-hosting using [CapRover](https://caprover.com/) and its
[Windmill one-click-app](https://github.com/caprover/one-click-apps/blob/master/public/v4/apps/windmill.yml).
