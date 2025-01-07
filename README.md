# Guardian Connector Scripts Hub

This repository contains scripts and flows to help communities guard and manage their land.

The scripts and flows are intended to run on [Windmill](https://www.windmill.dev/), a platform that
can turn scripts into workflows and UIs.  It empowers semi-technical users to access and edit code
without being overwhelmed by the usual barriers to entry (git, IDE, local environments, secrets
managements, etc).


## Deploying these scripts to a Windmill workspace

[Install the Windmill CLI](https://www.windmill.dev/docs/advanced/cli), and
[set it up to talk to your deployed Workspace](https://www.windmill.dev/docs/advanced/cli/workspace-management).

Then push the code in this Git repo your workspace:

    wmill sync push --skip-variables

Folders named `./tests/` are excluded by `wmill.yml` from syncing to Windmill —
because otherwise Windmill tries to make the tests (as with ALL python files) into bona-fide Windmill scripts.

### Development

In Windmill, scripts and flows can be written in Python, TypeScript, Go, and a number of other languages.

The `f/` directory is designated for storing code in a workspace folder, and will be used when synchronizing the contents of this repository with a server.

Within the `f/` directory, we store code in directories that represent a specific set of tasks. For example, the `f/connectors/` directory contains scripts for data ETL and pipelining tasks.

Note that Windmill also designates a `u/` directory for storing code per user on a workspace. We are not using this convention in this repository. See [Windmill's local development guide](https://www.windmill.dev/docs/advanced/local_development) for more information on these directories and how they are synchronized with a server.

For information on developing scripts, see the [Windmill Scripts quickstart](https://www.windmill.dev/docs/getting_started/scripts_quickstart).

While Windmill allows [sharing common logic across scripts with relative imports](https://www.windmill.dev/docs/advanced/sharing_common_logic), we have chosen to avoid this approach for now. This decision aims to keep each script as self-contained as possible, promoting modularity and reducing interdependencies. Additionally, sharing code across scripts is challenging because each script has its own dependency stack. Shared code must either work across varying dependencies or explicitly declare its own, making management cumbersome without creating a proper package. As a trade-off resulting from this decision, some redundancy may exist in common operations, such as database writes.

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


# Running Windmill

[You may get a free cloud workspace, or self-host.](https://www.windmill.dev/docs/getting_started/how_to_use_windmill)

We recommend self-hosting using [CapRover](https://caprover.com/) and its
[Windmill one-click-app](https://github.com/caprover/one-click-apps/blob/master/public/v4/apps/windmill.yml).
