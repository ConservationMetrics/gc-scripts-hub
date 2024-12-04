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

### Development

You may develop within Windmill's code editor, or locally.  Developing locally has the advantage
of being able to run tests.

If you developed on the server, sync your remote changes into Git version control once done:

    wmill sync pull --skip-variables  # optionally add --raw to clobber your local repo
    # TODO: git add, commit, etc


# Running Windmill

[You may get a free cloud workspace, or self-host.](https://www.windmill.dev/docs/getting_started/how_to_use_windmill)

We recommend self-hosting using [CapRover](https://caprover.com/) and its
[Windmill one-click-app](https://github.com/caprover/one-click-apps/blob/master/public/v4/apps/windmill.yml).
