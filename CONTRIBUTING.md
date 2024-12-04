Thank you for contributing to the GuardianConnector Scripts Hub project!
Here are some conventions we use to smooth things along.

# Using GitHub

We use GitHub to host code, track issues, and accept pull requests.

## Git Branching

**We build and release from the `main` branch, so code merged here should always be stable.**

Prefer short lived features branches.
- Piecemeal progress towards broad code changes should merge to long-running branches until
  everything there is stable and deployable, at which point the long-running branch gets merged
  to `main`.
- Since short, coherent patches are easier to review, we code-review the individual PRs into
  the long-running feature branch (instead of review when merging the long-running branch to
  `main`)


## GitHub Issues & Labels

Create new Github Issues using the templates wherever possible.

## PR Review & Committing code

Multiple commits or PRs can be created for an Issue. e.g. each implementation step might get its own PR.

Code review is encouraged as a powerful tool for learning.  Benefits include
- Spread knowledge of the code base throughout the team: reviewing code is a remarkably effective way to learn the codebase.
- Expose everyone to different approaches.
- Ensure code is readable (and therefore maintainable).
- Yield better software (but ultimately the responsibility
  for bug-free code is on the code author, not the reviewer).

Code review is not limited to approval/rejection of PRs. Also consider involving a collaborator
earlier in the process, before the code is finished.  Ask them for a narrower review—e.g., a
design review or to focus on a specific part of the code change.

As a reviewer:
- be kind & helpful, but do not be "nice" for the sake of avoiding conflict.
- "I find this very hard to follow" is valid feedback, even if the code's behavior is technically correct.
- Ability for code review to find defects diminishes with longer PRs: Feel free to reject any
  review that adds more than 400 lines of new code. (no upper limit on deletions!)

Merging branches and PRs to `main`:
- The branch author should be the one to merge.
- Merge PRs via 'Merge Commit' option in GitHub.
- Delete a branch when you are done with it.


# Tests

## Testing Strategy

Keep the test suite
* **complete**: every feature needs a test.
* **focused**: test behavior, not implementation.
* **stable**: tests should be deterministic and fully repeatable.
* **fast**: no sleeps, no time-based tests.

## CircleCI

All tests in the repo should be executed by CircleCI (see `.circleci/config.yml`)
automatically upon push to any branch.  (This can be disabled on a per-commit basis
by appending a **`[ci skip]`** line to the commit message.)

Do not merge branches if CircleCI is not green.


# Coding & Documentation Style

## Prettier & ESLint for JavaScript, JSON, & SCSS

All developers are expected to use [prettier](https://prettier.io/) to format their JavaScript or TypeScript scripts in this repository.

> To apply Prettier autoformatting, you can configure your text editor to format on save [according
> to these docs](https://prettier.io/docs/en/editors.html)
> - For VS Code, set `editor.formatOnSave` to true in your editor config.


ESLint helps identify problematic patterns found in JavaScript code.

> Follow [ESLint Getting Started](https://eslint.org/docs/user-guide/getting-started).
> To configure ESLint in VS Code, create a `.vscode/settings.json` in the project's root and add
> ```
>    "eslint.nodePath": "/home/\${user}/dev/nosh-by-gosh/client/node_modules",
>    "eslint.workingDirectories": ["client"]
> ```


## Ruff for Python

All developers are expected to use [Ruff](https://github.com/astral-sh/ruff) to lint and format
their Python code. Ruff is a fast, all-in-one Python linter and formatter that ensures consistency
and code quality across the project.

First install Ruff:

    pip install ruff

Developers can choose when in their workflow to apply Ruff formatting and linting. Common choices are as a pre-commit hook or format-on-save.

> You may set up Ruff as a pre-commit hook, to automatically run on all staged Python files before each commit:
> 1. `.pre-commit-config.yaml` is already defined at the top of the repo.
> 2. Install the pre-commit package: `pip install pre-commit`
> 3. Install the git hook: `pre-commit install`
>
>
> Or you may configure your text editor to lint and format on save:
> - For VS Code,
>   1. Install the "Ruff" extension
>   2. Add the following to your `settings.json`:
>      ```
>      "[python]": {
>          "editor.formatOnSave": true,
>          "editor.codeActionsOnSave": {
>              "source.fixAll": "explicit",
>              "source.organizeImports": "explicit"
>          },
>          "editor.defaultFormatter": "charliermarsh.ruff"
>      }
>      ```