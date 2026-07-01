Thank you for contributing to the GuardianConnector Scripts Hub project!
Here are some conventions we use to smooth things along.

# Use of LLMs

Our team maintains a separate document, [`CONTRIBUTING_LLM_GUIDELINES.md`](CONTRIBUTING_LLM_GUIDELINES.md) that covers rules and best practices for human contributors using AI and LLMs in code, data work, and team communication. Its guidelines carry the same weight as the core guidelines below, while remaining a distinct file due to its length and specialized content.

> [!IMPORTANT]
> All contributors should read the [LLM Contributing Guidelines](CONTRIBUTING_LLM_GUIDELINES.md).


# Using GitHub

We use GitHub to host code, track issues, and accept pull requests.

## GitHub Issues & Labels

Create new Github Issues using the templates wherever possible.

## Git Branching

**We release from the `main` branch, so code merged here should always be stable.**

Prefer short lived features branches.
- Piecemeal progress towards broad code changes should merge to long-running branches until
  everything there is stable and deployable, at which point the long-running branch gets merged
  to `main`.
- Since short, coherent patches are easier to review, we code-review the individual PRs into
  the long-running feature branch (instead of review when merging the long-running branch to
  `main`)

> [!TIP]
> For concrete, example-driven guidance on managing long-running branches and stacking dependent PRs,
> see [CONTRIBUTING_EXAMPLES.md](./CONTRIBUTING_EXAMPLES.md).


## PR Review & Committing code

Code review is encouraged as a powerful tool for learning.  Benefits include
- Spread knowledge of the code base throughout the team: reviewing code is a remarkably effective way to learn the codebase.
- Expose everyone to different approaches.
- Ensure code is readable (and therefore maintainable).
- Yield better software.

The ideal PR is **cohesive**, and most cohesive PRs are quite small (< 200 new lines).
* Prefer eliminating scope over splitting a cohesive changeset arbitrarily. Extraneous or unrelated changes may be moved to their own (also small) PR.
* Sometimes a large refactor is best reviewed as one cohesive PR (i.e. where all changes depend on each other, or there is no incremental value of a partial deploy). In this case, a commit-by-commit review pattern can help: see [CONTRIBUTING_EXAMPLES.md](./CONTRIBUTING_EXAMPLES.md).

As code author:
- Keep scope in-check. Limit PRs to the goal at hand: no extra code beyond what is absolutely necessary to solve the problem.
- When you anticipate a PR will necessarily be large, loop in your reviewer early: they may have ideas that reduce scope, or they may want to agree in advance on a review approach.
- The first reviewer of the code or documentation that you submit should be YOU!  (More at https://blog.beanbaginc.com/2014/12/01/practicing-effective-self-review/)
- Ultimately the responsibility for bug-free code is on the code author, not the reviewer.
- Code review is not limited to approval/rejection of PRs. Consider involving a collaborator
earlier in the process, before the code is finished. Ask them for a narrower review—e.g., a
design review or to focus on a specific part of the code change.  Use Draft PRs, or prose documents outside of Github.

As a reviewer:
- [This Code Review Checklist](http://web.archive.org/web/20180219163514/https://blog.fogcreek.com/increase-defect-detection-with-our-code-review-checklist-example/)
  gives concrete examples of what reviewers should look for.
- Be kind & helpful, but do not ignore problems for the sake of avoiding conflict.
- "I find this very hard to follow" is valid feedback, even if the code's behavior is technically correct.
- Ability for code review to find defects diminishes with longer PRs: Feel free to reject any
  review that adds more than 400 lines of new code
  unless you believe it's as cohesive as it can be. (no upper limit on deletions!)
- Feel free to use code review as an instructional forum (for example suggesting
  clearer ways of solving the problem at hand), but do not comment only because
  the author did something differently than you would have. Use "FYI" in your
  comment to distinguish comments that do not require action by the author.

> [!TIP]
> If you're unsure how to break up a large PR, see [CONTRIBUTING_EXAMPLES.md](./CONTRIBUTING_EXAMPLES.md)
> for illustrative git commands.


Merging branches and PRs to `main`:
- The branch author should be the one to merge.
- Merge PRs via 'Merge Commit' option in GitHub.
- Delete a branch when you are done with it.


# Tests

## Dependency cooldown policy (Python)

New Python package versions must be at least 7 days old before they can be introduced.
If dependency resolution fails due to package age, wait and retry, or use an older version.

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

When possible, prefer **pure functions**, because they are easier to reason
about and easier to test. (A pure function's return is only determined by its
input values, not influenced by hidden or internal
state:
[[pure vs non-pure example](https://stackoverflow.com/a/22733240/850883)]). Our
team is fine with the fact that writing a pure function may require a few more
parameters than a stateful method, and/or might not use object-oriented
programming at all.


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

Document Python code according to **[NumPy's documentation
standard](https://numpydoc.readthedocs.io/en/latest/format.html#docstring-standard)**.