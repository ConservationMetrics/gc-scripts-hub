These examples illustrate practical workflows that support [our contributing guidelines](CONTRIBUTING.md).

They are not new rules, but you may find them helpful in staying consistent with the guidelines.


## Example: Stacked or Dependent PRs Workflow

When multiple PRs build upon each other -- for example `main ← PR1 ← PR2 ← PR3`:

1. **Start a long-running branch other than `main`.** Many repos deploy straight from `main`, so this branch prevents features landing in `main` until they are complete.
    - Example: `git checkout -b 191 main` becomes the integration point for a feature family.
    - If there's a clear issue number that all the dependent PRs target, name it after that issue number.
2. Open a PR targeting that branch. If targeting a long-running feature branch, use that as the branch name prefix.
    - Example: `git checkout -b 191a-api 191`
    - PR1: "Author wants to merge _m_ commits into [**`191`**] from `191a-api`"
3. **When another PR is heavily dependent on the first PR, have it target the first PR's branch.** This way, the downstream PR's diff gets limited to what's new—what's not already in another PR. That focuses the code reviewer's attention and discussion on the PR to the correct scope.
    - Example: `git checkout -b 191b-types 191a-api`
    - PR2: "Author wants to merge _n_ commits into [**`191a-api`**] from `191b-types`"
4. **After a PR merges, retarget the next one.**  Let's say that `191a-api`` goes thru code review and gets approved. Then you can:
    - Merge `191a-api` into the long-running branch `191`.
    - Update `191b-types`'s merge target to now be the long-running branch:
        - PR2: "Author wants to merge _n_ commits into [**`191`**] from `191b-types`"
        - Actually I think Github sometimes does this for you.
    - From the reviewers point of view, the patchset between this PR and its target never changes (it's always the same diff to review), nor does the PR scope or description need to change.
5. **Use merge commits.**  As code author, when merging a branch, choosing the "Merge Commit" method will simplify future meregs to of other branches.
    - Bring other open PRs up-to-date by running, on their branches, `git merge 191`.
    - When doing a Dependent PR Workflow like this I almost never `git rebase` or `squash` commits that exist in a different branch. Those operations will just introduce conflicts that are tedious to clean up. `git merge` is the answer for both the upstream branch and the downstream branch.
6. **Iterate and stabilize the long-running branch.** Keep merging small PRs until `191` is stable and deployable.
    - Since a human's already reviewed all code that made it to `191`, we do not require another human code review to merge `191` into `main`. Code author (aided by any workflow bots [e.g. tests pass, etc.]) can decide when to do this. The threshold for merge to `main` is "Is this ready to deploy to production?"
    - They may run `git merge 191` directly, or open a PR and self-review.

This workflow works well when you have heavy dependencies between branches. However **prefer to not introduce dependencies when you can.**
If your next PR _can_ build directly on `191` or `main`, do that. It reduces the chance of an upstream PR blocking this PR's merge.


## Example: Split Large Commits or PRs Post-Hoc

Sometimes work is already committed or pushed as a large PR. Learn & use these tools to split your very large branch into smaller, more reviewable PRs.

### Interactive staging with `git add -p`

Say you've made many unrelated changes -- all in the latest commit or all unstaged. `git add -p` helps you interatively select which lines of code to add to a more specific commit:

```bash
git reset HEAD~  # unstage recent commit(s), if needed
git add -p       # interactively stage hunks that are all related
git commit -m "Bugfix: do not read negative bytes"
git add -p       # interactively stage hunks that are all related
git commit -m "Feature: Render pending blob state on dashboard"
```

### Interactively rewrite commits with `git rebase -i`

Use this when you realize that what looked like one feature really contains several separable units of work. An interactive rebase lets you split, combine, rename, or reorder commits to produce smaller, clearer PRs.

While our contributing guidelines don't demand clean commit history, reviewers benefit when you can break unrelated commits to separate, conceptually focused PRs.

```bash
git rebase -i origin/main
# In your editor:
# - mark a commit as 'edit' to split or revise it
# - mark as 'squash' or 'fixup' to combine several commits into one
# - reorder lines to change commit order
# - mark a commit as 'drop' to discard temporary or exploratory work
```

When you mark a commit as edit, Git will stop so you can split it. To split it, follow the `git add -p` example above, and after replacing the old commit making multiple new commits, continue with `git rebase --continue`.

After you've rewritten your branch into smaller or more cohesive commits, you can cherry-pick subsets of commits onto new branches, and push each branch as its own PR:

```bash
git log --oneline
# 9a8c123 Add compound metrics export
# 4b2e789 Refactor metrics collection

git checkout -b refactor-metrics
git cherry-pick 4b2e789
git push origin refactor-metrics

git checkout -b export-compound-metrics
git cherry-pick 9a8c123
git push origin export-compound-metrics
```
