# Continuous Integration

Drafter uses [Circle CI](https://circleci.com). 

Every commit of every branch is built.

Additionally, commits to the master branch creates and publishes omni packages with names like `<branch-name>-circle_<build-no>_<commit>` e.g. `master-circle_643_fd4570`

Tags that look like `v<number>.<number>` e.g. `v2.6` will also create and publish an omni package that looks like: `2.6-circle_999_abcdef` (not a real versioned release).

_Tip_: you can put `[skip ci]` in your commit message to avoid triggering a CI build.