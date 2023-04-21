# Developing Drafter

## Environment

You will need to set some env vars before running drafter in dev.  An example set that should work are defined in `.envrc.example` you can either source these directly or use a tool like `direnv` to automatically source them after you copy them to a project level `.envrc` file.

## REPL

For development you should start a repl with the following aliases. You may need to add more for cider and other tools.

```sh
$ clj -A:dev:test
```

## Testing

The test suite uses kaocha.CircleCI is set to run the tests twice in two modes corresponding to the two auth modes (auth0 and basic-auth).

_Note_: For a successful, local test run you need all authentication backend dependencies (e.g. MongoDB). Otherwise you may narrow down the set of tests to run as shown below.

For tests with auth0 backend:

```sh
./bin/kaocha --focus auth0
```

For tests with basic-auth backend:

```sh
./bin/kaocha --focus basic-auth
```

For tests that aren't driving the REST-API (and therefore don't require authentication).  These are typically more unit level.

```sh
./bin/kaocha --focus non-api
```

## Making a release

To make a release, tag and push a tag that looks like `v<number>.<number>` to the repo (either via CLI or via Github UI).

Make a release through the Github UI and choose the tag. Add some notes about what's new (and link to issues).

Existing releases can be found here: [Github releases](https://github.com/Swirrl/drafter/releases).


## Migrations

Migrations live in [./migrations](/drafter/migrations), and for now are run manually with e.g.

```sh
$ stardog query <database-name> <path-to-migration>
```

If a version bump requires a migration to be run, it should be noted in the release notes.

## Docker

[CI](ci.md) will build and push a docker image to

```text
europe-west2-docker.pkg.dev/swirrl-devops-infrastructure-1/swirrl/drafter-pmd4
```

tagged with git tag, branch name, and commit sha.

Consumers may want to mount volumes at `/app/config` to provide custom configuration, and at `/app/stasher-cache` to persist the cache.