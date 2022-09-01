[![Build Status](https://travis-ci.com/Swirrl/drafter.svg?token=RcApqLo51DL6VpVysv8Q&branch=master)](https://travis-ci.com/Swirrl/drafter)

# Drafter 2

A RESTful Clojure web service to support PMD's admin tool in moving data updates between draft and live triple stores.

## Developing Drafter

Developing on Drafter itself?  Then see the [Developing
Drafter](/drafter/doc/developing-drafter.md) for how to use Drafter
and set up your Dev environment.

## Configuring Drafter

You will need to configure drafter before using it.

Drafter uses [aero](https://github.com/juxt/aero) for its configuration. It is configurable in a variety of ways, through EDN configuration profiles supplied via the command line, environment variables (and/or java properties).

See the [Configuring Drafter](https://github.com/Swirrl/drafter/blob/master/drafter/doc/configuring-drafter.org) document for more information.

## Using Drafter as a Service (API Docs)

Drafter 2 exposes its API documentation along with a tool for driving
the API through its web interface. By default, this is available at:

    http://localhost:3001/

If you don't have a running drafter and wish to consult the
documentation you will need to inspect the Yaml file from which the above is generated.

This can be found in [/drafter/doc/drafter.yml](/drafter/doc/drafter.yml).

## Continuous Integration

Drafter uses Circle CI. 

Every commit of every branch is built.

Additionally, commits to the master branch creates and publishes omni packages with names like `<branch-name>-circle_<build-no>_<commit>` e.g. `master-circle_643_fd4570`

Tags that look like `v<number>.<number>` e.g. `v2.6` will also create and publish an omni package that looks like: `2.6-circle_999_abcdef` (not a real versioned release).

## Making a release

To make a release, tag and push a tag that looks like `v<number>.<number>` to the repo (either via CLI or via Github UI).

Make a release through the Github UI and choose the tag. Add some notes about what's new (and link to issues).

Existing releases can be found here: [Github releases](https://github.com/Swirrl/drafter/releases).


## Migrations

Migrations live in [./migrations](./migrations), and for now are run manually
with e.g.

```
$ stardog query <database-name> <path-to-migration>
```

If a version bump requires a migration to be run, it should be noted in the
release notes.

## Docker

CI will build and push a docker image to

```
europe-west2-docker.pkg.dev/swirrl-devops-infrastructure-1/swirrl/drafter-pmd4
```

tagged with git tag, branch name, and commit sha.

Consumers may want to mount volumes at `/app/config` to provide custom
configuration, and at `/app/stasher-cache` to persist the cache.

## RBAC

When deploying drafter with auth0 auth, users are expected to have been
configured with some subset of the following permissions, (depending on what
they should be allowed to do), and for those permissions to be passed in the
`permissions` claim of the auth token.

```
drafter:draft:claim
drafter:draft:create
drafter:draft:delete
drafter:draft:edit
drafter:draft:publish
drafter:draft:share
drafter:draft:submit
drafter:draft:view
drafter:job:view
drafter:public:view
drafter:user:view
```

How exactly this is done isn't important, and these permissions can be split
between roles in a way that makes sense for the specific deployment, but for
example you might:

1. create a new API called PMD, with audience `https://pmd`
2. in RBAC Settings, "Enable RBAC" and "Add Permissions in the Access Token"
3. add all of the above permissions under "Permissions"
4. authorize the drafter and muttnik "Machine to Machine Applications"
5. under "User Management" > "Roles" create roles (see below)
6. assign roles to the relevant users

### Example role mapping:

- PMD-RBAC:User has drafter:public:view
- PMD-RBAC:Reviewer has drafter:draft:view drafter:job:view drafter:public:view
  drafter:user:view
- PMD-RBAC:Editor has drafter:draft:claim drafter:draft:create
  drafter:draft:delete drafter:draft:edit drafter:draft:share
  drafter:draft:submit drafter:draft:view drafter:job:view drafter:public:view
  drafter:user:view
- PMD-RBAC:Publisher has drafter:draft:claim drafter:draft:create
  drafter:draft:delete drafter:draft:edit drafter:draft:publish
  drafter:draft:share drafter:draft:submit drafter:draft:view drafter:job:view
  drafter:public:view drafter:user:view

## Maintenance read-only mode

In production, you can execute the `drafter-toggle-writing.sh` script which communicates
with Drafter server over a TCP socket to toggle Drafter into a read-only whereby writes
(write jobs, or direct calls to `append` etc.) are rejected.

The socket (and hence script) will wait until all jobs are flushed before returning and
updating the user when all jobs are flushed, and it is safe to do maintenance. This
same information is also written to the drafter log.
