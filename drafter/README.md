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

## Migrations

Migrations live in [./migrations](./migrations), and for now are run manually
with e.g.

```
$ stardog query <database-name> <path-to-migration>
```

If a version bump requires a migration to be run, it should be noted in the
release notes.
