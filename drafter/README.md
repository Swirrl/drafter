[![Build Status](https://travis-ci.com/Swirrl/drafter.svg?token=RcApqLo51DL6VpVysv8Q&branch=master)](https://travis-ci.com/Swirrl/drafter)

# Drafter 2

A RESTful Clojure web service to support PMD's admin tool in moving data updates between draft and live triple stores.

- Using Drafter as part of PMD or otherwise?  Then see the [User Guide](https://github.com/Swirrl/drafter/blob/master/drafter/doc/using-drafter.md)
- Developing on Drafter itself?  Then see the [Getting Started Guide](https://github.com/Swirrl/drafter/blob/master/drafter/doc/getting-started.org) for how to use Drafter and set up your Dev environment.

## Developing Drafter

### Environment

You will need to set some env vars before running drafter in dev.  An example set that should work are defined in .envrc.example you can either source these directly or use a tool like `direnv` to automatically source them after you copy them to a project level `.envrc` file.

### REPL

For dev you should start a repl with the following aliases.  You may need to add more for cider and other tools etc...

```
$ clj -A:dev:test
```

### Testing

The test suite uses kaocha and travis is set to run the tests twice in two modes corresponding to the two auth modes (auth0 and basic-auth).

For tests with auth0 backend:

```
./bin/kaocha --focus auth0
```

For tests with basic-auth backend:

```
./bin/kaocha --focus basic-auth
```

For tests that aren't driving the REST-API (and therefore don't require authentication).  These are typically more unit level.

```
./bin/kaocha --focus non-api
```


## Configuring Drafter

Drafter uses [aero](https://github.com/juxt/aero) for its configuration. This means it uses environment variables (and/or java properties) to pass configuration
variables from the environment.

The defaults should work for most cases, but for further details on the options drafter supports you should see the [Configuring Drafter](https://github.com/Swirrl/drafter/blob/master/drafter/doc/configuring-drafter.org) page.

 You may also wish to configure drafter logging by putting a
`log-config.edn` file in drafter's working directory. We provide an
example file you can modify at [log-config.edn.example](https://github.com/Swirrl/drafter/blob/master/drafter/log-config.edn.example).

## API Docs

Drafter 2 exposes its API documentation along with a tool for driving the API through its web interface. By default, this is available at:

    http://localhost:3001/

The source for the swagger docs can be found in [/doc/drafter.yml](https://github.com/Swirrl/drafter/blob/master/doc/drafter.yml).

Additionally the raw endpoints and a trig dump endpoint are exposed on a UI along with a live query endpoint at:

    http://localhost:3001/live
