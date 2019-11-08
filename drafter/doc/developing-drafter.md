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
