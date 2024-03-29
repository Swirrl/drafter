# drafter-client

A clojure client for the Drafter HTTP API.

## Developing in this project

In order to run the tests you need to

1. Create an empty `drafter-client-test` database created in stardog.
```shell
stardog-admin db create \
    -n drafter-client-test \
    -o strict.parsing=false \
    -o query.all.graphs=true \
    -o reasoning.schema.graphs='http://publishmydata.com/graphs/reasoning-tbox' \
    -o reasoning.type=SL --
```

2. Set required environment variables how you prefer. E.G.,

``` shell
export SPARQL_QUERY_ENDPOINT=http://localhost:5820/drafter-client-test/query
export SPARQL_UPDATE_ENDPOINT=http://localhost:5820/drafter-client-test/update
export DRAFTER_ENDPOINT=http://localhost:3001
export AUTH0_AUD=https://pmd
```

3. Run `drafter-client` tests from `drafter-client` directory

``` shell
.../drafter/drafter-client $ ./bin/kaocha
```

## Usage

The main functionality for the drafter client is defined in the drafter-client.client namespace:

    (require '[drafter-client.client :as client])

The first step is to create a new client by providing the location of a drafter instance:

    (def c (client/web-client "http://localhost:3002"))

Or, more likely you'll want an integrant component of `:drafter-client/client`

# API

The main API is found in namespace `drafter-client/client`. This is mostly a
convenience wrapper around the `martian` data API, generated in
`drafter-client.client/impl`.
