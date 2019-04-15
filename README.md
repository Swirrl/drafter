# drafter-client

A clojure client for the Drafter HTTP API.

## Developing in this project

In order to run the tests you need to

1. Create an empty `drafter-client-test` database created in stardog.
   Switch reasoning off to be safe.
2. Launch drafter `env DRAFTER_JWS_SIGNING_KEY=foo SPARQL_QUERY_ENDPOINT=http://localhost:5820/drafter-client-test/query SPARQL_UPDATE_ENDPOINT=http://localhost:5820/drafter-client-test/update lein run`
3. Run `drafter-client` tests in the same environment `env DRAFTER_JWS_SIGNING_KEY=foo SPARQL_QUERY_ENDPOINT=http://localhost:5820/drafter-client-test/query SPARQL_UPDATE_ENDPOINT=http://localhost:5820/drafter-client-test/update lein test`

## Usage

The main functionality for the drafter client is defined in the drafter-client.client namespace:

    (require '[drafter-client.client :as client])

The first step is to create a new client by providing the location of a drafter instance:

    (def c (client/create "http://localhost:3002"))

### Graph operations

Once the client has been created you can use it to create a new draft with the given live graph URI:

    (def graph-uri (client/create-draft-graph c "http://example.org/graphs/live"))

This returns the URI of the created draft.

There are two ways to populate a draft graph with statements using the API. The simplest method is to create a DrafterAPIWriter since this
implements the Grafter ITripleWritable protocol.

    (require '[grafter.rdf.protocols :as grafter])
	(require '[grafter.rdf :as rdf])

    (def writer (client/->DrafterAPIWriter c graph-uri))
    ;add a single statement
	(grafter/add-statement writer (grafter/->Triple "http://subject.org" "http://predicate.org" "http://object.org"))

	;add a sequence of statements
	(grafter/add writer [(grafter/->Triple "http://subject.org/1" "http://predicate.org/1" "http://object.org/1")
	                     (grafter/->Triple "http://subject.org/2" "http://predicate.org/2" "http://object.org/2")]

	;add the contents of an RDF stream
	;NOTE: triple-input-stream is a java.io.InputStream containing serialised RDF data
	(grafter/add writer "http://example.org/graphs/1" rdf/format-rdf-ntriples triple-input-stream)

The DrafterAPI writer always appends to the destination graph, so for more control there is a function which matches the Drafter API more closely:

    ;NOTE: Last argument is optional - default is :append
	(require '[grafter.rdf :as rdf])
    (client/populate-from-stream c graph-uri input-stream rdf/format-rdf-ntriples :append)
	(client/populate-from-stream c graph-uri input-stream rdf/format-rdf-ntriples :replace)

This function takes the URI of the draft to populate, a java.io.InputStream of formatted RDF data, the format of the triple input stream
and an optional argument describing how to do the update. If the update method is :append or not provided, then the statements will be
appended to the graph, otherwise if :replace is specified then the existing data in the graph will be replaced.

If even more control is required, a function exists which is a thin wrapper around the HTTP interface to the Drafter API:

    (client/populate-from-stream-request c graph-uri input-stream "triples.nt" "application/n-triples" "utf-8" :replace)

As with populate-from-stream, this function take the client, URI of the draft graph to populate and the input stream of RDF data. It also
requires the name of the file in the multipart request, the MIME type of the uploaded file and its content encoding. It also has an optional
parameter controlling how the data is to be added to the graph.

Once the data has been writen to the graph it can be made live:

    (client/make-draft-graph-live c graph-uri)

### SPARQL endpoints

The client can also be used to access the SPARQL endpoints for the state, live and draft graphs.

    (def live-endpoint (client/sparql-live-uri c))
	(def state-endpoint (client/sparql-draft-uri c))
	(def draft-endpoint (client/sparql-draft-uri c [graph-uri other-draft-graph-uri]))
