# Drafter

A RESTful Clojure web service to support PMD's admin tool in moving
data updates between draft and live triple stores.


Getting started
==================

* clone this project
* [install leiningen](http://leiningen.org/#install)
* cd into the project directory `cd drafter`
* `lein repl` This will start an http server on port 3001


Building and running
--------------------

To run a dev build of the drafter server with configuration defaults
(port 3001 and a repo stored in ./drafter/repositories/db) simply run

    $ lein repl

To run the tests from the console:

    $ lein test


To run a test from a repl, make sure the test namespace is loaded (evaling it will usually do this) then run (for example):

    (clojure.test/run-tests 'drafter.routes.sparql-test)

To build:

    $ lein uberjar

To run a built drafter server (without leiningen or a repl) on port 3001:

    $ java -jar target/drafter-0.1.0-SNAPSHOT-standalone.jar

Configuring Drafter
-------------------

Drafter uses [environ](https://github.com/weavejester/environ) for its
configuration.  This means it uses environment variables (and/or java
properties) to pass configuration variables from the environment.

Configurable properties and their defaults are:

    DRAFTER_HTTP_PORT:            (default 3001)
    DRAFTER_REPO_PATH:            (default "drafter-db")
    DRAFTER_INDEXES:              spoc,posc,cosp (Best for query performance, slowest for insert)

Some examples of supplying these properties are provided below:

As environment variables via lein run:

    $ env DRAFTER_HTTP_PORT=3040 DRAFTER_REPO_PATH=/var/drafter-db lein run

As java properties via a built application jar:

    $ java -Ddrafter.http.port=3050 -Ddrafter.repo.path=./drafter-database -jar target/drafter-0.1.0-SNAPSHOT-standalone.jar

As environment variables via a built application jar:

    $ env DRAFTER_HTTP_PORT=3050 DRAFTER_REPO_PATH=./drafter-database java -jar target/drafter-0.1.0-SNAPSHOT-standalone.jar

Indexes can be configured via the property `DRAFTER_INDEXES`, we use
the same default indexes as sesame. `spoc,posc,cosp` which covers all
possible

Configuring Logging
-------------------

Logging on the JVM can be awkward due to Java's legacy, however we've
tried to keep it as simple as possible by supporting:

- Logging config via our log-config.edn file, which supports pure
  Clojure configuration (No XML or Log4J Property Files).
- Logging both Java and Clojure code (meaning it's easy to output
  sesame's log messages with the same config)
- A logging configuration fallback chain (log-config.edn -> default
  config in drafter.jar!log-config.edn)

This is achieved by through the use of various logging libraries:

- [clojure.tools.logging](https://github.com/clojure/tools.logging) with
  the [Log4J](http://logging.apache.org/log4j/2.x/) logging

- [clj-logging-config](https://github.com/malcolmsparks/clj-logging-config)
  for the configuration DSL to provide simple Log4J configuration to
  configure logging in different namespaces, Java packages and setup
  your own log appenders.

To configure logging in Drafter you should create a file called
`log-config.edn` by copying the example `log-config.edn.example`.

An example of a drafter `log-config.edn` file is here:

````
(let [log-pattern "%d{ABSOLUTE} %-5p %.20c{1} %-10.10X{reqId} :: %m%n"]
       (set-loggers!
        ["org.openrdf" "drafter"]
        {:name "drafter"
         :level :trace
         :out (DailyRollingFileAppender.
               (EnhancedPatternLayout. log-pattern)
               "logs/drafter.log" "'.'YYYY-MM")}

        "drafter.rdf.sparql-protocol"
        {:name "sparql"
         :level :info
         :out (DailyRollingFileAppender.
               (EnhancedPatternLayout. log-pattern)
               "logs/sparql.log" "'.'YYYY-MM")}))
````

The first thing to note is that this
[edn](https://github.com/edn-format/edn) file must contain a Clojure
form to be evaluated.

When drafter starts it first looks at the environment for a variable
called `LOG_CONFIG_FILE` if this is defined then the file at the
location is evaluated, and if no file is found then Drafter will
fallback to a file in its working directory named `log-config.edn`.
Finally if this file is not found then drafter will configure its
logging by using a default `log-config.edn` file contained in its Jar
file.

To configure the logging these forms should then contain a call to
[clj-logging-config's](https://github.com/malcolmsparks/clj-logging-config)
`set-loggers!` macro.


Docker
======

Successful drafter builds get built into docker images on dockerhub (hub.docker.com) at `swirrl/drafter`.


Running the container
---------------------

Suggested commands:

1. Pull the image

`docker pull swirrl/drafter:build_<build-number>`

2. Make a data container for the database

`docker run -d -v /data/drafter-database --name drafter-data ubuntu true`

3. Make a data container for the logs

`docker run -d -v /drafter/logs --name drafter-logs ubuntu true`

4. Run drafter itself

Depending on the server topology, you might want to do one of the following:

* link the drafter container to a client container
* expose a port to the host server (which you could proxy-to via nginx)
* use an ambassador container to link over the network

e.g. (exposing a port to the host server)
`docker run -d -p 127.0.0.1:<port-to-expose-on-host>:3001 --volumes-from drafter-data --volumes-from drafter-logs --name drafter swirrl/drafter:build_xxx`

(check it's running with `docker ps`)

To backup the data
------------------
`docker run --volumes-from drafter-data -v <folder-on-the-host-to-backup-to>:/data-backup ubuntu tar cvf /data-backup/backup.tar /data/drafter-database`

Explanation: this

* creates a new container based on `ubuntu` image,
* mounts the data volume onto it
* mounts a folder of your choice from the host system into `/data-backup` inside the container
* inside the container, it tars contents of `/var/lib/drafter-database` (the mounted volume from `drafter-data`) into `/data-backup` volume (which will appear on the mounted host dir).

To backup the logs
------------------

Similarly to the data:

`docker run --volumes-from drafter-logs -v <folder-on-the-host-to-backup-to>:/logs-backup ubuntu tar cvf /logs-backup/backup.tar /drafter/logs`

To peek inside a running drafter docker container
-------------------------------------------------

This will get you a bash shell inside the running drafter docker container

`docker exec -i -t drafter bash`


Drafter's SPARQL Endpoints
=========================

Drafter exposes a number of specialised SPARQL endpoints for querying they are:

`GET | POST /sparql/state?query=select * FROM...`

SPARQL endpoint on state graph only

`GET | POST /sparql/live?query=select * FROM...`

SPARQL endpoint on live union graph only (drafter to check state
graph on each request to know what this is).

`GET | POST /sparql/draft?graph=GURI1&graph=GURI2...&query=select * FROM...`

SPARQL endpoint on an arbitrary set of drafts (plus the rest from live).

If on the draft graph you want the drafts to be unioned with the
current state of the public live graphs then you should also pass the
HTTP parameter `&union-with-live=true`.  The default value for this is
false.


Drafters REST API:
==================

Graph Management Operations
---------------------------

**Create a new draft**

`POST /draft/create?live-graph=GURI`

Supply the ultimate intended live graph uri.

Synchronously creates a new draft and returns the `GURI` (Graph URI) of the draft graph.

**Add content from a file to a draft**

`PUT | POST /draft?graph=GURI`

Enqueues an append/replace of the contents of the draft graph with the file data.

Use `PUT` for replace, and `POST` for append semantics.

Must contain the file of triples with a correct mime-type.

The file must be supplied as multi-part-form data under the key `file`.

Returns a 202 if enqueued successfully, with the `queue-id` in the response body

**Add content from another graph to a draft**

`PUT | POST /draft?graph=graph-uri&source-graph=GURI`

Same as above, but instead of reading the file from the request body, it takes
the data from the supplied `source-graph`.

**Delete a draft or live graph**

`DELETE /graph?graph=GURI`

Enqueues a delete of the draft or live graph. If draft, removes the draft from the state graph.

Returns a 202 if enqueued successfully, with the `queue-id` in the response body

**Making a draft live**

`PUT /graph/live?graph=GURI`

Enqueues transactional migration the specified graph(s) from draft to live.

If you wish to make multiple graphs live at once, simply supply
multiple graph arguments, these should all be scheduled together to
occur in a single transaction e.g.

`PUT /graph/live?graph=GURI&graph=GURI&graph=GURI`

This replaces the content of the live graph with the draft one, removing
the draft afterwards.  It also sets isPublic to true.

Returns a 202 if enqueued successfully, with the `queue-id` in the response body

**Making a 'live graph' public/private (TODO - not available yet)**

`PUT /graph/live?graph=GURI&public=true`
`PUT /graph/live?graph=GURI&public=false`

**Metadata**

For all of the above routes, you can supply additional k-v pairs (slug->str). The slug should start `meta-`. e.g.

`PUT | POST /draft?graph=graph-uri&source-graph=GURI&meta-foo=bar`

For `POST/draft/create`, it will add metadata to the state graph.

For the rest, it will add metadata to the queue :meta (as URI->str) (available via `/queue/peek`)

Dumps End Points
================

For each of its endpoints drafter supports a dumps endpoint, for
retrieving data in the requested graph serialisation.  The endpoints
are at the following locations:

`GET /data/draft`
`GET /data/live`
`GET /data/raw`

Each of these endpoints supports a `graph-uri` parameter to specify
which graph you wish to retrieve the data for.

Unlike the others the draft endpoint should be given the live graph
URI along with `graph` parameters that specify the draft set.  NOTE
you can supply just the draft graph for the desired live graph.

Any supported graph serialisation can be selected by setting the
accept header to the desired mime/type e.g. `application/n-triples`.

Each of these endpoints should additionally support all of the
behaviours and options of the endpoints that they wrap.  For example
the drafts dumps end point will inherit support of the drafts endpoint
`union-with-live` parameter.

Data Model
==========

This is an alternative model to that written up by Ric.  It is
also different from what I was originally pitching.  The
key difference is that it models both live and draft graphs.
Whilst you can get away with less, Ric was right doing so feels
unnatural and asymetrical.  Hopefully this approach is intuitive
and symetrical!

In the db, we'll have a (private) 'state' graph which stores
details of the state of each graph.  The state graph and all
other graphs will be stored within the same triple store.
Some points to note about this approach:

- A hasDraft predicate associates a live graph with many drafts.
- The union of all live graphs can be obtained with the query:

            SELECT ?live WHERE {
               ?live a drafter:ManagedGraph ;
                     drafter:isPublic true .
            }

- When drafts are migrated into the "live" graph their entries
  and associations are removed from the state graph.

- The is:Public boolean lets you toggle whether the "live" graph
  is actually online and publicly accessible, and do so
  independently of creating a new graph.

      <http://example.org/graph/live/1>
         a            drafter:ManagedGraph ;
         <created-at> "DateTime" ;
         <isPublic>  false ;
         <hasDraft> <http://drafter.swirrl.com/draft/graph/GUID-123> ;
                        a drafter:DraftGraph ;
                        <owner> "bob" ;
                        <updated-at> "DateTime" .
         <hasDraft> <http://drafter.swirrl.com/draft/graph/GUID-124> ;
                        a drafter:DraftGraph ;
                        <owner> "joe" ;
                        <updated-at> "DateTime" .

      # this is a graph with no draft changes
      <http://example.org/graph/live/1>
         a            drafter:ManagedGraph ;
         <isPublic>  true ;
         <created-at> "DateTime" .

Other notes
===========

Drafter doesn't know the difference between metadata graphs
and data graphs. It just moves data around and between states.
That's up to PMD to orchestrate.
