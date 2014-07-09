# Drafter

A RESTful Clojure web service to support PMD's admin tool in moving
data updates between draft and live triple stores.


Getting started
==================

* clone this project
* [install leiningen](http://leiningen.org/#install)
* cd into the project directory `cd drafter`
* `lein repl` This will start an http server on port 3000

Connecting to the repl with LightTable
-----------------------------------

Add this to your `.lein/profiles.clj`:


    { :user {
         :plugins [[lein-light-nrepl "0.0.18"]] ;;Make sure to check what the latest version of lein-light-nrepl is
         :dependencies [[lein-light-nrepl "0.0.18"]]
         :repl-options {:nrepl-middleware [lighttable.nrepl.handler/lighttable-ops]}
         }
    }

In LightTable, you can then add a connection to a Clojure (remote nREPL) (view->connections), on localhost:5678.



Drafters REST API:
==================

Querying
--------

Drafter exposes a number of specialised SPARQL endpoints for querying they are:

`GET | POST /sparql/state?query=select * FROM...`

SPARQL endpoint on state graph only

`GET | POST /sparql/live?query=select * FROM...`

SPARQL endpoint on live union graph only (drafter to check state
graph on each request to know what this is).

`GET | POST /sparql/draft?draft-graphs=GUID1,GUID2...&query=select * FROM...`

SPARQL endpoint on an arbitrary set of drafts (plus the rest from live).

Graph Management Operations
---------------------------

**Create a new draft**

`POST /draft/create?live-graph=GURI`

Supply the ultimate intended live graph uri.

Returns the `GURI` (Graph URI) of the draft graph.

**Add content from a file to a draft**

`PUT | POST /draft?graph=GURI`

Appends or replaces the contents of the draft graph with the file data.

Use `PUT` for replace, and `POST` for append semantics.

Must contain the file of triples with a correct mime-type.

The file must be supplied as multi-part-form data under the key
`file`.

**Add content from another graph to a draft**

`PUT | POST /draft?graph=graph-uri?source-graph=GURI`

Same as above, but instead of reading the file from the request body, it takes
the data from the supplied `source-graph`.

**Delete a draft**

`DELETE /draft?graph=GURI`

Deletes the draft.

**Making drafts live**

`PUT /live?graphs=GURI1,GURI2...&extra-metadata-uri=str-value&extra-metadata-uri=str-value`

Transactionally migrates the specified graph(s) from draft to live.

This replaces the content of the live graph with the draft one, removing
the draft afterwards.  It also sets isPublic to true.

Can supply additional k-v pairs (uri->str) to save against the GURI
in the state graph.

**Making a 'live graph' public/private**

`PUT /live?graph=GURI&public=true`
`PUT /live?graph=GURI&public=false`

Drafter Data Model
==================

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
