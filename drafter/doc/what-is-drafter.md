# What is Drafter?

<div style="background-color: #f8cd82; padding: 0.5em">
   <i>Note</i>: This document is a draft may be incomplete.
</div>

Drafter is a HTTP service which supports RDF data management
workflows, by letting users create, edit, preview and publish sets of
changes (draftsets).

It operates as a proxy server, sitting between an RDF database and an
application or user. Draftsets then provide their own SPARQL endpoint
which lets users query, review and preview their changes prior to
publication.

Drafter works by implementing COW (Copy On Write) at the level of RDF
graphs. Only graphs which have changes are copied, yet other published
graphs can still be previewed within the Draftset as if the Draftset
also contained all the published too.

Drafter works by dynamically rewriting SPARQL queries, responses and
RDF data at ingestion. Applications using the SPARQL endpoints need
not be aware of its presence.

Drafter also provides a REST API for managing the workflow associated
with Draftsets; bundling the changes together, providing access
control over them, allowing users to share them, preview them, query
them and publish them as appropriate.

The diagram below shows the high level interfaces and typical
arrangement of a system deploying drafter. Application and Admin UI
can in principle be any application, but is usually an instance of
PublishMyData either 3 or 4.

```
┌──────────────────┐
│                  │                                  ┌──────────────────┐          ┌──────────────────┐
│                  │                                  │                  │          │                  │
│  Application UI  │               ┌──────────────────┴──┐               │          │                  │
│                  │────SPARQL────▶│    /sparql/live     │───────────────┼─SPARQL───▶                  │
│                  │               └──────────────────┬──┘               │          │                  │
│                  │                                  │                  │          │                  │
├──────────────────┤                                  │                  │          │                  │
│                  │               ┌──────────────────┴──┐               │          │                  │
│                  ├────SPARQL────▶│ /draftset/:id/query │───────────────┼─SPARQL───▶     Stardog      │
│                  │               └──────────────────┬──┘  Drafter      │          │  (Triplestore)   │
│                  │                                  │                  │          │                  │
│                  │               ┌──────────────────┴──┐               │          │                  │
│     Admin UI     ├─────REST ────▶│ Data Management API │───────────────┼─Writes───▶                  │
│                  │   Requests    └──────────────────┬──┘               │          │                  │
│                  │                                  │                  │          │                  │
│                  │                                  │                  │          │                  │
│                  │                                  │                  │          │                  │
│                  │                                  │                  │          │                  │
└──────────────────┘                                  └──────────────────┘          └──────────────────┘
```

### The Drafter Illusion

When the option `?union-with-live=true` is set Drafter tries to
maintain an illusion. That every draftset is a complete copy of the
current "live database/endpoint", with the draft changes layered over
this. One way it does this is by controlling the visibility of the
graphs you can see.

The logical diagram below describes what is happening. Here we see the
"live endpoint" contains two graphs of data, and we have a draftset
called "trade-update" which currently has no changes in it.

At this point querying the empty draftset (with the flag
`?union-with-live=true`) behaves exactly as if you were querying the
live endpoint directly. Drafter knows to adjust the visibility of
graphs such that the draftset looks exactly like live from the
perspective of an observer issuing SPARQL queries to the draftset.

Creating new draftsets is a very cheap constant-time operation, as no
data needs be copied at this point. The graphs logically appear to be
in the draftset (indicated by a dotted line), but drafter avoids
copying the whole live endpoint into the draft, and instead just
references the set of the graphs in the live endpoint to make it look
like the draft contains the currently published data.

If drafter didn't do this, it would need to copy all the data into
live when the draft was created. Instead we follow a more efficient
Copy On Write strategy.

```
                                                                 ?union-with-live=true
┌─────────────────────────────────────────┐           ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
│                                         │           ┃                                         ┃
│   ┌──────────────────────────┐          │           ┃     ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─         ┃
│   │   data/trade-in-goods    │──────────┼───────────╋────▶    data/trade-in-goods    │        ┃
│   └──────────────────────────┘          │           ┃     └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─         ┃
│                                         │           ┃                                         ┃
│                                         │           ┃                                         ┃
│                                         │           ┃                                         ┃
│   ┌──────────────────────────┐          │           ┃     ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─         ┃
│   │codelist/trade-industries ├──────────┼───────────╋────▶ codelist/trade-industries │        ┃
│   └──────────────────────────┘          │           ┃     └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─         ┃
│              Live Endpoint              │           ┃         Draftset: trade-update          ┃
└─────────────────────────────────────────┘           ┗━━━━━━━━━━━━━━▲━━━━━━━━━━━┳━━━━━━━━━━━━━━┛
                                                                     │           │
                                                                     │           │
                                                                   SPARQL      SPARQL
                                                                  Queries    Responses
                                                                     │           │
                                                                     │           │
                                                                     │           │
                                                                     │           ▼
```

NEXT describe CoW

----
