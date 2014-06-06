# Drafter

A RESTful Clojure web service to support PMD's admin tool in moving
data updates between draft and live triple stores.

## Idea

* Only have one (JENA) database for live and draft data.
* draft data is stored in different graphs to live data (one draft graph per user per changed graph).
* the live endpoint/site queries against a context/default graph which includes only the live data's graphs.
* users (with appropriate permissions) can choose a preview of combination of live and draft graphs.
  - it would be great if we could also change how sparql queries get interpreted, so that we can map live graph names to the appropriate replacement draft graph name in this preview mode.
* Applying a change to live would result in deleting the live graph's contents, and replacing them with the draft's contents.

* We probably actually to do these actions with several drafts at once for a dataset/vocab (i.e. metadata, data graph and attachments graph).

### Actions that drafter needs to do

* Copy or create graph(s)
* import triples from files into graph uri(s)
* delete graph(s)
* Make graph(s) live: delete+ (re)create in live.
