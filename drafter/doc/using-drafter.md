# Using Drafter

`TODO` Make this more of a guide on how to run drafter as a user etc.

## Convert any existing Graphs to work with Drafter

If you have an existing Database with quads in it you may need to make drafter aware of them.  To do this you can run the update statement though be careful not to run it twice on the same database!

```sparql
INSERT {
  GRAPH <http://publishmydata.com/graphs/drafter/drafts> {
    ?g a <http://publishmydata.com/def/drafter/ManagedGraph> .
    ?g <http://publishmydata.com/def/drafter/isPublic> true .
  }
} WHERE {
  SELECT DISTINCT ?g
  WHERE {
    GRAPH ?g {
       ?s ?p ?o .
    }
  }
}
```
