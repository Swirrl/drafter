(ns drafter-swagger-client.api.updating-data
  (:require [drafter-swagger-client.core :refer [call-api check-required-params with-collection-format]])
  (:import (java.io File)))

(defn draftset-id-changes-delete-with-http-info
  "Remove all the changes to a named graph from the Draftset
  Removes all of the changes to the specified graph from the
Draftset.

This route is different to `DELETE /draftset/{:id}/graph` in
that it is about undoing changes you have made and not about
scheduling deletions against live.

This route is equivalent to explicitly deleting all the
triples from a named graph within the draftset with `DELETE
/draftset/{:id}/data`, the benefit to using this route is that
you don\\'t need to list all the triples contained within the
graph."
  [id graph ]
  (check-required-params id graph)
  (call-api "/draftset/{id}/changes" :delete
            {:path-params   {"id" id }
             :header-params {}
             :query-params  {"graph" graph }
             :form-params   {}
             :content-types ["application/json"]
             :accepts       ["application/json"]
             :auth-names    ["jws-auth"]}))

(defn draftset-id-changes-delete
  "Remove all the changes to a named graph from the Draftset
  Removes all of the changes to the specified graph from the
Draftset.

This route is different to `DELETE /draftset/{:id}/graph` in
that it is about undoing changes you have made and not about
scheduling deletions against live.

This route is equivalent to explicitly deleting all the
triples from a named graph within the draftset with `DELETE
/draftset/{:id}/data`, the benefit to using this route is that
you don\\'t need to list all the triples contained within the
graph."
  [id graph ]
  (:data (draftset-id-changes-delete-with-http-info id graph)))

(defn draftset-id-data-delete-with-http-info
  "Remove the supplied RDF data from this Draftset
  Removes the supplied data from the Draftset identified by this
resource.

If the RDF data is supplied in a quad serialisation then the graph query parameter can be ommited, and the supplied quads will be removed from this Draftset.

If quads are uploaded and a graph parameter is specified all quads will be treated as if they are triples in the specified graph, and will be removed from it.

If a graph parameter is supplied then
the RDF data can be supplied in a triple serialisation.  The supplied triples will be removed from the Draftset."
  ([id data ] (draftset-id-data-delete-with-http-info id data nil))
  ([id data {:keys [graph ]}]
   (check-required-params id data)
   (call-api "/draftset/{id}/data" :delete
             {:path-params   {"id" id }
              :header-params {}
              :query-params  {"graph" graph }
              :form-params   {}
              :body-param    data
              :content-types ["application/n-quads" "application/trig" "application/trix" "application/n-triples" "application/rdf+xml" "text/turtle"]
              :accepts       ["application/json"]
              :auth-names    ["jws-auth"]})))

(defn draftset-id-data-delete
  "Remove the supplied RDF data from this Draftset
  Removes the supplied data from the Draftset identified by this
resource.

If the RDF data is supplied in a quad serialisation then the graph query parameter can be ommited, and the supplied quads will be removed from this Draftset.

If quads are uploaded and a graph parameter is specified all quads will be treated as if they are triples in the specified graph, and will be removed from it.

If a graph parameter is supplied then
the RDF data can be supplied in a triple serialisation.  The supplied triples will be removed from the Draftset."
  ([id data ] (draftset-id-data-delete id data nil))
  ([id data optional-params]
   (:data (draftset-id-data-delete-with-http-info id data optional-params))))

(defn draftset-id-data-put-with-http-info
  "Append the supplied RDF data to this Draftset
  Appends the supplied data to the Draftset identified by this
resource.

If the RDF data is supplied in a quad serialisation then the
graph query parameter can be ommited.  If quads are uploaded
and a graph parameter is specified the graph parameter will
take precedence, causing all quads to be loaded into the same
graph.

If a graph parameter is supplied then
the RDF data can be supplied in a triple serialisation."
  ([id data ] (draftset-id-data-put-with-http-info id data nil))
  ([id data {:keys [graph ]}]
   (check-required-params id data)
   (call-api "/draftset/{id}/data" :put
             {:path-params   {"id" id }
              :header-params {}
              :query-params  {"graph" graph }
              :form-params   {}
              :body-param    data
              :content-types (if graph
                               ["application/n-triples"]
                               ["application/n-quads"])
              :accepts       ["application/json"]
              :auth-names    ["jws-auth"]})))

(defn draftset-id-data-put
  "Append the supplied RDF data to this Draftset
  Appends the supplied data to the Draftset identified by this
resource.

If the RDF data is supplied in a quad serialisation then the
graph query parameter can be ommited.  If quads are uploaded
and a graph parameter is specified the graph parameter will
take precedence, causing all quads to be loaded into the same
graph.

If a graph parameter is supplied then
the RDF data can be supplied in a triple serialisation."
  ([id data ] (draftset-id-data-put id data nil))
  ([id data optional-params]
   (:data (draftset-id-data-put-with-http-info id data optional-params))))

(defn draftset-id-graph-delete-with-http-info
  "Delete the contents of a graph in this Draftset
  Schedules the deletion of the specified graph from live and
deletes its contents from the Draftset.  At publication time
the specified graph will be removed from the live site.

If you wish to undo changes you have made in a draftset you
should not use this route, and should use

`DELETE /draftset/{id}/changes` instead.

If the silent is true the request will always complete successfully,
otherwise if the specified graph does not exist in live then a 422 error
will be returned."
  ([id graph ] (draftset-id-graph-delete-with-http-info id graph nil))
  ([id graph {:keys [silent ]}]
   (check-required-params id graph)
   (call-api "/draftset/{id}/graph" :delete
             {:path-params   {"id" id }
              :header-params {}
              :query-params  {"graph" graph "silent" silent }
              :form-params   {}
              :content-types ["application/json"]
              :accepts       ["application/json"]
              :auth-names    ["jws-auth"]})))

(defn draftset-id-graph-delete
  "Delete the contents of a graph in this Draftset
  Schedules the deletion of the specified graph from live and
deletes its contents from the Draftset.  At publication time
the specified graph will be removed from the live site.

If you wish to undo changes you have made in a draftset you
should not use this route, and should use

`DELETE /draftset/{id}/changes` instead.

If the silent is true the request will always complete successfully,
otherwise if the specified graph does not exist in live then a 422 error
will be returned."
  ([id graph ] (draftset-id-graph-delete id graph nil))
  ([id graph optional-params]
   (:data (draftset-id-graph-delete-with-http-info id graph optional-params))))

(defn draftset-id-graph-put-with-http-info
  "Copy a graph from live into this Draftset
  Copies the contents of the specified live graph into this
Draftset.

If the specified graph does not exist in live then a 422 error
will be returned."
  [id graph ]
  (check-required-params id graph)
  (call-api "/draftset/{id}/graph" :put
            {:path-params   {"id" id }
             :header-params {}
             :query-params  {"graph" graph }
             :form-params   {}
             :content-types ["application/json"]
             :accepts       ["application/json"]
             :auth-names    ["jws-auth"]}))

(defn draftset-id-graph-put
  "Copy a graph from live into this Draftset
  Copies the contents of the specified live graph into this
Draftset.

If the specified graph does not exist in live then a 422 error
will be returned."
  [id graph ]
  (:data (draftset-id-graph-put-with-http-info id graph)))
