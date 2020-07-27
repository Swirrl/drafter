(ns drafter.backend.draftset.rewrite-query
  "Functions related to syntactically rewriting drafter queries."
  (:require [clojure.tools.logging :as log]
            [clojure.zip :as z]
            [drafter.backend.draftset.arq :as arq :refer [apply-rewriter]])
  (:import org.apache.jena.graph.NodeFactory
           org.apache.jena.sparql.sse.Item))

(defn- uri-node?
  "Predicate function that tests if the Item is a URI node."
  [i]
  (and (instance? Item i)
       (.isNodeURI i)))

(defn uri-constant-rewriter
  "Takes a map m from URI-string to URI-string representing the syntactic
  substitutions to perform and an SSE tree.

  We walk the SSE tree performing substitutions and return a transformed SSE
  tree.

  Note: this function should normally be wrapped with the apply-rewriter
  function to rebuild the rest of the SPARQL string, including any prefixes
  and the query type (SELECT/CONSTRUCT etc)."
  [m ssez]

  (let [ssez (if (uri-node? (z/node ssez))
               (z/edit ssez
                       (fn [guri]
                         (NodeFactory/createURI (let [graph-uri (str (.getNode guri))]
                                                  (get m graph-uri graph-uri)))))
               ssez)]
    (if (z/end? ssez)
      (z/root ssez)
      (recur m (z/next ssez)))))

;rewrite-sparql-string :: Map[Uri, Uri] -> String -> String
(defn rewrite-sparql-string
  "Parses a SPARQL query string, rewrites it according to the given
  live->draft graph mapping and then returns the re-written query
  serialised as a string."
  [live->draft query-str]
  (log/info "Rewriting query " query-str)

  (let [live->draft (zipmap (map str (keys live->draft))
                            (map str (vals live->draft)))
        rewritten-query (str (apply-rewriter (partial uri-constant-rewriter live->draft) query-str))]

    (log/debug "Rewritten query: " rewritten-query)
    rewritten-query))

(defn rewrite-sparql-update [live->draft update-str]
  (log/info "Rewriting update " update-str)

  (let [live->draft (zipmap (map str (keys live->draft))
                            (map str (vals live->draft)))
        rewriter (partial uri-constant-rewriter live->draft)
        rewritten-update (->> update-str (arq/rewrite-update rewriter) str)]

    (log/debug "Rewritten update: " rewritten-update)
    rewritten-update))


(comment

  ;; An example of using the rewriter

  (apply-rewriter (partial uri-constant-rewriter {"http://foo.com/" "http://bar.com/"})
                  "SELECT * WHERE { GRAPH <http://foo.com/> { ?s ?p ?o }}")

  (apply-rewriter (partial uri-constant-rewriter {})
                  "SELECT DISTINCT ?mdg
WHERE  {
  VALUES ?mdg {  }
         GRAPH ?mdg {
           ?ds <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://publishmydata.com/def/dataset#Dataset> .
         }
}")


  (->
   (partial uri-constant-rewriter {"http://a" "http://b"})
   (apply-rewriter "
INSERT { GRAPH ?g { <http://a> ?p ?o } }
"))

  (arq/rewrite-update
   (partial uri-constant-rewriter {"http://a" "http://b"})
   "
INSERT { GRAPH ?g { <http://a> ?p ?o } }
WHERE  { GRAPH ?g { <http://a> ?p ?o } }")

(arq/rewrite-update
   (partial uri-constant-rewriter {"http://a" "http://b"})
   "
INSERT { GRAPH ?g { <http://a> ?p ?o } }")


)
