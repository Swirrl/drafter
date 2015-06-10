(ns drafter.rdf.rewriting.query-rewriting
  "Functions related to syntactically rewriting drafter queries."
  (:require [clojure.zip :as z]
            [clojure.tools.logging :as log]
            [drafter.rdf.rewriting.arq :refer [apply-rewriter]])
  (:import [org.apache.jena.sparql.sse SSE Item ItemList]
           [org.apache.jena.graph NodeFactory]))

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
                            (map str (vals live->draft)))]
    (str (apply-rewriter (partial uri-constant-rewriter live->draft) query-str))))


(comment

  ;; An example of using the rewriter

  (apply-rewriter (partial uri-constant-rewriter {"http://foo.com/" "http://bar.com/"})
                  "SELECT * WHERE { GRAPH <http://foo.com/> { ?s ?p ?o }}")


)
