(ns drafter.rdf.arq
  (:import [com.hp.hpl.jena.query QueryFactory Syntax]))

(defn sparql-string->ast [query-str]
  (QueryFactory/create query-str Syntax/syntaxSPARQL_11))

;->sparql-string :: AST -> String
(defn ->sparql-string
  "Converts a SPARQL AST back into a query string."
  [query-ast]
  (.serialize query-ast Syntax/syntaxSPARQL_11))
