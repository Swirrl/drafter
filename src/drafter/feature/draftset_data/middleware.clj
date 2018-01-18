(ns drafter.feature.draftset-data.middleware
  (:require [drafter.feature.middleware :as feat-middleware]
            [drafter.rdf.sesame :refer [is-triples-format?]]))

(defn require-graph-for-triples-rdf-format [inner-handler]
  (fn [{{:keys [rdf-format]} :params :as request}]
    (if (is-triples-format? rdf-format)
      (let [h (feat-middleware/parse-graph-param-handler true inner-handler)]
        (h request))
      (inner-handler request))))
