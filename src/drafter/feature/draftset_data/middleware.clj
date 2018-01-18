(ns drafter.feature.draftset-data.middleware
  (:require [drafter.rdf.sesame :refer [is-triples-format?]]
            [drafter.responses :refer [unprocessable-entity-response]])
  (:import java.net.URI))

(defn require-graph-for-triples-rdf-format [inner-handler]
  (fn [{{:keys [rdf-format]} :params :as request}]
    (if (is-triples-format? rdf-format)
      (let [h (parse-graph-param-handler true inner-handler)]
        (h request))
      (inner-handler request))))
