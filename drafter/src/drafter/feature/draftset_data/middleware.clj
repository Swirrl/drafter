(ns drafter.feature.draftset-data.middleware
  (:require [drafter.feature.middleware :as feat-middleware]
            [drafter.rdf.sesame :refer [is-triples-format?]]))

(defn parse-graph-for-triples [inner-handler]
  (fn [{{:keys [rdf-format]} :params :as request}]
    (let [h (feat-middleware/parse-graph-param-handler (is-triples-format? rdf-format) inner-handler)]
      (h request))))
