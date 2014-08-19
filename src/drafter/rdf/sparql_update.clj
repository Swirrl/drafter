(ns drafter.rdf.sparql-update
  (:require [grafter.rdf.sesame :refer [prepare-update]]
            [taoensso.timbre :as timbre]))

(defn make-rewritten-update [repo update-str graphs]
  (timbre/warn "TODO implement make-rewritten-update")
  (prepare-update repo update-str))
