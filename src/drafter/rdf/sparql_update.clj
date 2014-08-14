(ns drafter.rdf.sparql-update
  (:require [grafter.rdf.sesame :as ses]
            [taoensso.timbre :as timbre]))

(defn make-rewritten-update [repo update-str graphs]
  (timbre/warn "TODO implement make-rewritten-update")
  (ses/prepare-update repo update-str))
