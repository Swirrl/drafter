(ns drafter.backend.draftset.spec
  (:require [clojure.spec.alpha :as s]
            [drafter.spec]
            [drafter.backend.spec]
            [drafter.backend.draftset :as draftset]
            [drafter.backend :as backend]))

(s/def :exec/live->draft ::backend/DraftsetGraphMapping)
(s/def ::draftset/RewritingSesameSparqlExecutor (s/keys :req-un [::backend/repo :exec/live->draft :drafter/union-with-live?]))

