(ns immutant.init
  (:require [drafter.core :as core]
            [drafter.import-rdf]
            [immutant.web :as web]))

(web/start core/drafter-app)
