(ns drafter.feature.draftset.set-metadata-test
  (:require [clojure.test :as t]
            [drafter.test-common :as tc]))

(t/use-fixtures :each tc/with-spec-instrumentation)