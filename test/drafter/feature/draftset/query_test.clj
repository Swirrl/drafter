(ns drafter.feature.draftset.query-test
  (:require [clojure.test :as t]
            [drafter.test-common :as tc]))

(t/use-fixtures :each tc/with-spec-instrumentation)
