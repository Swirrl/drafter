(ns drafter.configuration-test
  (:require [drafter.configuration :refer :all]
            [clojure.test :refer :all]
            [schema.test :refer [validate-schemas]]))

(use-fixtures :each validate-schemas)
