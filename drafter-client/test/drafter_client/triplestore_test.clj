(ns drafter-client.triplestore-test
  (:require [clojure.test :as t]
            [drafter-client.client-test :as ct]
            [drafter-client.client.draftset :as draftset]
            [drafter-client.test-helpers :as h]
            [drafter-client.test-util.auth :as auth-util]
            [drafter-client.triplestore :as sut]
            [grafter-2.rdf.protocols :as pr]
            [grafter-2.rdf4j.repository :as repo])
  (:import java.net.URI))

(t/use-fixtures :each
  h/with-spec-instrumentation
  ;; Drop db after tests
  h/db-fixture)

(t/use-fixtures :once
  ct/drafter-server-fixture)

(t/deftest grafter-repo-test
  (let [client (ct/drafter-client)
        triples (ct/test-triples)
        how-many 5
        token (auth-util/system-token)]
    (t/testing "adding triples to drafter grafter repo with-transaction"
      (let [graph (URI. "http://test.graph.com/triple-graph1")
            triples (take how-many triples)
            store (sut/auth-code-triplestore client token draftset/live)]
        (with-open [conn (repo/->connection store)]
          (repo/with-transaction conn
            (pr/add conn graph triples))
          (let [q "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"
                res (repo/query conn q)]
            (t/is (= (set triples) (set res)))))))))
