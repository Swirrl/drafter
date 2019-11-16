(ns oom-check
  (:require [clj-time.coerce :refer [to-date]]
            [clj-time.core :as time]
            [clj-http.client :as http]
            [clojure.java.io :as io]
            [drafter.main :as main]
            [environ.core :refer [env]]
            [integrant.core :as ig]
            [cheshire.core :as json]
            [grafter-2.rdf.protocols :as pr]
            [grafter-2.rdf4j.io :as gio]
            [ring.util.io :refer [piped-input-stream]])
  (:import [com.auth0.jwk Jwk JwkProvider]
           com.auth0.jwt.algorithms.Algorithm
           com.auth0.jwt.JWT
           [java.io FileInputStream FileOutputStream]
           java.net.URI
           java.security.KeyPairGenerator))

(defonce keypair
  (-> (KeyPairGenerator/getInstance "RSA")
      (doto (.initialize 4096))
      (.genKeyPair)))

(def pubkey (.getPublic keypair))
(def privkey (.getPrivate keypair))

(def alg (Algorithm/RSA256 pubkey privkey))

(defn token [iss aud sub role]
  (-> (JWT/create)
      (.withIssuer (str iss \/))
      (.withSubject sub)
      (.withAudience (into-array String [aud]))
      (.withExpiresAt (to-date (time/plus (time/now) (time/minutes 10))))
      (.withClaim "scope" role)
      (.sign alg)))

(def jwt
  (format "Bearer %s"
          (token (env :auth0-domain)
                 (env :auth0-aud)
                 "publisher@swirrl.com"
                 "drafter:publisher")))

(defn mock-jwk []
  (reify JwkProvider
    (get [_ _]
      (proxy [Jwk] ["" "" "RSA" "" '() "" '() "" {}]
        (getPublicKey [] (.getPublic keypair))))))

;; Override the :drafter.auth.auth0/jwk init-key otherwise it'll be trying to
;; contact auth0
(defmethod ig/init-key :drafter.auth.auth0/jwk [_ {:keys [endpoint] :as opts}]
  (mock-jwk))

;; But this is the one that everything should use anyway
(defmethod ig/init-key :drafter.auth.auth0/mock-jwk [_ {:keys [endpoint] :as opts}]
  (mock-jwk))

(defn resfile [filename]
  (or (some-> filename io/resource io/file .getCanonicalPath)
      (throw (Exception. (format "Cannot find %s on resource path" filename)))))

(defn start-drafter-server []
  (main/-main (resfile "drafter-client-test-config.edn")))

(defn stop-drafter-server []
  (main/stop-system!))

(def draftset-id
  (-> "http://publisher%40swirrl.com:password@localhost:3002/v1/draftsets"
      (http/post {:params {"name" "test" "description" "test"}
                  :headers {"Authorization" jwt}})
      (:body)
      (json/parse-string keyword)
      (:id)))


(defn infinite-test-triples
  "Generate an infinite amount of test triples.  Take the amount you
  need!"
  [graph]
  (->> (range)
       (map (fn [i]
              (pr/->Quad (URI. (str "http://s/" i))
                         (URI. (str "http://p/" i))
                         i
                         graph)))))

;; (pr/add (gio/rdf-writer "/tmp/test.nq" :format :nq)
;;         (take 1 (infinite-test-triples (URI. "http://test-graph"))))

;; (pr/to-statements (piped-input-stream
;;                    (fn [outstream]
;;                      (with-open [writer (gio/rdf-writer outstream :format :nq)]
;;                        (pr/add writer (take 1 (infinite-test-triples (URI. "http://test-graph")))))))
;;                   {:format :nq})

(defn upload-triples [n]
  (let [stream (piped-input-stream
                (fn [outstream]
                  (with-open [writer (gio/rdf-writer outstream :format :nq)]
                    (pr/add writer (take n (infinite-test-triples (URI. "http://test-graph"))))
                    (.flush writer))))]
    (-> (format "http://publisher%%40swirrl.com:password@localhost:3002/v1/draftset/%s/data" draftset-id)
        (http/put {:method "PUT"
                   :body stream
                   :headers {"Content-Type"  "application/n-quads"
                             "Accept"        "application/json"
                             "Authorization" jwt}
                   :throw-exceptions false
                   :as :json})
        (clojure.pprint/pprint))))
