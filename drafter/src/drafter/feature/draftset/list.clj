(ns drafter.feature.draftset.list
  (:require [drafter.middleware :as middleware]
            [ring.util.response :as ring]
            [drafter.backend.draftset.operations :as dsops]
            [clojure.spec.alpha :as s]
            [drafter.user :as user]
            [drafter.rdf.drafter-ontology :refer :all]
            [drafter.routes.draftsets-api :refer [parse-union-with-live-handler]]
            [integrant.core :as ig]
            [drafter.feature.endpoint.public :as pub]
            [drafter.endpoint :as ep]))

(defn- role-scores-values-clause [scored-roles]
  (let [score-pairs (map (fn [[r v]] (format "(\"%s\" %d)" (name r) v)) scored-roles)]
    (clojure.string/join " " score-pairs)))

(defn- user-is-in-claim-role-clause [user]
  (let [role (user/role user)
        user-role-score (role user/role->permission-level)]
    (str
      "{"
      "   VALUES (?role ?rv) { " (role-scores-values-clause user/role->permission-level) " }"
      "  ?ds <" drafter:hasSubmission "> ?submission ."
      "  ?submission <" drafter:claimRole "> ?role ."
      "  FILTER (" user-role-score " >= ?rv)"
      "}")))

(defn- user-is-claim-user-clause [user]
  (str
    "{"
    "  ?ds <" drafter:hasSubmission "> ?submission ."
    "  ?submission <" drafter:claimUser "> <" (user/user->uri user) "> ."
    "}"))

(defn- user-is-submitter-clause [user]
  (str
    "{"
    "  ?ds <" drafter:submittedBy "> <" (user/user->uri user) "> ."
    "  FILTER NOT EXISTS { ?ds <" drafter:hasOwner "> ?owner }"
    "}")
  )

(defn- user-is-owner-clause [user]
  (str "{ ?ds <" drafter:hasOwner "> <" (user/user->uri user) "> . }"))

(defn user-claimable-clauses [user]
  [(user-is-in-claim-role-clause user)
   (user-is-claim-user-clause user)
   (user-is-submitter-clause user)])

(defn user-all-visible-clauses [user]
  (conj (user-claimable-clauses user)
        (user-is-owner-clause user)))

(defn get-all-draftsets-info [repo user]
  (dsops/get-all-draftsets-by repo (user-all-visible-clauses user)))

(defn get-draftsets-claimable-by [repo user]
  (dsops/get-all-draftsets-by repo (user-claimable-clauses user)))

(defn get-draftsets-owned-by [repo user]
  (dsops/get-all-draftsets-by repo [(user-is-owner-clause user)]))

(defn get-draftsets [backend user include union-with-live?]
  (let [draftsets (case include
                    :all (get-all-draftsets-info backend user)
                    :claimable (get-draftsets-claimable-by backend user)
                    :owned (get-draftsets-owned-by backend user))]
    (if (and union-with-live?
             (seq draftsets))
      (let [public (pub/get-public-endpoint backend)]
        (map (fn [ds] (ep/merge-endpoints public ds)) draftsets))
      draftsets)))

(defn get-draftsets-handler
  ":get /draftsets"
  [{backend :drafter/backend}]
  (middleware/wrap-authorize :editor
    (middleware/include-endpoints-param
      (parse-union-with-live-handler
        (fn [{user :identity {:keys [include union-with-live]} :params :as request}]
          (ring/response (get-draftsets backend user include union-with-live)))))))

(defmethod ig/pre-init-spec ::get-draftsets-handler [_]
  (s/keys :req [:drafter/backend]))

(defmethod ig/init-key ::get-draftsets-handler [_ opts]
  (get-draftsets-handler opts))
