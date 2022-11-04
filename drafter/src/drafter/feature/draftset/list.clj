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

;; Fetches all drafts with any claim permission set, we still need to check
;; that the user actually has the set permission.
(def claim-permission-clause
  (str
   "{"
   "  ?ds <" drafter:hasSubmission "> ?submission ."
   "  ?submission <" drafter:claimPermission "> ?permission ."
   "}"))

(def claim-permission-clause2
  '[[?ds :drafter/hasSubmission ?submission]
    [?submission :drafter/claimPermission ?permission]])

(defn- user-is-claim-user-clause [user]
  (str
    "{"
    "  ?ds <" drafter:hasSubmission "> ?submission ."
    "  ?submission <" drafter:claimUser "> <" (user/user->uri user) "> ."
    "}"))

(defn- user-is-claim-user-clause2 [user]
  ['[?ds :drafter/hasSubmission ?submission]
   ['?submission :drafter/claimUser (user/user->uri user)]])

(defn- user-is-submitter-clause [user]
  (str
    "{"
    "  ?ds <" drafter:submittedBy "> <" (user/user->uri user) "> ."
    "  FILTER NOT EXISTS { ?ds <" drafter:hasOwner "> ?owner }"
    "}")
  )

(defn- user-is-submitter-clause2 [user]
  [['?ds :drafter/submittedBy (user/user->uri user)]
   [:filter '(not-exists [[?ds :drafter/hasOwner ?owner]])]])

;; Fetches all drafts with any view permission set, we still need to check that
;; the user actually has the set permission.
(def view-permission-clause
  (str "{ ?ds <" drafter:viewPermission "> ?permission }"))

(def view-permission-clause2
  '[[?ds :drafter/viewPermission ?permission]])

(defn- user-is-view-user-clause [user]
  (str
    "{ ?ds <" drafter:viewUser "> <" (user/user->uri user) "> }"))

(defn- user-is-view-user-clause2 [user]
  [['?ds :drafter/viewUser (user/user->uri user)]])

(defn- user-is-owner-clause [user]
  (str "{ ?ds <" drafter:hasOwner "> <" (user/user->uri user) "> . }"))

(defn- user-is-owner-clause2 [user]
  [['?ds :drafter/hasOwner (user/user->uri user)]])

(defn user-claimable-clauses [user]
  [claim-permission-clause
   (user-is-claim-user-clause user)
   (user-is-submitter-clause user)])

(defn user-claimable-clauses2 [user]
    [claim-permission-clause2
     (user-is-claim-user-clause2 user)
     (user-is-submitter-clause2 user)])

(defn user-all-visible-clauses [user]
  (conj (user-claimable-clauses user)
        (user-is-owner-clause user)
        view-permission-clause
        (user-is-view-user-clause user)))

(defn user-all-visible-clauses2 [user]
  (vec
    (concat
      (user-claimable-clauses2 user)
      [(user-is-owner-clause2 user)
       view-permission-clause2
       (user-is-view-user-clause2 user)])))

(defn get-all-draftsets-info [repo user]
  (filter #(user/can-view? user %)
          (dsops/get-all-draftsets-by repo {:v1 (user-all-visible-clauses user)
                                            :v2 (user-all-visible-clauses2 user)})))

(defn get-draftsets-claimable-by [repo user]
  (filter #(user/can-claim? user %)
          (dsops/get-all-draftsets-by repo {:v1 (user-claimable-clauses user)
                                            :v2 (user-claimable-clauses2 user)})))

(defn get-draftsets-owned-by [repo user]
  (dsops/get-all-draftsets-by repo {:v1 [(user-is-owner-clause user)]
                                    :v2 [(user-is-owner-clause2 user)]}))

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
  [{:keys [drafter/backend wrap-authenticate]}]
  (middleware/wrap-authorize wrap-authenticate :drafter:draft:view
    (middleware/include-endpoints-param
      (parse-union-with-live-handler
        (fn [{user :identity {:keys [include union-with-live]} :params :as request}]
          (ring/response (get-draftsets backend user include union-with-live)))))))

(defmethod ig/pre-init-spec ::get-draftsets-handler [_]
  (s/keys :req [:drafter/backend]))

(defmethod ig/init-key ::get-draftsets-handler [_ opts]
  (get-draftsets-handler opts))
