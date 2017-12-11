(ns drafter.backend
  (:require [clojure.spec.alpha :as s]
            [drafter.backend.draftset :as draftsets]
            [drafter.backend.live :as live]
            [drafter.backend.spec :as bs]
            [grafter.rdf4j.repository :as repo]
            [integrant.core :as ig]
            [grafter.url :as url]
            [drafter.draftset :as ds])
  (:import java.net.URI
           drafter.draftset.DraftsetId))

;; A DrafterBackend is intended to be(come) the top level API access
;; to the drafter service.
;;
;; From here you can ask for sub-repositories, that are repo like
;; things which implement our various restrictions/rewriting etc.
;;
;; The root object is itself a repository, that has access to the
;; whole database, so queries on this directly can query the drafter
;; state-graph etc, but calling (endpoint-repo ::live drafter-service)
;; will return a repository that is restricted to the live/public
;; endpoint.
;;
;; In the future this would be a natural place to add internal
;; functions to the drafter API, so it can be used as a drafter
;; client etc...
(defrecord DrafterService [uncached-repo stasher-repo]
  repo/ToConnection
  (repo/->connection [this]
    ;; For now return the uncached connection here; though in the
    ;; future we should be able to cache them too.
    ;;
    ;; Note calling ->connection on DrafterService returns an
    ;; unrestricted endpoint, that has access to the state graph
    ;; and can perform updates etc...
    (repo/->connection uncached-repo)))

(defmulti endpoint-repo*
  "The multimethod that backs endpoint-repo, end users should use endpoint-repo."
  (fn [drafter endpoint-id opts]
    (if (keyword? endpoint-id)
      endpoint-id
      (type endpoint-id))))

;; named endpoint's
(defmethod endpoint-repo* ::live [drafter endpoint-id opts]
  (live/live-endpoint-with-stasher drafter))

;; draftsets
(defmethod endpoint-repo* String [drafter draftset-ref {:keys [union-with-live?]}]
  (draftsets/build-draftset-endpoint drafter (ds/->DraftsetId draftset-ref) union-with-live?))

(defmethod endpoint-repo* URI [drafter draftset-ref {:keys [union-with-live?]}]
  (draftsets/build-draftset-endpoint drafter draftset-ref union-with-live?))

(defmethod endpoint-repo* DraftsetId [drafter draftset-ref {:keys [union-with-live?]}]
  (draftsets/build-draftset-endpoint drafter draftset-ref union-with-live?))

(defn drafter-repo
  "Return a repository from the backend that has acccess to the
  drafter state graph and information."
  [{:keys [::bs/uncached-repo]}]

  ;; TODO when drafter 
  uncached-repo)

(defn endpoint-repo
  "Given a drafter backend and an endpoint id, return a repository on
  the endpoint.  Hard coded endpoints should be identified by
  keywords, dynamic draftset endpoints by their URI's or String ID
  slugs."
  ([drafter endpoint-id]
   (endpoint-repo* drafter endpoint-id {}))
  ([drafter endpoint-id opts]
   (endpoint-repo* drafter endpoint-id opts)))

(defmethod ig/pre-init-spec :drafter/backend [_]
  (s/keys :req-un [::bs/uncached-repo ::bs/stasher-repo]))

(defmethod ig/init-key :drafter/backend [_ opts]
  (map->DrafterService opts))


