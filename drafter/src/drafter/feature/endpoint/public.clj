(ns drafter.feature.endpoint.public
  (:require [integrant.core :as ig]
            [grafter-2.rdf.protocols :as gproto]
            [grafter-2.rdf4j.repository :as repo]
            [grafter-2.rdf4j.sparql :as sp]
            [drafter.endpoint :as ep]
            [clojure.spec.alpha :as s]
            [drafter.rdf.drafter-ontology :refer [drafter:endpoints drafter:public drafter:Endpoint]]
            [grafter.vocabularies.dcterms :refer [dcterms:modified dcterms:issued]]
            [drafter.middleware :refer [require-role]]
            [drafter.responses :as response]
            [grafter-2.rdf4j.io :as gio]
            [drafter.util :as util]
            [ring.util.response :as ring])
  (:import [java.time ZonedDateTime]
           [java.time.format DateTimeParseException]))

(defn- ensure-public-endpoint-query
  ([] (ensure-public-endpoint-query nil))
  ([created-at]
   (let [created-at-expr (if created-at
                           (str (gio/->backend-type created-at))
                           "now()")]
     (str "INSERT {"
          "  GRAPH <" drafter:endpoints "> {"
          "    <" drafter:public "> a <" drafter:Endpoint "> ;"
          "                         <" dcterms:modified "> ?n ;"
          "                         <" dcterms:issued "> ?i ."
          "  }"
          "} WHERE {"
          "  GRAPH <" drafter:endpoints "> {"
          "    { SELECT * WHERE {"
          "        BIND(now() AS ?n)"
          "        BIND(" created-at-expr " AS ?i)"
          "      }"
          "    }"
          "    FILTER NOT EXISTS {"
          "      <" drafter:public "> a <" drafter:Endpoint "> ;"
          "                           <" dcterms:modified "> ?modified ;"
          "                           <" dcterms:issued "> ?created ."
          "    }"
          "  }"
          "}"))))

(defn ensure-public-endpoint
  "Ensures the public endpoint exists within the endpoints graph, creating it if
   necessary."
  ([repo] (ensure-public-endpoint repo nil))
  ([repo created-at]
   (with-open [conn (repo/->connection repo)]
     (let [u (ensure-public-endpoint-query created-at)]
       (gproto/update! conn u)))))

(defn get-public-endpoint
  "Returns a map representation of the public endpoint"
  [repo]
  (with-open [conn (repo/->connection repo)]
    (let [bindings (vec (sp/query "drafter/feature/endpoint/get_public_endpoint.sparql" conn))]
      (case (count bindings)
        0 nil
        1 (let [{:keys [created modified]} (first bindings)]
            {:id "public" :type "Endpoint" :created-at created :updated-at modified})
        (throw (ex-info "Found multiple public endpoints - expected at most one" {:bindings bindings}))))))

(s/fdef get-public-endpoint
  :args (s/cat :repo any?)
  :ret (s/nilable ::ep/Endpoint))

(defn- get-created-at
  "Parses the created-at parameter for the public endpoint if one is provided.
   If present it must parse as an ISO datetime. Returns nil if no created-at parameter
   exists on the request, an exception if the parameter exists but could not be parsed,
   or the parsed datetime if the parameter is valid."
  [params]
  (if-let [created-at-str (:created-at params)]
    (try
      (let [zdt (ZonedDateTime/parse created-at-str)]
        (.toOffsetDateTime zdt))
      (catch DateTimeParseException ex
        ex))))

(defn- create-public-endpoint
  "Creates the public endpoint if it does not already exist. Supports an optional created-at parameter
   specifying the creation time to use when creating the endpoint. The created-at parameter must represent
   a valid ISO datetime if provided. Returns a 201 response if the endpoint has been created or a 200 response
   if the endpoint already exists. In both these cases a JSON representation of the public endpoint is returned
   in the body of the response."
  [wrap-auth repo]
  (wrap-auth
    (require-role
      :system
      (fn [{:keys [params] :as request}]
        (let [created-at (get-created-at params)
              public-endpoint (get-public-endpoint repo)]
          (cond
            (util/throwable? created-at)
            (response/bad-request-response (.getMessage created-at))

            (some? public-endpoint)
            (ring/response public-endpoint)

            :else
            (do
              (ensure-public-endpoint repo created-at)
              (response/created-response (get-public-endpoint repo)))))))))

(defmethod ig/init-key ::create-handler [_ {:keys [wrap-auth repo]}]
  (create-public-endpoint wrap-auth repo))
