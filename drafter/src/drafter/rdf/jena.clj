(ns drafter.rdf.jena
  (:require [grafter-2.rdf.protocols :as pr])
  (:import [org.apache.jena.graph NodeFactory]
           [org.apache.jena.datatypes TypeMapper]
           [org.apache.jena.datatypes.xsd XSDDatatype]
           [java.time.format DateTimeFormatter]
           [java.time LocalDateTime]
           [org.apache.jena.sparql.core Quad]
           [org.apache.jena.sparql.modify.request QuadDataAcc UpdateDataInsert UpdateDataDelete]
           [org.apache.jena.update UpdateRequest Update]))

(def ^TypeMapper type-mapper
  (doto (TypeMapper.)
    (XSDDatatype/loadXSDSimpleTypes)))

(defn- format-date-time [x]
  (.format (DateTimeFormatter/ofPattern "uuuu-MM-dd'T'HH:mm:ss.SSS") x))

(defn- ^String value-str [x]
  ;; if we have a `LocalDateTime` which should get mapped to XMLSchema#dateTime
  ;; the default toString implementation may render an unparsable format, if
  ;; the seconds/milliseconds fields read zero.
  ;; See: https://docs.oracle.com/javase/8/docs/api/java/time/LocalDateTime.html#toString--
  (if (instance? LocalDateTime x)
    (format-date-time x)
    (str (pr/raw-value x))))

(defn- ->literal [x]
  (let [t (.getSafeTypeByName type-mapper (str (pr/datatype-uri x)))
        lang (when (satisfies? pr/IRDFString x)
               (some-> x pr/lang name))]
    (NodeFactory/createLiteral (value-str x) lang t)))

(defn- ->jena-quad [{:keys [c s p o]}]
  (letfn [(->node [x]
            (if (uri? x)
              (NodeFactory/createURI (str x))
              (->literal x)))]
    (let [[c s p o] (map ->node [c s p o])]
      (Quad. c s p o))))

(defn insert-data-stmt
  "Creates a Jena data insert update from a sequence of grafter quads"
  [quads]
  (UpdateDataInsert. (QuadDataAcc. (map ->jena-quad quads))))

(defn delete-data-stmt
  "Creates a Jena data delete update from a sequences of grafter quads"
  [quads]
  (UpdateDataDelete. (QuadDataAcc. (map ->jena-quad quads))))

(defprotocol JenaUpdateOperation
  "A representation of an UPDATE operation which can be added to an UpdateRequest"
  (add-to-update [this update]
    "Adds this operation to "))

(extend-protocol JenaUpdateOperation
  String
  (add-to-update [^String this ^UpdateRequest update]
    (.add update this))

  Update
  (add-to-update [^Update this ^UpdateRequest update]
    (.add update this)))

(defn ->update
  "Converts a sequence of update operations into an UpdateRequest"
  [update-operations]
  (let [update (UpdateRequest.)]
    (doseq [u update-operations]
      (add-to-update u update))
    update))

(defn ->update-string
  "Converts a sequence of update operations into a SPARQL UPDATE string"
  [update-operations]
  (str (->update update-operations)))
