(ns drafter.stasher.timing
  "Functions to create query results and result handlers instances that log
   timing metrics to DataDog"
  (:require [cognician.dogstatsd :as dd]
            [drafter.stasher.cancellable :refer [Cancellable cancel]])
  (:import (org.eclipse.rdf4j.query GraphQueryResult TupleQueryResult
                                    TupleQueryResultHandler)
           (org.eclipse.rdf4j.rio RDFHandler)))

(defn graph-result
  ([metric-name ^GraphQueryResult delegate]
   (graph-result metric-name delegate (System/currentTimeMillis)))
  ([metric-name delegate start-time]
   (reify GraphQueryResult
     (getNamespaces [_]
       (.getNamespaces delegate))
     (close [_]
       (.close delegate)
       (dd/histogram! metric-name (- (System/currentTimeMillis) start-time)))
     (hasNext [_]
       (.hasNext delegate))
     (next [_]
       (.next delegate))
     (remove [_]
       (.remove delegate)))))

(defn tuple-result
  ([metric-name ^TupleQueryResult  delegate]
   (tuple-result metric-name delegate (System/currentTimeMillis)))
  ([metric-name ^TupleQueryResult delegate start-time]
   (reify
     TupleQueryResult
     (getBindingNames [_]
       (.getBindingNames delegate))
     (close [_]
       (.close delegate)
       (dd/histogram! metric-name (- (System/currentTimeMillis) start-time)))
     (hasNext [_]
       (.hasNext delegate))
     (next [_]
       (.next delegate)))))

(defn tuple-handler
  ([metric-name ^TupleQueryResultHandler delegate]
   (tuple-handler metric-name delegate (System/currentTimeMillis)))
  ([metric-name ^TupleQueryResultHandler delegate start-time]
   (reify
     TupleQueryResultHandler
     (endQueryResult [_]
       (.endQueryResult delegate)
       (dd/histogram! metric-name (- (System/currentTimeMillis) start-time)))
     (handleLinks [_ links]
       (.handleLinks delegate links))
     (handleBoolean [_ bool]
       (.handleBoolean delegate bool))
     (handleSolution [_ binding-set]
       (.handleSolution delegate binding-set))
     (startQueryResult [_ binding-names]
       (.startQueryResult delegate binding-names))

     java.io.Closeable
     (close [t]
       (.close delegate))

     Cancellable
     (cancel [t]
       (cancel delegate)))))

(defn rdf-handler
  ([metric-name ^RDFHandler delegate]
   (rdf-handler metric-name delegate (System/currentTimeMillis)))
  ([metric-name ^RDFHandler delegate start-time]
   (reify
     RDFHandler
     (startRDF [_]
       (.startRDF delegate))
     (endRDF [_]
       (.endRDF delegate)
       (dd/histogram! metric-name (- (System/currentTimeMillis) start-time)))
     (handleStatement [_ statement]
       (.handleStatement delegate statement))
     (handleComment [_ comment]
       (.handleComment delegate comment))
     (handleNamespace [_ prefix-str uri-str]
       (.handleNamespace delegate prefix-str uri-str))

     java.io.Closeable
     (close [t]
       (.close delegate))

     Cancellable
     (cancel [t]
       (cancel delegate)))))
