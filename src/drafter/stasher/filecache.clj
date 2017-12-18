(ns drafter.stasher.filecache
  (:require [clojure.core.cache :as cache]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [grafter.rdf4j.formats :as fmt]
            [grafter.rdf4j.io :as gio]
            [integrant.core :as ig]
            [me.raynes.fs :as fs])
  (:import java.io.File
           java.security.MessageDigest
           org.apache.commons.codec.binary.Hex
           (org.eclipse.rdf4j.query GraphQueryResult TupleQueryResultHandler)
           org.eclipse.rdf4j.rio.RDFFormat
           (org.eclipse.rdf4j.query.resultio BooleanQueryResultFormat BooleanQueryResultParserRegistry
                                             TupleQueryResultWriter TupleQueryResultFormat
                                             QueryResultIO TupleQueryResultParserRegistry)
           (org.eclipse.rdf4j.common.lang FileFormat)
           org.eclipse.rdf4j.query.impl.BackgroundGraphResult
           org.eclipse.rdf4j.query.resultio.helpers.BackgroundTupleResult
           [org.eclipse.rdf4j.rio RDFHandler RDFWriter]))

(def todo-replace-me-with-init-base-cache {}) ;; TODO replace me

(def default-cache-dir "stasher-cache")

(def rdf-formats [RDFFormat/BINARY
                  RDFFormat/NTRIPLES
                  RDFFormat/RDFXML
                  RDFFormat/RDFJSON
                  RDFFormat/TURTLE])

(def tuple-formats [TupleQueryResultFormat/BINARY
                    TupleQueryResultFormat/SPARQL
                    TupleQueryResultFormat/JSON])

(def boolean-formats [BooleanQueryResultFormat/TEXT
                      BooleanQueryResultFormat/JSON
                      BooleanQueryResultFormat/SPARQL])

(def supported-cache-formats (concat rdf-formats tuple-formats boolean-formats))

(defn build-format-keyword->format-map
  "Builds a hashmap from format keywords to RDFFormat's. 

  e.g. nt => RDFFormat/NTRIPLES etc..."
  [fmt-list]
  (->> fmt-list
       (mapcat (fn [fmt]
                 (mapcat (fn [ext]
                           [(keyword ext) fmt])
                         (.getFileExtensions fmt))))
       (apply hash-map)))

(defn get-format
  "Given a file path as a string or java.io.File return the RDF4j
  FileFormat object to parse it, based upon its extension in the RDF4j
  registry."
  [filename]
  (.orElse (FileFormat/matchFileName (str filename) supported-cache-formats) nil))

(defn get-parser
  "Given a filename find an RDF4j file format parser for that
  file extension.
  
  In the case where an extension is shared between parsers the first
  encountered in supported-cache-formats is used."
  [fmt]
  
  (when-let [parser-factory (.orElse (condp = (type fmt)
                                       RDFFormat (.get (org.eclipse.rdf4j.rio.RDFParserRegistry/getInstance) fmt)
                                       BooleanQueryResultFormat (.get (BooleanQueryResultParserRegistry/getInstance) fmt)
                                       TupleQueryResultFormat (.get (TupleQueryResultParserRegistry/getInstance) fmt)
                                       nil (java.util.Optional/empty)) nil)]
    (.getParser parser-factory)))

(def default-cache-rdf-format :brf)
(def default-cache-tuple-format #_:brt :srj)
(def default-cache-boolean-format :txt)

(def fmt-kw->rdf-format (build-format-keyword->format-map rdf-formats))
(def fmt-kw->tuple-format (build-format-keyword->format-map tuple-formats))
(def fmt-kw->boolean-format (build-format-keyword->format-map boolean-formats))


(defn backend-rdf-format [cache]
  (or (:backend-rdf-format (.opts cache)) default-cache-rdf-format))

(defn backend-tuple-format [cache]
  (or (:backend-tuple-format (.opts cache)) default-cache-tuple-format))

(defn backend-boolean-format [cache]
  (or (:backend-boolean-format (.opts cache)) default-cache-boolean-format))


#dbg (defmulti backend-format (fn [cache cache-key]
                           (:query-type cache-key)))

(defmethod backend-format :tuple [cache cache-key]
  (backend-tuple-format cache))

(defmethod backend-format :graph [cache cache-key]
  (backend-rdf-format cache))

(defmethod backend-format :boolean [cache cache-key]
  (backend-boolean-format cache))

(defn cache-key->hash-key
  "Cache keys at the interface to the cache are maps, but we need to
  repeatably hash these to an MD5 sum for the filename/location on
  disk.  This function takes a cache-key (map) and converts it into an
  MD5 sum."
  [cache-key]
  (let [cache-key-str (pr-str (if (coll? cache-key)
                                (sort cache-key)
                                cache-key))
        md (MessageDigest/getInstance "MD5")]
    (Hex/encodeHexString (.digest md (.getBytes cache-key-str)))))

(defn hash-key->file-path
  "Returns a relative path styled variant of the hash key with a
  supplied format extension appended.  e.g. given a hash-key of
  \"0cc23d1cefa62b120c5f3b289503c01e\" and an extension of :ext converts
  it into the string \"0c/c2/0cc23d1cefa62b120c5f3b289503c01e.ext\".
  "
  ([hash-key fmt-extension]
   (let [sub-dirs (->> hash-key
                       (partition 2)
                       (take 2)
                       (mapv (partial apply str)))]
     (apply io/file (conj sub-dirs (str hash-key "." (name fmt-extension)))))))

(defn cache-key->cache-path
  [cache cache-key]
  (let [fmt (backend-format cache cache-key)
        hash-key (hash-key->file-path (cache-key->hash-key cache-key) fmt)]
    (io/file (:dir (.opts cache)) hash-key)))

(defn- move-file-to-cache!
  "Move the supplied file into the cache under the specified
  cache-key.  Note this is a mutable operation that moves the file you
  supply on disk."
  [cache cache-key temp-file]
  (let [cache-key-fname (cache-key->cache-path cache cache-key)]
    (io/make-parents cache-key-fname)
    (fs/rename temp-file cache-key-fname)
    cache))

(defn remove-cache-entry [cache key]
  (let [fname (cache-key->cache-path cache key)]
    (fs/delete fname))
  cache)

(cache/defcache FileCache [base opts]
  cache/CacheProtocol

  ;; TODO implement these
  (cache/lookup [this item]
                (cache/lookup this item nil))
  
  (cache/lookup [this item not-found]
                (let [cache-fpath (cache-key->cache-path this item)]
                  (if (.exists cache-fpath)
                    cache-fpath ;; TODO wrap in rdf-handler
                    not-found)))

  (cache/has? [this item]
              (.exists (cache-key->cache-path this item)))
  
  (cache/hit [this item] this)

  ;; add file to cache here
  (cache/miss [this item result]
              (move-file-to-cache! this item result))
  
  (cache/evict [this key]
               (remove-cache-entry this key))
  
  (cache/seed [this base]

              ;dir tempdir backend-rdf-format
              ;(or (.dir this) dir) (or (.tempdir this) tempdir) (or (.backend-rdf-format this) tempdir)
              (FileCache. base opts))
  
  Object
  (toString [_] (str (:dir opts))))

(s/def ::base-cache #(satisfies? cache/CacheProtocol %))
(s/def ::dir fs/directory?)
(s/def ::persist-on-shutdown? boolean?) ;; use this property when using a cache in unit tests to prevent caching between test runs

;; TODO will need to actually honour the charset support when we
;; support other serialization backends other than :brf
(s/def ::charset string?)               ;; use nil if using :brf format

(s/def ::backend-rdf-format (set (keys fmt-kw->rdf-format)))
(s/def ::backend-tuple-format (set (keys (build-format-keyword->format-map tuple-formats))))
(s/def ::backend-boolean-format (set (keys (build-format-keyword->format-map boolean-formats))))

(defmethod ig/pre-init-spec :drafter.stasher/filecache [_]
  (s/and (s/keys :opt-un [::base-cache ::dir ::persist-on-shutdown? ::charset ::backend-rdf-format ::backend-tuple-format ::backend-boolean-format])))

(defn file-cache-factory [opts]
  (let [{:keys [dir base-cache] :as opts} (merge opts {:base-cache (or (:base-cache opts) {})
                                                       :dir (or (:dir opts) default-cache-dir)})]
    (fs/mkdir dir)
    (fs/mkdir (io/file dir "tmp"))
    (FileCache. base-cache opts)))

(defmethod ig/init-key :drafter.stasher/filecache [_ opts]
  (file-cache-factory opts))

(defmethod ig/halt-key! :drafter.stasher/filecache [k cache]
  (when-not (:persist-on-shutdown? (.opts cache) true)
    (log/info "Clearing" k)
    (fs/delete-dir (:dir (.opts cache)))))

(defn select-output-coercer [fmt]
  (let [fmt (or (fmt-kw->rdf-format fmt) (fmt-kw->tuple-format fmt))]
    (if (#{TupleQueryResultFormat/BINARY RDFFormat/BINARY} fmt)
      io/output-stream
      #_io/writer
      io/output-stream)))

(defn stashing-graph-query-result
  "Wrap a BackgroundGraphResult with one that will write the stream of
  RDF into a temp file and move the file into the cache when it's
  finished.

  For RDF pull query results."
  [cache cache-key ^BackgroundGraphResult bg-graph-result]
  (let [rdf-format (backend-rdf-format cache)
        temp-file (File/createTempFile "stasher" (str "tmp." (name rdf-format)) (io/file (.dir cache) "tmp"))
        make-stream (select-output-coercer rdf-format)
        stream (make-stream temp-file :buffer 8192)]

    (let [prefixes (.getNamespaces bg-graph-result) ;; take prefixes from supplied result
          cache-file-writer ^RDFWriter (gio/rdf-writer stream :format (fmt-kw->rdf-format rdf-format) :prefixes prefixes)]
      
      (.startRDF cache-file-writer)

      (reify GraphQueryResult
        (getNamespaces [this]
          prefixes)
        (close [this]
          (try
            (.endRDF cache-file-writer)
            (move-file-to-cache! cache cache-key temp-file)
            (.close bg-graph-result)
            (catch Throwable ex
              (.delete temp-file)
              (throw ex))))
        (hasNext [this]
          (try
            (.hasNext bg-graph-result)
            (catch Throwable ex
              (.delete temp-file)
              (throw ex))))
        (next [this]
          (try
            (let [quad (.next bg-graph-result)]
              (.handleStatement cache-file-writer quad)
              quad)
            (catch Throwable ex
              (.delete temp-file)
              (throw ex))))
        (remove [this]
          (try 
            (.remove bg-graph-result)
            (catch Throwable ex
              (.delete temp-file)
              (throw ex))))))))

(defn tuple-writer [destination format]
  (QueryResultIO/createTupleWriter (fmt-kw->tuple-format format) destination))


(defn stashing-tuple-query-result
  "Wrap a BackgroundTupleResult with one that will write the stream of
  RDF into a temp file and move the file into the cache when it's
  finished.

  For RDF pull query results."
  [cache cache-key ^BackgroundTupleResult bg-tuple-result]
  (let [tuple-format (backend-tuple-format cache)
        temp-file (File/createTempFile "stasher" (str "tmp." (name tuple-format)) (io/file (:dir (.opts cache)) "tmp"))
        make-stream (select-output-coercer tuple-format)
        stream (make-stream temp-file :buffer 8192)]

    (let [cache-file-writer ^TupleQueryResultWriter (tuple-writer stream tuple-format)
          bindings (.getBindingNames bg-tuple-result)]

      ;; copy the projected variables/column names across to the
      ;; cached file.
      (.startQueryResult cache-file-writer bindings)
      
      (reify
        #_TupleQueryResultHandler
        #_(startQueryResult [this binding-names]
          (.startQueryResult cache-file-writer binding-names))
        
        TupleQueryResult
        (getBindingNames [this]
          (.getBindingNames bg-tuple-result))
        (close [this]
          (try
            (.endQueryResult cache-file-writer)
            (move-file-to-cache! cache cache-key temp-file)
            (.close bg-tuple-result)
            (catch Throwable ex
              (.delete temp-file)
              (throw ex))))

        (hasNext [this]
          (try
            (.hasNext bg-tuple-result)
            (catch Throwable ex
              (.delete temp-file)
              (throw ex))))
        
        (next [this]
          (try
            (let [solution (.next bg-tuple-result)]
              (.handleSolution cache-file-writer solution)
              solution)
            (catch Throwable ex
              (.delete temp-file)
              (throw ex))))))))

(defn stashing-rdf-handler
  "Wrap an RDFHandler with one that will write the stream of RDF into
  a temp file and move the file into the cache when it's finished.

  For RDF push query results."
  [cache cache-key inner-rdf-handler]
  (let [rdf-format (backend-rdf-format cache)
        temp-file (File/createTempFile "stasher" (str "tmp." (name rdf-format)) (io/file (:dir (.opts cache)) "tmp"))
        make-stream (select-output-coercer rdf-format)
        stream (make-stream temp-file :buffer 8192)

        ;; explicitly set prefixes to nil as gio/rdf-writer will write
        ;; the grafter default-prefixes otherwise.  By setting to nil,
        ;; use what comes from the stream instead.
        cache-file-writer (gio/rdf-writer stream :format rdf-format :prefixes nil)]

    (reify RDFHandler
      (startRDF [this]
        (try
          (.startRDF cache-file-writer)
          (.startRDF inner-rdf-handler)
          (catch Throwable ex
            (.delete temp-file)
            (throw ex))))
      (endRDF [this]
        (try 
          (.endRDF cache-file-writer)
          (.endRDF inner-rdf-handler)
          (.close stream)
          (move-file-to-cache! cache cache-key temp-file)
          (catch Throwable ex
            (.delete temp-file)
            (throw ex))))
      (handleStatement [this statement]
        (try
          (.handleStatement cache-file-writer statement)
          (.handleStatement inner-rdf-handler statement)
          (catch Throwable ex
            (.delete temp-file)
            (throw ex))))
      (handleComment [this comment]
        (try
          (.handleComment cache-file-writer comment)
          (.handleComment inner-rdf-handler comment)
          (catch Throwable ex
            (.delete temp-file)
            (throw ex))))
      (handleNamespace [this prefix-str uri-str]
        (try 
          (.handleNamespace cache-file-writer prefix-str uri-str)
          (.handleNamespace inner-rdf-handler prefix-str uri-str)
          (catch Throwable ex
            (.delete temp-file)
            (throw ex)))))))
