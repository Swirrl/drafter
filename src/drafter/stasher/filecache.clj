(ns drafter.stasher.filecache
  (:require [clojure.core.cache :as cache]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [grafter.rdf4j.formats :as fmt]
            [grafter.rdf4j.io :as gio]
            [integrant.core :as ig]
            [me.raynes.fs :as fs]
            [drafter.stasher.cache-key :as ck])
  (:import java.io.File
           java.security.MessageDigest
           org.apache.commons.codec.binary.Hex
           (org.eclipse.rdf4j.query GraphQueryResult TupleQueryResultHandler TupleQueryResult)
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
  (let [parser (when-let [parser-factory (.orElse (condp instance? fmt
                                                    RDFFormat (.get (org.eclipse.rdf4j.rio.RDFParserRegistry/getInstance) fmt)
                                                    BooleanQueryResultFormat (.get (BooleanQueryResultParserRegistry/getInstance) fmt)
                                                    TupleQueryResultFormat (.get (TupleQueryResultParserRegistry/getInstance) fmt)
                                                    (java.util.Optional/empty)) nil)]
                 (.getParser parser-factory))]
    (assert parser (str "Error could not find a parser for format:" fmt))
    parser))

(def default-cache-rdf-format :brf)
(def default-cache-tuple-format :brt #_:srj)
(def default-cache-boolean-format :txt)

(def fmt-kw->rdf-format (build-format-keyword->format-map rdf-formats))
(def fmt-kw->tuple-format (build-format-keyword->format-map tuple-formats))
(def fmt-kw->boolean-format (build-format-keyword->format-map boolean-formats))


(defn backend-rdf-format [cache]
  (:backend-rdf-format (.opts cache)))

(defn backend-tuple-format [cache]
  (:backend-tuple-format (.opts cache)))

(defn backend-boolean-format [cache]
  (:backend-boolean-format (.opts cache)))


(defmulti backend-format (fn [cache cache-key]
                           (:query-type cache-key)))

(defmethod backend-format :tuple [cache cache-key]
  (backend-tuple-format cache))

(defmethod backend-format :graph [cache cache-key]
  (backend-rdf-format cache))

(defmethod backend-format :boolean [cache cache-key]
  (backend-boolean-format cache))

(defn- cache-key->hash-key
  "Cache keys at the interface to the cache are maps, but we need to
  repeatably hash these to an MD5 sum for the filename/location on
  disk.  This function takes a cache-key (map) and converts it into an
  MD5 sum."
  [cache-key]
  (let [cache-key-str (pr-str (ck/static-component cache-key))
        md (MessageDigest/getInstance "MD5")]
    (Hex/encodeHexString (.digest md (.getBytes cache-key-str)))))

(defn- hash-key->file-path
  "Returns a relative path styled variant of the hash key.
   e.g. given a hash-key of \"0cc23d1cefa62b120c5f3b289503c01e\", convert
  it into the vector [\"0c\" \"c2\" \"0cc23d1cefa62b120c5f3b289503c01e\"]."
  [hash-key]
  (let [sub-dirs (->> hash-key
                      (partition 2)
                      (take 2)
                      (mapv (partial apply str)))]
    (conj sub-dirs hash-key)))

(defn assert-spec! [spec value]
  (when-not (s/valid? spec value)
    (throw (ex-info (str "Attempt to put an invalid entry in the stasher query cache.  "
                         (s/explain-str spec value))
                    (s/explain-data spec value)))))

(defn- cache-key->file-name [cache-key fmt]
  (format "%s.%s" (ck/time-component cache-key) (name fmt)))

(defn cache-key->cache-path
  [cache cache-key]
  (assert-spec! ::ck/cache-key cache-key)
  (let [hash (cache-key->hash-key cache-key)
        dirs (hash-key->file-path hash)
        fmt (backend-format cache cache-key)
        filename (cache-key->file-name cache-key fmt)]
    (apply io/file (concat [(:dir (.opts cache))]
                           (conj dirs filename)))))

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
  (let [default-opts {:base-cache {}
                      :dir default-cache-dir
                      :backend-rdf-format default-cache-rdf-format
                      :backend-tuple-format default-cache-tuple-format
                      :backend-boolean-format default-cache-boolean-format}
        
        {:keys [dir base-cache] :as opts} (merge default-opts opts)]
    (fs/mkdir dir)
    (fs/mkdir (io/file dir "tmp"))
    (FileCache. base-cache opts)))

(defmethod ig/init-key :drafter.stasher/filecache [_ opts]
  (file-cache-factory opts))

(defmethod ig/halt-key! :drafter.stasher/filecache [k cache]
  (when-not (:persist-on-shutdown? (.opts cache) true)
    (log/info "Clearing" k)
    (fs/delete-dir (:dir (.opts cache)))))


;; TODO can probably kill this now both sides of conditional are
;; always an io/output-stream
(defn select-output-coercer [fmt]
  (let [fmt (or (fmt-kw->rdf-format fmt) (fmt-kw->tuple-format fmt))]
    (if (#{TupleQueryResultFormat/BINARY RDFFormat/BINARY} fmt)
      io/output-stream
      #_io/writer
      io/output-stream)))

(defn- ^java.io.File create-temp-file!
  "Create and return a temp file inside the cache :dir.  Takes also a
  keyword indicating the file format."
  [cache format-kw]
  (let [tmp-dir (io/file (:dir (.opts cache)) "tmp")]
    (fs/mkdir tmp-dir)
    (File/createTempFile "stasher" (str "tmp." (name format-kw)) tmp-dir)))

(defn stashing-graph-query-result
  "Wrap a BackgroundGraphResult with one that will write the stream of
  RDF into a temp file and move the file into the cache when it's
  finished.

  For RDF pull query results."
  [cache cache-key ^BackgroundGraphResult bg-graph-result]
  (let [rdf-format (backend-rdf-format cache)
        temp-file (create-temp-file! cache rdf-format)
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

  mode - must be either :sync or :async depending on which .evaluate
         method arity is used.

  For RDF pull query results."
  [mode cache cache-key ^TupleQueryResultHandler bg-tuple-result]
  {:pre [(#{:sync :async} mode)]}
  (let [tuple-format (backend-tuple-format cache)
        temp-file (create-temp-file! cache tuple-format)
        make-stream (select-output-coercer tuple-format)
        stream (make-stream temp-file :buffer 8192)

        cache-file-writer ^TupleQueryResultWriter (tuple-writer stream tuple-format)
        bindings (.getBindingNames bg-tuple-result)]

    ;; copy the projected variables/column names across to the
    ;; cached file.
    (when (= mode :sync)
      (.startQueryResult cache-file-writer bindings))


    ;; TODO TODO TODO
    ;; figure out why this stuff ain't working with the tests:
    ;;
    ;;  [[file:~/repos/drafter/test/drafter/stasher_test.clj::(t/testing%20"SELECT"][broken test]]
    (reify

      ;; Push interface...
      TupleQueryResultHandler
      (endQueryResult [this]
        (.endQueryResult bg-tuple-result) 
        (.close this))

      (handleLinks [this links]
        ;; pretty sure this is really a no/op
        (.handleLinks cache-file-writer links)
        (.handleLinks bg-tuple-result links))

      (handleBoolean [this bool]
        ;; pretty sure this is really a no/op
        (.handleBoolean cache-file-writer bool)
        (.handleBoolean bg-tuple-result bool))

      (handleSolution [this binding-set]
        (.handleSolution cache-file-writer binding-set)
        (.handleSolution bg-tuple-result binding-set))

      (startQueryResult [this binding-names]
        (.startQueryResult cache-file-writer binding-names)
        (.startQueryResult bg-tuple-result binding-names))

      ;; pull interface
      TupleQueryResult
      (getBindingNames [this]
        (.getBindingNames bg-tuple-result))
      
      (close [this]
        (try
          (.close bg-tuple-result)
          (.endQueryResult cache-file-writer)
          (move-file-to-cache! cache cache-key temp-file)
          
          (catch Throwable ex
            (log/warn ex "Error whilst closing stream.  Cleaning up stashed tempfile:" temp-file)
            (.delete temp-file)
            (throw ex))))

      (hasNext [this]
        (try
          (.hasNext ^TupleQueryResult bg-tuple-result)
          (catch Throwable ex
            (.delete temp-file)
            (throw ex))))
      
      (next [this]
        (try
          (let [solution (.next ^TupleQueryResult bg-tuple-result)]
            (.handleSolution cache-file-writer solution)
            solution)
          (catch Throwable ex
            (.delete temp-file)
            (throw ex)))))))

(defn stashing-boolean-query-result [cache cache-key boolean-result]
  (let [boolean-format (backend-boolean-format cache)
        temp-file (create-temp-file! cache boolean-format)]
    
    (with-open [output-stream (io/output-stream temp-file)]
      (let [bool-writer (QueryResultIO/createBooleanWriter (fmt-kw->boolean-format boolean-format) output-stream)]
        ;; Write the result to the file
        (.handleBoolean bool-writer boolean-result)))

    (move-file-to-cache! cache cache-key temp-file)
    
    ;; return the actual result this will get returned to the
    ;; outer-most call to query.  NOTE that boolean's are different to
    ;; other query types as they don't have a background-evaluator.
    boolean-result))

(defn stashing-rdf-handler
  "Wrap an RDFHandler with one that will write the stream of RDF into
  a temp file and move the file into the cache when it's finished.

  For RDF push query results."
  [cache cache-key ^RDFHandler inner-rdf-handler]
  (let [rdf-format (backend-rdf-format cache)
        temp-file (create-temp-file! cache rdf-format)
        make-stream (select-output-coercer rdf-format)
        stream ^java.io.OutputStream (make-stream temp-file :buffer 8192)

        ;; explicitly set prefixes to nil as gio/rdf-writer will write
        ;; the grafter default-prefixes otherwise.  By setting to nil,
        ;; use what comes from the stream instead.
        cache-file-writer ^RDFWriter (gio/rdf-writer stream :format rdf-format :prefixes nil)]

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
