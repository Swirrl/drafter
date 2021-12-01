(ns drafter-client.client.impl-test
  (:require [clojure.test :as t]
            [clojure.java.io :as io]
            [drafter-client.client.impl :as sut]
            [grafter-2.rdf4j.io :as gio]
            [grafter-2.rdf.protocols :as pr])
  (:import [java.io InputStream ByteArrayInputStream]
           [java.util.zip GZIPInputStream]
           [java.net URI]
           [org.eclipse.rdf4j.rio RDFFormat]))

(t/deftest rdf-input-request-headers-test
  (t/testing "No compression"
    (t/is (= {:Content-Type "application/trig"} (sut/rdf-input-request-headers {:format RDFFormat/TRIG :gzip :none}))))

  (t/testing "Compression applied"
    (t/is (= {:Content-Type "application/n-triples"
              :Content-Encoding "gzip"}
             (sut/rdf-input-request-headers {:format RDFFormat/NTRIPLES :gzip :apply}))))

  (t/testing "Input compressed"
    (t/is (= {:Content-Type "text/turtle"
              :Content-Encoding "gzip"}
             (sut/rdf-input-request-headers {:format RDFFormat/TURTLE :gzip :applied})))))

(t/deftest rdf-request-properties-test
  (t/testing "File"
    (t/testing "unknown file type and no options"
      (let [properties (sut/rdf-request-properties (io/file "file.unknown") {})]
        (t/is (= {:format RDFFormat/NQUADS :gzip :none} properties))))

    (t/testing "unknown file type and graph option"
      (let [properties (sut/rdf-request-properties (io/file "file.unknown") {:graph (URI. "http://g")})]
        (t/is (= {:format RDFFormat/NTRIPLES :gzip :none} properties))))

    (t/testing "unknown compressed file and no options"
      (let [properties (sut/rdf-request-properties (io/file "file.unknown.gz") {})]
        (t/is (= {:format RDFFormat/NQUADS :gzip :applied} properties))))

    (t/testing "unknown compressed file and graph option"
      (let [properties (sut/rdf-request-properties (io/file "file.unknown.gz") {:graph (URI. "http://g")})]
        (t/is (= {:format RDFFormat/NTRIPLES :gzip :applied} properties))))

    (t/testing "known uncompressed file type and no options"
      (let [properties (sut/rdf-request-properties (io/file "file.ttl") {})]
        (t/is (= {:format RDFFormat/TURTLE :gzip :none} properties))))

    (t/testing "known compressed file type and no options"
      (let [properties (sut/rdf-request-properties (io/file "file.trig.gz") {})]
        (t/is (= {:format RDFFormat/TRIG :gzip :applied} properties))))

    (t/testing "known uncompressed file with legacy gzip"
      (let [properties (sut/rdf-request-properties (io/file "file.nq") {:gzip true})]
        (t/is (= {:format RDFFormat/NQUADS :gzip :apply} properties))))

    (t/testing "known uncompressed file with gzip"
      (let [properties (sut/rdf-request-properties (io/file "file.nq") {:gzip :apply})]
        (t/is (= {:format RDFFormat/NQUADS :gzip :apply} properties))))

    (t/testing "unknown compressed file with gzip"
      (let [properties (sut/rdf-request-properties (io/file "file.unknown.gz") {:gzip :apply})]
        (t/is (= {:format RDFFormat/NQUADS :gzip :apply} properties))))

    (t/testing "unknown uncompressed file with gzip applied"
      (let [properties (sut/rdf-request-properties (io/file "file.unknown") {:gzip :applied})]
        (t/is (= {:format RDFFormat/NQUADS :gzip :applied} properties))))

    (t/testing "known uncompressed file with gzip applied"
      (let [properties (sut/rdf-request-properties (io/file "file.ttl") {:gzip :applied})]
        (t/is (= {:format RDFFormat/TURTLE :gzip :applied} properties))))

    (t/testing "unknown compressed file with gzip applied"
      (let [properties (sut/rdf-request-properties (io/file "file.unknown.gz") {:gzip :applied})]
        (t/is (= {:format RDFFormat/NQUADS :gzip :applied} properties))))

    (t/testing "known compressed file with gzip applied"
      (let [properties (sut/rdf-request-properties (io/file "file.nt.gz") {:gzip :applied})]
        (t/is (= {:format RDFFormat/NTRIPLES :gzip :applied} properties))))

    (t/testing "unknown uncompressed file with no gzip"
      (let [properties (sut/rdf-request-properties (io/file "file.unknown") {:gzip :none})]
        (t/is (= {:format RDFFormat/NQUADS :gzip :none} properties))))

    (t/testing "unknown compressed file with no gzip"
      (let [properties (sut/rdf-request-properties (io/file "file.unknown.gz") {:gzip :none})]
        (t/is (= {:format RDFFormat/NQUADS :gzip :none} properties))))

    (t/testing "known uncompressed file with no gzip"
      (let [properties (sut/rdf-request-properties (io/file "file.trig") {:gzip :none})]
        (t/is (= {:format RDFFormat/TRIG :gzip :none} properties))))

    (t/testing "known compressed file with no gzip"
      (let [properties (sut/rdf-request-properties (io/file "file.nq.gz") {:gzip :none})]
        (t/is (= {:format RDFFormat/NQUADS :gzip :none} properties))))

    (t/testing "unknown file type with format"
      (let [properties (sut/rdf-request-properties (io/file "file.unknown") {:format :trig})]
        (t/is (= {:format RDFFormat/TRIG :gzip :none} properties))))

    (t/testing "unknown compressed file type with format"
      (let [properties (sut/rdf-request-properties (io/file "file.unknown.gz") {:format :nq})]
        (t/is (= {:format RDFFormat/NQUADS :gzip :applied} properties))))

    (t/testing "known file type with format"
      (let [properties (sut/rdf-request-properties (io/file "file.nq") {:format :rj})]
        (t/is (= {:format RDFFormat/RDFJSON :gzip :none} properties))))

    (t/testing "known compressed file type with format"
      (let [properties (sut/rdf-request-properties (io/file "file.trig.gz") {:format "application/n-quads"})]
        (t/is (= {:format RDFFormat/NQUADS :gzip :applied} properties)))))

  (t/testing "InputStream"
    (let [input-stream (ByteArrayInputStream. (.getBytes "test"))]
      (t/testing "no options"
        (let [properties (sut/rdf-request-properties input-stream {})]
          (t/is (= {:format RDFFormat/NQUADS :gzip :none} properties))))

      (t/testing "with graph"
        (let [properties (sut/rdf-request-properties input-stream {:graph (URI. "http://s")})]
          (t/is (= {:format RDFFormat/NTRIPLES :gzip :none} properties))))

      (t/testing "with legacy gzip"
        (let [properties (sut/rdf-request-properties input-stream {:gzip true})]
          (t/is (= {:format RDFFormat/NQUADS :gzip :apply} properties))))

      (t/testing "with gzip"
        (let [properties (sut/rdf-request-properties input-stream {:gzip :apply})]
          (t/is (= {:format RDFFormat/NQUADS :gzip :apply} properties))))

      (t/testing "with gzip applied"
        (let [properties (sut/rdf-request-properties input-stream {:gzip :applied})]
          (t/is (= {:format RDFFormat/NQUADS :gzip :applied} properties))))

      (t/testing "with no gzip"
        (let [properties (sut/rdf-request-properties input-stream {:gzip :applied})]
          (t/is (= {:format RDFFormat/NQUADS :gzip :applied} properties))))

      (t/testing "with format"
        (let [properties (sut/rdf-request-properties input-stream {:format :trig})]
          (t/is (= {:format RDFFormat/TRIG :gzip :none} properties))))))

  (t/testing "Quads"
    (let [quads [(pr/->Quad (URI. "http://s") (URI. "http://p") "o" (URI. "http://g"))]]
      (t/testing "none"
        (let [properties (sut/rdf-request-properties quads {})]
          (t/is (= {:format RDFFormat/NQUADS :gzip :none} properties))))

      (t/testing "legacy gzip"
        (let [properties (sut/rdf-request-properties quads {:gzip true})]
          (t/is (= {:format RDFFormat/NQUADS :gzip :apply} properties))))

      (t/testing "gzip"
        (let [properties (sut/rdf-request-properties quads {:gzip :apply})]
          (t/is (= {:format RDFFormat/NQUADS :gzip :apply} properties))))

      (t/testing "gzip applied"
        (let [properties (sut/rdf-request-properties quads {:gzip :applied})]
          (t/is (= {:format RDFFormat/NQUADS :gzip :applied} properties))))

      (t/testing "no gzip"
        (let [properties (sut/rdf-request-properties quads {:gzip :none})]
          (t/is (= {:format RDFFormat/NQUADS :gzip :none} properties))))

      (t/testing "format"
        (let [properties (sut/rdf-request-properties quads {:format RDFFormat/TRIG})]
          (t/is (= {:format RDFFormat/TRIG :gzip :none} properties))))))

  (t/testing "Triples"
    (let [triples [(pr/->Triple (URI. "http://s") (URI. "http://p") "o")]
          graph (URI. "http://g")]
      (t/testing "graph only"
        (let [properties (sut/rdf-request-properties triples {:graph graph})]
          (t/is (= {:format RDFFormat/NTRIPLES :gzip :none} properties))))

      (t/testing "legacy gzip"
        (let [properties (sut/rdf-request-properties triples {:graph graph :gzip true})]
          (t/is (= {:format RDFFormat/NTRIPLES :gzip :apply} properties))))

      (t/testing "gzip"
        (let [properties (sut/rdf-request-properties triples {:graph graph :gzip :apply})]
          (t/is (= {:format RDFFormat/NTRIPLES :gzip :apply} properties))))

      (t/testing "gzip applied"
        (let [properties (sut/rdf-request-properties triples {:graph graph :gzip :apply})]
          (t/is (= {:format RDFFormat/NTRIPLES :gzip :apply} properties))))

      (t/testing "no gzip"
        (let [properties (sut/rdf-request-properties triples {:graph graph :gzip :apply})]
          (t/is (= {:format RDFFormat/NTRIPLES :gzip :apply} properties))))

      (t/testing "format"
        (let [properties (sut/rdf-request-properties triples {:graph graph :format RDFFormat/TURTLE})]
          (t/is (= {:format RDFFormat/TURTLE :gzip :none} properties)))))))

(t/deftest format-body-test
  (t/testing "File"
    (let [file (io/file "test/resources/rdf-syntax-ns.ttl")
          {:keys [input worker]} (sut/format-body file RDFFormat/TURTLE false)]
      (t/is (= file input) "Expected file input")
      (t/is (nil? worker))))

  (t/testing "File with gzip"
    (let [file (io/file "test/resources/rdf-syntax-ns.ttl")
          input-statements (set (gio/statements file))
          {:keys [input worker]} (sut/format-body file RDFFormat/TURTLE true)]
      (t/is (instance? InputStream input))
      (t/is (some? worker))

      (with-open [gs (GZIPInputStream. input)]
        (let [zipped-statements (set (gio/statements gs :format :ttl))]
          @worker
          (t/is (= input-statements zipped-statements))))))

  (t/testing "InputStream"
    (let [file (io/file "test/resources/rdf-syntax-ns.ttl")
          input-statements (set (gio/statements file))]
      (with-open [is (io/input-stream file)]
        (let [{:keys [input worker]} (sut/format-body is RDFFormat/TURTLE false)]
          (t/is (= is input))
          (t/is (nil? worker))

          (let [stmts (set (gio/statements input :format :ttl))]
            (t/is (= input-statements stmts)))))))

  (t/testing "InputStream to gzip"
    (let [file (io/file "test/resources/rdf-syntax-ns.ttl")
          input-statements (set (gio/statements file))]
      (with-open [is (io/input-stream file)]
        (let [{:keys [input worker]} (sut/format-body is RDFFormat/TURTLE true)]
          (t/is (instance? InputStream input))
          (t/is (some? worker))

          (let [stmts (set (gio/statements (GZIPInputStream. input) :format :ttl))]
            (t/is (= input-statements stmts)))))))

  (t/testing "Statements"
    (let [g1 (URI. "http://g1")
          g2 (URI. "http://g2")
          quads [(pr/->Quad (URI. "http://s1") (URI. "http://p1") "o1" g1)
                 (pr/->Quad (URI. "http://s2") (URI. "http://p2") "o2" g1)
                 (pr/->Quad (URI. "http://s3") (URI. "http://p3") "o3" g2)]]
      (t/testing "uncompressed"
        (let [{:keys [input worker]} (sut/format-body quads RDFFormat/NQUADS false)]
          (t/is (instance? InputStream input))
          (t/is (some? worker))

          (let [stmts (set (gio/statements input :format :nq))]
            (t/is (= (set quads) stmts)))))

      (t/testing "gzipped"
        (let [{:keys [input worker]} (sut/format-body quads RDFFormat/NQUADS true)]
          (t/is (instance? InputStream input))
          (t/is (some? worker))

          (with-open [gs (GZIPInputStream. input)]
            (let [stmts (set (gio/statements gs :format :nq))]
              (t/is (= (set quads) (set stmts))))))))))
