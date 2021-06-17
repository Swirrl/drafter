(ns drafter-client.client.impl-test
  (:require [clojure.test :as t]
            [clojure.java.io :as io]
            [drafter-client.client.impl :as sut]
            [grafter-2.rdf4j.io :as gio]
            [grafter-2.rdf.protocols :as pr])
  (:import [java.io InputStream]
           [java.util.zip GZIPInputStream]
           [java.net URI]))

(t/deftest format-body-test
  (t/testing "File"
    (let [file (io/file "test/resources/rdf-syntax-ns.ttl")
          {:keys [input worker owned]} (sut/format-body file :ttl nil)]
      (t/is (= file input) "Expected file input")
      (t/is (not owned))))

  (t/testing "File with gzip"
    (let [file (io/file "test/resources/rdf-syntax-ns.ttl")
          input-statements (set (gio/statements file))
          {:keys [input worker owned]} (sut/format-body file :ttl true)]
      (t/is (instance? InputStream input))
      (t/is (not owned))
      (t/is (some? worker))

      (with-open [gs (GZIPInputStream. input)]
        (let [zipped-statements (set (gio/statements gs :format :ttl))]
          @worker
          (t/is (= input-statements zipped-statements))))))

  (t/testing "InputStream"
    (let [file (io/file "test/resources/rdf-syntax-ns.ttl")
          input-statements (set (gio/statements file))]
      (with-open [is (io/input-stream file)]
        (let [{:keys [input worker owned]} (sut/format-body is :ttl nil)]
          (t/is (= is input))
          (t/is (nil? worker))
          (t/is (not owned))

          (let [stmts (set (gio/statements input :format :ttl))]
            (t/is (= input-statements stmts)))))))

  (t/testing "InputStream to gzip"
    (let [file (io/file "test/resources/rdf-syntax-ns.ttl")
          input-statements (set (gio/statements file))]
      (with-open [is (io/input-stream file)]
        (let [{:keys [input worker owned]} (sut/format-body is :ttl true)]
          (t/is (instance? InputStream input))
          (t/is (some? worker))
          (t/is (not owned))

          (let [stmts (set (gio/statements (GZIPInputStream. input) :format :ttl))]
            (t/is (= input-statements stmts)))))))

  (t/testing "Statements"
    (let [g1 (URI. "http://g1")
          g2 (URI. "http://g2")
          quads [(pr/->Quad (URI. "http://s1") (URI. "http://p1") "o1" g1)
                 (pr/->Quad (URI. "http://s2") (URI. "http://p2") "o2" g1)
                 (pr/->Quad (URI. "http://s3") (URI. "http://p3") "o3" g2)]]
      (t/testing "uncompressed"
        (let [{:keys [input worker owned]} (sut/format-body quads "application/n-quads" nil)]
          (t/is (instance? InputStream input))
          (t/is (not owned))
          (t/is (some? worker))

          (let [stmts (set (gio/statements input :format :nq))]
            (t/is (= (set quads) stmts)))))

      (t/testing "gzipped"
        (let [{:keys [input worker owned]} (sut/format-body quads "application/n-quads" true)]
          (t/is (instance? InputStream input))
          (t/is (not owned))
          (t/is (some? worker))

          (with-open [gs (GZIPInputStream. input)]
            (let [stmts (set (gio/statements gs :format :nq))]
              (t/is (= (set quads) (set stmts))))))))))
