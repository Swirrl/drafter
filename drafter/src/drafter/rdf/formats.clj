(ns drafter.rdf.formats
  (:import [org.eclipse.rdf4j.rio RDFFormat]
           [java.nio.charset Charset]))

(def ^:private ascii (Charset/forName "US-ASCII"))
(def csv-rdf-format (RDFFormat. "CSV" "text/csv" ascii "csv" false true))
(def tsv-rdf-format (RDFFormat. "TSV" "text/tab-separated-values" nil "tsv" false true))

