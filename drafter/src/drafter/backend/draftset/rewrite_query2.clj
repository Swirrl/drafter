(ns drafter.backend.draftset.rewrite-query2
  "Demonstrate how to use JENA's Query API to rewrite a SPARQL query.

  NOTE: this is an incomplete implementation, and only targets a small
  part of the AST."
  (:import org.apache.jena.graph.NodeFactory
           [org.apache.jena.query Query QueryFactory Syntax QueryVisitor]
           [org.apache.jena.sparql.syntax ElementVisitor ElementTriplesBlock

            ElementAssign ElementBind ElementData ElementDataset ElementExists ElementFilter ElementGroup ElementMinus ElementNamedGraph ElementNotExists ElementOptional ElementPathBlock ElementService ElementSubQuery ElementTriplesBlock ElementUnion ]
           [org.apache.jena.sparql.engine.binding BindingFactory]
           [org.apache.jena.sparql.util NodeUtils]
           [org.apache.jena.graph Node_URI]))


(declare build-rewrite-visitor)

(defn build-element-rewriter [live->draft]
  (reify ElementVisitor

    (^void visit [t ^ElementBind el]
     #_(println "bind" el))

    (^void visit [t ^ElementSubQuery el]
     #_(println "elementsubquery" el)
     (let [qv (build-rewrite-visitor live->draft)
           subq (.getQuery el)]
       (when subq
         (.visit subq qv))))

    (^void visit [t ^ElementTriplesBlock el]
     (println "triples block" el))

    (^void visit [t ^ElementUnion el]
     #_(println "elementunion" el))

    ;; ElementData corresponds to a VALUES clause block
    (^void visit [t ^ElementData el]
     (println "eldata" (.getRows el))
     ;; collect new-bindings in mutable array
     (let [new-bindings (java.util.ArrayList.)]
       (doseq [var (.getVars el)]
         (let [bindings (.getRows el)
               li (.listIterator bindings)]
           (if (.hasNext li)
             (loop [row (.next li)]
               (let [val (.get row var)]
                 (println "looping " row)
                 #_(sc.api/spy)
                 (when-let [draft-uri (and (instance? Node_URI val)
                                           (NodeUtils/asNode (live->draft (str val))))]
                   (println "updating" val "with " draft-uri)

                   ;; remove old URI binding via list iterator
                   (.remove li)
                   ;; collect the newly bound value to apply
                   (.add new-bindings (BindingFactory/binding var draft-uri))))
               (when (.hasNext li)
                 (recur (.next li)))))))

       ;; Add collected "changed" bindings back into ElementData
       (doseq [b new-bindings]
         (.add el b))))

    (^void visit [t ^ElementGroup el]
     #_(println "elgroup " el "haselements " (.getElements el))
     (reduce (fn [acc e]
               ;(println "visiting " e)
               (.visit acc e)
               acc)
             t
             (.getElements el)))

    (^void visit [t ^ElementDataset el]
     #_(println "elementdataset" el))

    (^void visit [t ^ElementExists el]
     #_(println "elementexists" el))

    (^void visit [t ^ElementFilter el]
     #_(println "elementfilter" el))


    (^void visit [t ^ElementMinus el]
     #_(println "elementminus"))


    (^void visit [t ^ElementNamedGraph el]
     #_(println "elementnamedgraph"
              "graph name" (.getGraphNameNode el)
              "element: "(.getElement el)))

    (^void visit [t ^ElementNotExists el]
     #_(println "elementnotexists" el))

    (^void visit [t ^ElementOptional el]
     #_(println "elementoptional" el))

    (^void visit [t ^ElementPathBlock el]
     #_(println "elementpathblock" el ))

    (^void visit [t ^ElementService el]
     #_(println "elementservice" el))


    (^void visit [t ^ElementAssign el]
     #_(println "assign" el))

    ))

(defn build-rewrite-visitor [live->draft]
  (let [el-rewrite (build-element-rewriter live->draft)]
    (reify QueryVisitor
      (finishVisit [this q])
      (startVisit [this q])
      (visitAskResultForm [this q])
      (visitConstructResultForm [this q])
      (visitDatasetDecl [this q])
      (visitDescribeResultForm [this q])
      (visitGroupBy [this q])
      (visitHaving [this q])
      (visitJsonResultForm [this q])
      (visitLimit [this q])
      (visitOffset [this q]
        )
      (visitOrderBy [this q])
      (visitPrologue [this q]
        )
      (visitQueryPattern [this q]
        (when-let [qp (.getQueryPattern q)]
          (.visit el-rewrite qp)))
      (visitResultForm [this q])
      (visitSelectResultForm [this q])
      (visitValues [this q])
      )))

(defn rewrite [qstr live->draft]
  (let [q (QueryFactory/create qstr Syntax/syntaxSPARQL_11)
        rewriter (build-rewrite-visitor live->draft)]
    (.visit q rewriter)
    q))


(comment

  ;; query that demonstrates the issue
  ;; https://github.com/Swirrl/drafter/issues/346 we want to solve
  (def broken-qstr1 "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

CONSTRUCT {
  ?resource_type <http://publishmydata.com/muttnik/count> ?count .
  ?resource_type rdfs:label ?resource_type_label .
}
WHERE {
  {
    # In current drafter this inner select is lost due to rewriting via SSEs and
    # then patching things back into a Query

    SELECT ?resource_type ?resource_type_label (COUNT(DISTINCT(?ds_contents)) AS ?count) {
      VALUES ?ds_graph { <http://muttnik.gov/graph/animals> }

      GRAPH ?ds_graph {
        ?ds_contents a ?resource_type .
      }

      OPTIONAL {
        ?resource_type rdfs:label ?resource_type_label .
      }
    }
    GROUP BY ?resource_type ?resource_type_label
  }

}
")

  (println (str (rewrite broken-qstr1 {"http://muttnik.gov/graph/animals" "http://muttnik.gov/graph/animals/replaced"})))
  ;; =>

  ;; PREFIX  rdfs: <http://www.w3.org/2000/01/rdf-schema#>
  ;;
  ;; CONSTRUCT
  ;;   {
  ;;     ?resource_type <http://publishmydata.com/muttnik/count> ?count .
  ;;     ?resource_type rdfs:label ?resource_type_label .
  ;;   }
  ;; WHERE
  ;;   { { SELECT  ?resource_type ?resource_type_label (COUNT(DISTINCT ?ds_contents) AS ?count)
  ;;       WHERE
  ;;         # NOTE binding in values clause is rewritten.
  ;;         { VALUES ?ds_graph { <http://muttnik.gov/graph/animals/replaced> }
  ;;           GRAPH ?ds_graph
  ;;             { ?ds_contents  a                 ?resource_type }
  ;;           OPTIONAL
  ;;             { ?resource_type
  ;;                         rdfs:label  ?resource_type_label
  ;;             }
  ;;         }
  ;;       GROUP BY ?resource_type ?resource_type_label
  ;;     }
  ;;   }



  )
