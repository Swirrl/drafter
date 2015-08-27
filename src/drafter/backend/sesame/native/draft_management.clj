(ns drafter.backend.sesame.native.draft-management
  (:require [clojure.tools.logging :as log]
            [drafter.rdf.draft-management :as mgmt]
            [grafter.rdf.repository :as repo]
            [grafter.rdf :refer [add]]))

(defn- migrate-graph-to-live!
  "Moves the triples from the draft graph to the draft graphs live destination."
  [db draft-graph-uri]

  (if-let [live-graph-uri (mgmt/lookup-live-graph db draft-graph-uri)]
    (do
      ;;DELETE the target (live) graph and copy the draft to it
      ;;TODO: Use MOVE query?
      (mgmt/delete-graph-contents! db live-graph-uri)

      (let [contents (repo/query db
                            (str "CONSTRUCT { ?s ?p ?o } WHERE
                                 { GRAPH <" draft-graph-uri "> { ?s ?p ?o } }"))

            ;;If the source (draft) graph is empty then the migration
            ;;is a deletion. If it is the only draft of the live graph
            ;;then all references to the live graph are being removed
            ;;from the data. In this case the reference to the live
            ;;graph should be removed from the state graph. Note this
            ;;case and use it when cleaning up the state graph below.
            is-only-draft? (not (mgmt/has-more-than-one-draft? db live-graph-uri))]

        ;;if the source (draft) graph has data then copy it to the live graph and
        ;;make it public.
        (if (not (empty? contents))
          (do
            (add db live-graph-uri contents)
            (mgmt/set-isPublic! db live-graph-uri true)))

        ;;delete draft data
        (mgmt/delete-graph-contents! db draft-graph-uri)

        ;;NOTE: At this point all the live and draft graph data has
        ;;been updated: the live graph contents match those of the
        ;;published draft, and the draft data has been deleted.

        ;;Clean up the state graph - all references to the draft graph should always be removed.
        ;;The live graph should be removed if the draft was empty (operation was a deletion) and
        ;;it was the only draft of the live graph

        ;;WARNING: Draft graph state must be deleted before the live graph state!
        ;;DELETE query depends on the existence of the live->draft connection in the state
        ;;graph
        (mgmt/delete-draft-graph-state! db draft-graph-uri)

        (if (and is-only-draft? (empty? contents))
          (mgmt/delete-live-graph-from-state! db live-graph-uri)))
      
      (log/info (str "Migrated graph: " draft-graph-uri " to live graph: " live-graph-uri)))

    (throw (ex-info (str "Could not find the live graph associated with graph " draft-graph-uri)
                    {:error :graph-not-found}))))

(defn migrate-graphs-to-live! [backend graphs]
  (log/info "Starting make-live for graphs " graphs)
  (with-open [conn (repo/->connection (:repo backend))]
    (repo/with-transaction conn
      (doseq [g graphs]
        (migrate-graph-to-live! conn g))))
  (log/info "Make-live for graphs " graphs " done"))
