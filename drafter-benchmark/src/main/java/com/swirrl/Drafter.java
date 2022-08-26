package com.swirrl;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;

import java.io.File;
import java.net.URI;

/**
 * Wrapper class around internal drafter operations.
 */
public class Drafter {
    private final SPARQLRepository repo;
    private final Object manager;

    private Drafter(SPARQLRepository repo, Object  manager) {
        this.repo = repo;
        this.manager = manager;
    }

    /**
     * Creates a new draftset for the given user
     * @param user The owner for the new draftset
     * @return The created {@link Draftset}
     */
    public Draftset createDraft(User user) {
        Util.require("drafter.backend.draftset.operations");
        IFn createFn = Clojure.var("drafter.backend.draftset.operations", "create-draftset!");

        return new Draftset(createFn.invoke(this.repo, user.obj()));
    }

    private Object jobContext(Draftset draftset) {
        Util.require("drafter.feature.draftset-data.common");
        IFn contextFn = Clojure.var("drafter.feature.draftset-data.common", "job-context");
        return contextFn.invoke(manager, draftset.obj());
    }

    private static Object getAppendStateMachine() {
        Util.require("drafter.feature.draftset-data.append");
        IFn smFn = Clojure.var("drafter.feature.draftset-data.append", "append-state-machine");
        return smFn.invoke();
    }

    /**
     * Appends the data from {@param dataFile} into the draft represented by {@param draftset}
     * @param draftset The draft to append to
     * @param dataFile The quads data file to append. The serialisation format must be inferable from the extension.
     */
    public void append(Draftset draftset, File dataFile) {
        Object sm = getAppendStateMachine();
        execStateMachine(sm, draftset, dataFile);
    }

    private static Object getDeleteStateMachine() {
        Util.require("drafter.feature.draftset-data.delete");
        return Clojure.var("drafter.feature.draftset-data.delete", "delete-state-machine").invoke();
    }

    /**
     * Synchronously executes the given state machine within a draftset
     * @param stateMachine The state machine to execute
     * @param draftset The draftset the operation is executed within
     * @param dataFile The input file to process for the operation
     */
    private void execStateMachine(Object stateMachine, Draftset draftset, File dataFile) {
        Object liveToDraftMapping = draftset.getGraphMapping(this.repo);
        Object source = Util.getInputSource(dataFile);
        Object context = this.jobContext(draftset);

        Util.require("drafter.feature.draftset-data.common");
        IFn execFn = Clojure.var("drafter.feature.draftset-data.common", "exec-state-machine-sync");
        execFn.invoke(stateMachine, liveToDraftMapping, source, context);
    }

    /**
     * Deletes the contents of a data file from a draftset
     * @param draftset The draftset to delete from
     * @param toDelete File containing the data to delete
     */
    public void delete(Draftset draftset, File toDelete) {
        Object sm = getDeleteStateMachine();
        execStateMachine(sm, draftset, toDelete);
    }

    private Object getGraphManager() {
        Util.require("drafter.backend.draftset.graphs");
        return Clojure.var("drafter.backend.draftset.graphs", "create-manager").invoke(this.repo);
    }

    /**
     * Deletes a graph from a draftset
     * @param draftset The draftset to delete from
     * @param graphToDelete URI of the draftset graph to delete
     */
    public void deleteGraph(Draftset draftset, URI graphToDelete) {
        Util.require("drafter.backend.draftset.graphs");
        Clojure.var("drafter.backend.draftset.graphs", "delete-user-graph").invoke(this.getGraphManager(), draftset.obj(), graphToDelete);
    }

    /**
     * Publishes the given draftset to live
     * @param draftset The draftset to publish
     */
    public void publish(Draftset draftset) {
        Util.require("drafter.backend.draftset.operations.publish");
        Clojure.var("drafter.backend.draftset.operations.publish", "publish-draftset!").invoke(this.manager, draftset.obj());
    }

    private static Long MAX_UPDATE_SIZE = (long)5000;

    /**
     * Executes a SPARQL UPDATE query string within a draftset
     * @param draftset The draftset to execute the UPDATE in
     * @param query Contents of the query to execute
     */
    public void submitUpdate(Draftset draftset, String query) {
        Util.require("drafter.feature.draftset.update");
        Object parsedQuery = Clojure.var("drafter.feature.draftset.update", "parse-update-query").invoke(this.manager, query, MAX_UPDATE_SIZE);

        Clojure.var("drafter.feature.draftset.update", "update!").invoke(this.manager, MAX_UPDATE_SIZE, draftset.obj(), parsedQuery);
    }

    /**
     * Drops the contents of the backend database used by this drafter instance
     */
    public void dropDb() {
        try(RepositoryConnection conn = this.repo.getConnection()) {
            conn.prepareUpdate(QueryLanguage.SPARQL, "DROP ALL").execute();
        }
    }

    /**
     * Creates a new instance using the test database
     * @return
     */
    public static Drafter create() {
        return create(Util.getRepository());
    }

    /**
     * Creates a new instance which uses the given SPARQL repository
     * @param repo The repository to use
     * @return A new instance using the given repository
     */
    public static Drafter create(SPARQLRepository repo) {
        Util.require("drafter.manager");
        IFn createFn = Clojure.var("drafter.manager", "create-manager");
        return new Drafter(repo, createFn.invoke(repo));
    }
}
