package com.swirrl;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;

import java.io.File;
import java.net.URI;

public class Drafter {
    private final SPARQLRepository repo;
    private final Object manager;

    private Drafter(SPARQLRepository repo, Object  manager) {
        this.repo = repo;
        this.manager = manager;
    }

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

    public void append(Draftset draftset, URI graph, File dataFile) {
        Object sm = getAppendStateMachine();
        execStateMachine(sm, draftset, graph, dataFile);
    }

    private static Object getDeleteStateMachine() {
        Util.require("drafter.feature.draftset-data.delete");
        return Clojure.var("drafter.feature.draftset-data.delete", "delete-state-machine").invoke();
    }

    private void execStateMachine(Object stateMachine, Draftset draftset, URI graph, File dataFile) {
        Object liveToDraftMapping = draftset.getGraphMapping(this.repo);
        Object source = Util.getInputSource(graph, dataFile);
        Object context = this.jobContext(draftset);

        Util.require("drafter.feature.draftset-data.common");
        IFn execFn = Clojure.var("drafter.feature.draftset-data.common", "exec-state-machine-sync");
        execFn.invoke(stateMachine, liveToDraftMapping, source, context);
    }

    public void delete(Draftset draftset, URI graph, File toDelete) {
        Object sm = getDeleteStateMachine();
        execStateMachine(sm, draftset, graph, toDelete);
    }

    public void dropDb() {
        try(RepositoryConnection conn = this.repo.getConnection()) {
            conn.prepareUpdate(QueryLanguage.SPARQL, "DROP ALL").execute();
        }
    }

    public static Drafter create() {
        return create(Util.getRepository());
    }

    public static Drafter create(SPARQLRepository repo) {
        Util.require("drafter.manager");
        IFn createFn = Clojure.var("drafter.manager", "create-manager");
        return new Drafter(repo, createFn.invoke(repo));
    }
}
