package com.swirrl;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;

import java.io.File;

public class Drafter {
    private final SPARQLRepository repo;
    private final Object manager;

    private Drafter(SPARQLRepository repo, Object  manager) {
        this.repo = repo;
        this.manager = manager;
    }

    public Draftset createDraft(User user) {
        // TODO: add accessor function
        Object repo = Util.keyword("backend").invoke(manager);

        Util.require("drafter.backend.draftset.operations");
        IFn createFn = Clojure.var("drafter.backend.draftset.operations", "create-draftset!");

        return new Draftset(createFn.invoke(repo, user.obj()));
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

    private void innerAppend(Draftset draftset, Object source) {
        Object sm = getAppendStateMachine();

        // TODO: query db!
        // TODO: make argument?
        Object liveToDraftMapping = Clojure.var("clojure.core", "hash-map").invoke();

        Object context = this.jobContext(draftset);

        IFn execFn = Clojure.var("drafter.feature.draftset-data.common", "exec-state-machine-sync");
        execFn.invoke(sm, liveToDraftMapping, source, context);
    }

    public void append(Draftset draftset, File dataFile) {
        Object source = Util.getInputSource(dataFile);
        this.innerAppend(draftset, source);
    }

    public void append(Draftset draftset, String fileName) {
        Object source = Util.getInputSource(fileName);
        this.innerAppend(draftset, source);
    }

    private static Object getDeleteStateMachine() {
        Util.require("drafter.feature.draftset-data.delete");
        return Clojure.var("drafter.feature.draftset-data.delete", "delete-state-machine").invoke();
    }

    public void delete(Draftset draftset, File toDelete) {
        Object sm = getDeleteStateMachine();

        Object liveToDraftMapping = draftset.getGraphMapping(this.repo);
        Object source = Util.getInputSource(toDelete);
        Object context = this.jobContext(draftset);

        IFn execFn = Clojure.var("drafter.feature.draftset-data.common", "exec-state-machine-sync");
        execFn.invoke(sm, liveToDraftMapping, source, context);
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
