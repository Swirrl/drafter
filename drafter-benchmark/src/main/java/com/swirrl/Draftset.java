package com.swirrl;

import clojure.java.api.Clojure;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;

public class Draftset {
    private final Object obj;

    public Draftset(Object obj) {
        this.obj = obj;
    }

    public Object obj() { return this.obj; }

    public Object getGraphMapping(SPARQLRepository repo) {
        Util.require("drafter.backend.draftset.operations");
        return Clojure.var("drafter.backend.draftset.operations", "get-draftset-graph-mapping").invoke(repo, this.obj);
    }
}
