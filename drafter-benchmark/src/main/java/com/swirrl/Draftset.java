package com.swirrl;

import clojure.java.api.Clojure;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;

/**
 * Wrapper around a drafter draftset representation.
 */
public class Draftset {
    private final Object obj;

    /**
     * Creates a new instance of this class
     * @param obj The drafter draftset object
     */
    public Draftset(Object obj) {
        this.obj = obj;
    }

    public Object obj() { return this.obj; }

    /**
     * Returns the internal drafter representation of the draft graph mapping for this draftset
     * @param repo A SPARQL repository to query for the graph mapping
     * @return The raw drafter representation of the draft graph mapping
     */
    public Object getGraphMapping(SPARQLRepository repo) {
        Util.require("drafter.backend.draftset.operations");
        return Clojure.var("drafter.backend.draftset.operations", "get-draftset-graph-mapping").invoke(repo, this.obj);
    }
}
