package com.swirrl;

import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.query.GraphQueryResult;
import org.eclipse.rdf4j.query.QueryResults;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.openjdk.jmh.annotations.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;

public class UpdateQueryBenchmark {
    @State(Scope.Thread)
    public static class UpdateQueryState {
        private final Drafter drafter;
        private Draftset draftset;
        private final File dataFile;
        private String updateQuery;

        protected UpdateQueryState(String dataFileName) {
            this.dataFile = Util.resolveDataFile(dataFileName);
            this.drafter = Drafter.create();
        }

        @Setup(Level.Invocation)
        public void setup() throws Exception {
            this.draftset = this.drafter.createDraft(User.publisher());
            this.drafter.append(this.draftset, Util.CENSUS_URI, this.dataFile);
            this.updateQuery = this.generateUpdateQuery();
        }

        private static String deleteStatementQuery(URI graphUri, Statement triple) {
            // TODO: create from query class?
            return String.format("DELETE DATA { GRAPH <%1$s> { <%2$s> <%3$s> <%4$s> } }",
                    graphUri,
                    triple.getSubject(),
                    triple.getPredicate(),
                    triple.getObject());
        }

        private static Statement getTriple(File dataFile) throws Exception {
            try (InputStream is = new FileInputStream(dataFile)) {
                try (GraphQueryResult res = QueryResults.parseGraphBackground(is, "", RDFFormat.NTRIPLES)) {
                    if (! res.hasNext()) {
                        throw new RuntimeException("Failed to read triple from source file");
                    }
                    return res.next();
                }
            }
        }

        private String generateUpdateQuery() throws Exception {
            Statement triple = getTriple(this.dataFile);
            return deleteStatementQuery(Util.CENSUS_URI, triple);
        }

        @TearDown(Level.Invocation)
        public void tearDown() {
            this.drafter.dropDb();
        }

        public Drafter getDrafter() { return this.drafter; }
        public Draftset getDraftset() { return this.draftset; }
        public String getUpdateQuery() { return this.updateQuery; }
    }

    public static class UpdateQueryState_5k extends UpdateQueryState {
        public UpdateQueryState_5k() {
            super("data_5k.nt");
        }
    }

    public static class UpdateQueryState_50k extends UpdateQueryState {
        public UpdateQueryState_50k() {
            super("data_50k.nt");
        }
    }

    public static class UpdateQueryState_500k extends UpdateQueryState {
        public UpdateQueryState_500k() {
            super("data_500k.nt");
        }
    }

    public static class UpdateQueryState_5m extends UpdateQueryState {
        public UpdateQueryState_5m() {
            super("data_5m.nt");
        }
    }

    private static void updateQueryTest(UpdateQueryState state) {
        state.getDrafter().submitUpdate(state.getDraftset(), state.getUpdateQuery());
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void updateQueryTest_5k(UpdateQueryState_5k state) {
        updateQueryTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void updateQueryTest_50k(UpdateQueryState_50k state) {
        updateQueryTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void updateQueryTest_500k(UpdateQueryState_500k state) {
        updateQueryTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void updateQueryTest_5m(UpdateQueryState_5m state) {
        updateQueryTest(state);
    }
}
