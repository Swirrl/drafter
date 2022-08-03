package com.swirrl;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.query.GraphQueryResult;
import org.eclipse.rdf4j.query.QueryResults;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.sparqlbuilder.graphpattern.GraphPattern;
import org.eclipse.rdf4j.sparqlbuilder.graphpattern.GraphPatterns;
import org.eclipse.rdf4j.sparqlbuilder.rdf.Rdf;
import org.openjdk.jmh.annotations.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Random;

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

        @Setup(Level.Iteration)
        public void setupIteration() {
            Util.createTestDb();
        }

        @TearDown(Level.Iteration)
        public void tearDownIteration() {
            Util.dropTestDb();
        }

        @Setup(Level.Invocation)
        public void setup() throws Exception {
            this.draftset = this.drafter.createDraft(User.publisher());
            this.drafter.append(this.draftset, this.dataFile);
            this.updateQuery = this.generateUpdateQuery();
        }

        // generates an UPDATE statement which deletes a random statement from the input data
        private static String deleteStatementQuery(Statement quad) {
            GraphPattern gp = GraphPatterns.tp(quad.getSubject(), quad.getPredicate(), quad.getObject())
                    .from(Rdf.iri((IRI)quad.getContext()));
            return String.format("DELETE DATA { %1$s }", gp.getQueryString());
        }

        private static int getStatementIndex(File dataFile) {
            int numStatements = Util.getNumStatements(dataFile);
            return new Random().nextInt(numStatements);
        }

        private static Statement getStatement(File dataFile) throws Exception {
            try (InputStream is = new FileInputStream(dataFile)) {
                try (GraphQueryResult res = QueryResults.parseGraphBackground(is, "", RDFFormat.NQUADS)) {
                    int idx = getStatementIndex(dataFile);
                    return res.stream().skip(idx).findFirst().get();
                }
            }
        }

        private String generateUpdateQuery() throws Exception {
            Statement quad = getStatement(this.dataFile);
            return deleteStatementQuery(quad);
        }

        @TearDown(Level.Invocation)
        public void tearDown() {
            this.drafter.dropDb();
        }

        public Drafter getDrafter() { return this.drafter; }
        public Draftset getDraftset() { return this.draftset; }
        public String getUpdateQuery() { return this.updateQuery; }
    }

    public static class UpdateQueryState_1k_1g extends UpdateQueryState {
        public UpdateQueryState_1k_1g() {
            super("data_1k_1g.nq");
        }
    }

    public static class UpdateQueryState_1k_10g extends UpdateQueryState {
        public UpdateQueryState_1k_10g() { super("data_1k_10g.nq"); }
    }

    public static class UpdateQueryState_1k_100g extends UpdateQueryState {
        public UpdateQueryState_1k_100g() { super("data_1k_100g.nq"); }
    }

    public static class UpdateQueryState_1k_200g extends UpdateQueryState {
        public UpdateQueryState_1k_200g() { super("data_1k_200g.nq"); }
    }

    public static class UpdateQueryState_10k_1g extends UpdateQueryState {
        public UpdateQueryState_10k_1g() { super("data_10k_1g.nq"); }
    }

    public static class UpdateQueryState_10k_10g extends UpdateQueryState {
        public UpdateQueryState_10k_10g() { super("data_10k_10g.nq"); }
    }

    public static class UpdateQueryState_10k_100g extends UpdateQueryState {
        public UpdateQueryState_10k_100g() { super("data_10k_100g.nq"); }
    }

    public static class UpdateQueryState_10k_200g extends UpdateQueryState {
        public UpdateQueryState_10k_200g() { super("data_10k_200g.nq"); }
    }

    public static class UpdateQueryState_100k_1g extends UpdateQueryState {
        public UpdateQueryState_100k_1g() { super("data_100k_1g.nq"); }
    }

    public static class UpdateQueryState_100k_10g extends UpdateQueryState {
        public UpdateQueryState_100k_10g() { super("data_100k_10g.nq"); }
    }

    public static class UpdateQueryState_100k_100g extends UpdateQueryState {
        public UpdateQueryState_100k_100g() { super("data_100k_100g.nq"); }
    }

    public static class UpdateQueryState_100k_200g extends UpdateQueryState {
        public UpdateQueryState_100k_200g() { super("data_100k_200g.nq"); }
    }

    public static class UpdateQueryState_1000k_1g extends UpdateQueryState {
        public UpdateQueryState_1000k_1g() { super("data_1000k_1g.nq"); }
    }

    public static class UpdateQueryState_1000k_10g extends UpdateQueryState {
        public UpdateQueryState_1000k_10g() { super("data_1000k_10g.nq"); }
    }

    public static class UpdateQueryState_1000k_100g extends UpdateQueryState {
        public UpdateQueryState_1000k_100g() { super("data_1000k_100g.nq"); }
    }

    public static class UpdateQueryState_1000k_200g extends UpdateQueryState {
        public UpdateQueryState_1000k_200g() { super("data_1000k_200g.nq"); }
    }

    private static void updateQueryTest(UpdateQueryState state) {
        state.getDrafter().submitUpdate(state.getDraftset(), state.getUpdateQuery());
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void updateQueryTest_1k_1g(UpdateQueryState_1k_1g state) {
        updateQueryTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void updateQueryTest_1k_10g(UpdateQueryState_1k_10g state) {
        updateQueryTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void updateQueryTest_1k_100g(UpdateQueryState_1k_100g state) {
        updateQueryTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void updateQueryTest_1k_200g(UpdateQueryState_1k_200g state) {
        updateQueryTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void updateQueryTest_10k_1g(UpdateQueryState_10k_1g state) { updateQueryTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void updateQueryTest_10k_10g(UpdateQueryState_10k_10g state) { updateQueryTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void updateQueryTest_10k_100g(UpdateQueryState_10k_100g state) { updateQueryTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void updateQueryTest_10k_200g(UpdateQueryState_10k_200g state) { updateQueryTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void updateQueryTest_100k_1g(UpdateQueryState_100k_1g state) { updateQueryTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void updateQueryTest_100k_10g(UpdateQueryState_100k_10g state) { updateQueryTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void updateQueryTest_100k_100g(UpdateQueryState_100k_100g state) { updateQueryTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void updateQueryTest_100k_200g(UpdateQueryState_100k_200g state) { updateQueryTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void updateQueryTest_1000k_1g(UpdateQueryState_1000k_1g state) { updateQueryTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void updateQueryTest_1000k_10g(UpdateQueryState_1000k_10g state) { updateQueryTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void updateQueryTest_1000k_100g(UpdateQueryState_1000k_100g state) { updateQueryTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void updateQueryTest_1000k_200g(UpdateQueryState_1000k_200g state) { updateQueryTest(state); }
}
