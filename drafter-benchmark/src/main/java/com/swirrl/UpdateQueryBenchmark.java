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
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks for executing update queries. Each benchmark takes an instance of its own state class responsible for
 * setting up the test database. Each state class creates a new empty draftset and appends all data from the
 * associated benchmark data file. It chooses a random statement to delete and constructs a SPARQL UPDATE query to
 * delete it. Each benchmark then submits the update query and waits for it to complete.
 */
@Warmup(iterations = 0)
@Fork(value = 2, warmups = 0)
@Measurement(iterations = 2)
@Timeout(time = 2, timeUnit = TimeUnit.HOURS)
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

    public static class UpdateQueryState_1k_1g_0pc extends UpdateQueryState {
        public UpdateQueryState_1k_1g_0pc() {
            super("data_1k_1g_0pc.nq");
        }
    }

    public static class UpdateQueryState_1k_10g_0pc extends UpdateQueryState {
        public UpdateQueryState_1k_10g_0pc() { super("data_1k_10g_0pc.nq"); }
    }

    public static class UpdateQueryState_1k_100g_0pc extends UpdateQueryState {
        public UpdateQueryState_1k_100g_0pc() { super("data_1k_100g_0pc.nq"); }
    }

    public static class UpdateQueryState_1k_200g_0pc extends UpdateQueryState {
        public UpdateQueryState_1k_200g_0pc() { super("data_1k_200g_0pc.nq"); }
    }

    public static class UpdateQueryState_10k_1g_0pc extends UpdateQueryState {
        public UpdateQueryState_10k_1g_0pc() { super("data_10k_1g_0pc.nq"); }
    }

    public static class UpdateQueryState_10k_10g_0pc extends UpdateQueryState {
        public UpdateQueryState_10k_10g_0pc() { super("data_10k_10g_0pc.nq"); }
    }

    public static class UpdateQueryState_10k_100g_0pc extends UpdateQueryState {
        public UpdateQueryState_10k_100g_0pc() { super("data_10k_100g_0pc.nq"); }
    }

    public static class UpdateQueryState_10k_200g_0pc extends UpdateQueryState {
        public UpdateQueryState_10k_200g_0pc() { super("data_10k_200g_0pc.nq"); }
    }

    public static class UpdateQueryState_100k_1g_0pc extends UpdateQueryState {
        public UpdateQueryState_100k_1g_0pc() { super("data_100k_1g_0pc.nq"); }
    }

    public static class UpdateQueryState_100k_10g_0pc extends UpdateQueryState {
        public UpdateQueryState_100k_10g_0pc() { super("data_100k_10g_0pc.nq"); }
    }

    public static class UpdateQueryState_100k_100g_0pc extends UpdateQueryState {
        public UpdateQueryState_100k_100g_0pc() { super("data_100k_100g_0pc.nq"); }
    }

    public static class UpdateQueryState_100k_200g_0pc extends UpdateQueryState {
        public UpdateQueryState_100k_200g_0pc() { super("data_100k_200g_0pc.nq"); }
    }

    public static class UpdateQueryState_1000k_1g_0pc extends UpdateQueryState {
        public UpdateQueryState_1000k_1g_0pc() { super("data_1000k_1g_0pc.nq"); }
    }

    public static class UpdateQueryState_1000k_10g_0pc extends UpdateQueryState {
        public UpdateQueryState_1000k_10g_0pc() { super("data_1000k_10g_0pc.nq"); }
    }

    public static class UpdateQueryState_1000k_100g_0pc extends UpdateQueryState {
        public UpdateQueryState_1000k_100g_0pc() { super("data_1000k_100g_0pc.nq"); }
    }

    public static class UpdateQueryState_1000k_200g_0pc extends UpdateQueryState {
        public UpdateQueryState_1000k_200g_0pc() { super("data_1000k_200g_0pc.nq"); }
    }

    public static class UpdateQueryState_100k_10g_1pc extends UpdateQueryState {
        public UpdateQueryState_100k_10g_1pc() { super("data_100k_10g_1pc.nq"); }
    }

    public static class UpdateQueryState_100k_10g_5pc extends UpdateQueryState {
        public UpdateQueryState_100k_10g_5pc() { super("data_100k_10g_5pc.nq"); }
    }

    public static class UpdateQueryState_100k_10g_10pc extends UpdateQueryState {
        public UpdateQueryState_100k_10g_10pc() { super("data_100k_10g_10pc.nq"); }
    }

    private static void updateQueryTest(UpdateQueryState state) {
        state.getDrafter().submitUpdate(state.getDraftset(), state.getUpdateQuery());
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void updateQueryTest_1k_1g_0pc(UpdateQueryState_1k_1g_0pc state) {
        updateQueryTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void updateQueryTest_1k_10g_0pc(UpdateQueryState_1k_10g_0pc state) {
        updateQueryTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void updateQueryTest_1k_100g_0pc(UpdateQueryState_1k_100g_0pc state) {
        updateQueryTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void updateQueryTest_1k_200g_0pc(UpdateQueryState_1k_200g_0pc state) {
        updateQueryTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void updateQueryTest_10k_1g_0pc(UpdateQueryState_10k_1g_0pc state) { updateQueryTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void updateQueryTest_10k_10g_0pc(UpdateQueryState_10k_10g_0pc state) { updateQueryTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void updateQueryTest_10k_100g_0pc(UpdateQueryState_10k_100g_0pc state) { updateQueryTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void updateQueryTest_10k_200g_0pc(UpdateQueryState_10k_200g_0pc state) { updateQueryTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void updateQueryTest_100k_1g_0pc(UpdateQueryState_100k_1g_0pc state) { updateQueryTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void updateQueryTest_100k_10g_0pc(UpdateQueryState_100k_10g_0pc state) { updateQueryTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void updateQueryTest_100k_100g_0pc(UpdateQueryState_100k_100g_0pc state) { updateQueryTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void updateQueryTest_100k_200g_0pc(UpdateQueryState_100k_200g_0pc state) { updateQueryTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void updateQueryTest_1000k_1g_0pc(UpdateQueryState_1000k_1g_0pc state) { updateQueryTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void updateQueryTest_1000k_10g_0pc(UpdateQueryState_1000k_10g_0pc state) { updateQueryTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void updateQueryTest_1000k_100g_0pc(UpdateQueryState_1000k_100g_0pc state) { updateQueryTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void updateQueryTest_1000k_200g_0pc(UpdateQueryState_1000k_200g_0pc state) { updateQueryTest(state); }

    // graph-referencing tests
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void updateQueryTest_100k_10g_1pc(UpdateQueryState_100k_10g_1pc state) { updateQueryTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void updateQueryTest_100k_10g_5pc(UpdateQueryState_100k_10g_5pc state) { updateQueryTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void updateQueryTest_100k_10g_10pc(UpdateQueryState_100k_10g_10pc state) { updateQueryTest(state); }
}
