package com.swirrl;

import org.openjdk.jmh.annotations.*;

import java.io.File;

public class AppendBenchmark {
    @State(Scope.Thread)
    public static class DraftState {
        private Drafter drafter;
        private Draftset draftset;

        @Setup(Level.Iteration)
        public void setupIteration() {
            Util.createTestDb();
        }

        @TearDown(Level.Iteration)
        public void tearDownIteration() {
            Util.dropTestDb();
        }

        @Setup(Level.Invocation)
        public void setup() {
            this.drafter = Drafter.create();
            this.draftset = this.drafter.createDraft(User.publisher());
        }

        @TearDown(Level.Invocation)
        public void tearDown() {
            this.drafter.dropDb();
        }

        public Drafter getDrafter() { return this.drafter; }
        public Draftset getDraftset() { return this.draftset; }
    }

    private static void appendOnlyTest(DraftState state, String fileName) throws Exception {
        File dataFile = Util.resolveDataFile(fileName);
        state.getDrafter().append(state.getDraftset(), dataFile);
    }

    // 1k statements

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void appendTest_1k_1g(DraftState state) throws Exception {
        appendOnlyTest(state, "data_1k_1g.nq");
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void appendTest_1k_10g(DraftState state) throws Exception {
        appendOnlyTest(state, "data_1k_10g.nq");
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void appendTest_1k_100g(DraftState state) throws Exception {
        appendOnlyTest(state, "data_1k_100g.nq");
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void appendTest_1k_200g(DraftState state) throws Exception {
        appendOnlyTest(state, "data_1k_200g.nq");
    }

    // 10k statements

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void appendTest_10k_1g(DraftState state) throws Exception {
        appendOnlyTest(state, "data_10k_1g.nq");
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void appendTest_10k_10g(DraftState state) throws Exception {
        appendOnlyTest(state, "data_10k_10g.nq");
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void appendTest_10k_100g(DraftState state) throws Exception {
        appendOnlyTest(state, "data_10k_100g.nq");
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void appendTest_10k_200g(DraftState state) throws Exception {
        appendOnlyTest(state, "data_10k_200g.nq");
    }

    // 100k statements

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void appendTest_100k_1g(DraftState state) throws Exception {
        appendOnlyTest(state, "data_100k_1g.nq");
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void appendTest_100k_10g(DraftState state) throws Exception {
        appendOnlyTest(state, "data_100k_10g.nq");
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void appendTest_100k_100g(DraftState state) throws Exception {
        appendOnlyTest(state, "data_100k_100g.nq");
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void appendTest_100k_200g(DraftState state) throws Exception {
        appendOnlyTest(state, "data_100k_200g.nq");
    }

    // 1m statements

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void appendTest_1000k_1g(DraftState state) throws Exception {
        appendOnlyTest(state, "data_1000k_1g.nq");
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void appendTest_1000k_10g(DraftState state) throws Exception {
        appendOnlyTest(state, "data_1000k_10g.nq");
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void appendTest_1000k_100g(DraftState state) throws Exception {
        appendOnlyTest(state, "data_1000k_100g.nq");
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void appendTest_1000k_200g(DraftState state) throws Exception {
        appendOnlyTest(state, "data_1000k_200g.nq");
    }
}
