package com.swirrl;

import org.openjdk.jmh.annotations.*;

import java.io.File;
import java.util.concurrent.TimeUnit;

@Warmup(iterations = 0)
@Fork(value = 2, warmups = 0)
@Measurement(iterations = 2)
@Timeout(time = 2, timeUnit = TimeUnit.HOURS)
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
    public void appendTest_1k_1g_0pc(DraftState state) throws Exception {
        appendOnlyTest(state, "data_1k_1g_0pc.nq");
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void appendTest_1k_10g_0pc(DraftState state) throws Exception {
        appendOnlyTest(state, "data_1k_10g_0pc.nq");
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void appendTest_1k_100g_0pc(DraftState state) throws Exception {
        appendOnlyTest(state, "data_1k_100g_0pc.nq");
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void appendTest_1k_200g_0pc(DraftState state) throws Exception {
        appendOnlyTest(state, "data_1k_200g_0pc.nq");
    }

    // 10k statements

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void appendTest_10k_1g_0pc(DraftState state) throws Exception {
        appendOnlyTest(state, "data_10k_1g_0pc.nq");
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void appendTest_10k_10g_0pc(DraftState state) throws Exception {
        appendOnlyTest(state, "data_10k_10g_0pc.nq");
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void appendTest_10k_100g_0pc(DraftState state) throws Exception {
        appendOnlyTest(state, "data_10k_100g_0pc.nq");
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void appendTest_10k_200g_0pc(DraftState state) throws Exception {
        appendOnlyTest(state, "data_10k_200g_0pc.nq");
    }

    // 100k statements

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void appendTest_100k_1g_0pc(DraftState state) throws Exception {
        appendOnlyTest(state, "data_100k_1g_0pc.nq");
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void appendTest_100k_10g_0pc(DraftState state) throws Exception {
        appendOnlyTest(state, "data_100k_10g_0pc.nq");
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void appendTest_100k_100g_0pc(DraftState state) throws Exception {
        appendOnlyTest(state, "data_100k_100g_0pc.nq");
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void appendTest_100k_200g_0pc(DraftState state) throws Exception {
        appendOnlyTest(state, "data_100k_200g_0pc.nq");
    }

    // 1m statements

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void appendTest_1000k_1g_0pc(DraftState state) throws Exception {
        appendOnlyTest(state, "data_1000k_1g_0pc.nq");
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void appendTest_1000k_10g_0pc(DraftState state) throws Exception {
        appendOnlyTest(state, "data_1000k_10g_0pc.nq");
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void appendTest_1000k_100g_0pc(DraftState state) throws Exception {
        appendOnlyTest(state, "data_1000k_100g_0pc.nq");
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void appendTest_1000k_200g_0pc(DraftState state) throws Exception {
        appendOnlyTest(state, "data_1000k_200g_0pc.nq");
    }

    // graph-referencing statements
    // all use 100k statements over 10 graphs
    // NOTE: 100k_10g_0pc defined above

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void appendTest_100k_10g_1pc(DraftState state) throws Exception {
        appendOnlyTest(state, "data_100k_10g_1pc.nq");
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void appendTest_100k_10g_5pc(DraftState state) throws Exception {
        appendOnlyTest(state, "data_100k_10g_5pc.nq");
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void appendTest_100k_10g_10pc(DraftState state) throws Exception {
        appendOnlyTest(state, "data_100k_10g_10pc.nq");
    }
}
