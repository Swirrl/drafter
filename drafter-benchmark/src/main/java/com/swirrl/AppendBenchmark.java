package com.swirrl;

import org.openjdk.jmh.annotations.*;

import java.io.File;

public class AppendBenchmark {
    @State(Scope.Thread)
    public static class DraftState {
        private Drafter drafter;
        private Draftset draftset;

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
        state.getDrafter().append(state.getDraftset(), Util.CENSUS_URI, dataFile);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void appendTest_5k(DraftState state) throws Exception {
        appendOnlyTest(state, "data_5k.nt");
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void appendTest_50k(DraftState state) throws Exception {
        appendOnlyTest(state, "data_50k.nt");
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void appendTest_500k(DraftState state) throws Exception {
        appendOnlyTest(state, "data_500k.nt");
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void appendTest_5m(DraftState state) throws Exception {
        appendOnlyTest(state, "data_5m.nt");
    }
}
