package com.swirrl;

import org.openjdk.jmh.annotations.*;

import java.io.File;

public class DeleteBenchmark {

    public static class DeleteState_1k_1g extends DeleteState {
        public DeleteState_1k_1g() {
            super("data_1k_1g.nq");
        }
    }

    public static class DeleteState_1k_10g extends DeleteState {
        public DeleteState_1k_10g() {
            super("data_1k_10g.nq");
        }
    }

    public static class DeleteState_1k_100g extends DeleteState {
        public DeleteState_1k_100g() {
            super("data_1k_100g.nq");
        }
    }

    public static class DeleteState_1k_200g extends DeleteState {
        public DeleteState_1k_200g() {
            super("data_1k_200g.nq");
        }
    }

    public static class DeleteState_10k_1g extends DeleteState {
        public DeleteState_10k_1g() {
            super("data_10k_1g.nq");
        }
    }

    public static class DeleteState_10k_10g extends DeleteState {
        public DeleteState_10k_10g() {
            super("data_10k_10g.nq");
        }
    }

    public static class DeleteState_10k_100g extends DeleteState {
        public DeleteState_10k_100g() {
            super("data_10k_100g.nq");
        }
    }

    public static class DeleteState_10k_200g extends DeleteState {
        public DeleteState_10k_200g() {
            super("data_10k_200g.nq");
        }
    }

    public static class DeleteState_100k_1g extends DeleteState {
        public DeleteState_100k_1g() {
            super("data_100k_1g.nq");
        }
    }

    public static class DeleteState_100k_10g extends DeleteState {
        public DeleteState_100k_10g() {
            super("data_100k_10g.nq");
        }
    }

    public static class DeleteState_100k_100g extends DeleteState {
        public DeleteState_100k_100g() {
            super("data_100k_100g.nq");
        }
    }

    public static class DeleteState_100k_200g extends DeleteState {
        public DeleteState_100k_200g() {
            super("data_100k_200g.nq");
        }
    }

    public static class DeleteState_1000k_1g extends DeleteState {
        public DeleteState_1000k_1g() {
            super("data_1000k_1g.nq");
        }
    }

    public static class DeleteState_1000k_10g extends DeleteState {
        public DeleteState_1000k_10g() {
            super("data_1000k_10g.nq");
        }
    }

    public static class DeleteState_1000k_100g extends DeleteState {
        public DeleteState_1000k_100g() {
            super("data_1000k_100g.nq");
        }
    }

    public static class DeleteState_1000k_200g extends DeleteState {
        public DeleteState_1000k_200g() {
            super("data_1000k_200g.nq");
        }
    }

    @State(Scope.Thread)
    public static class DeleteState {
        private Drafter drafter;
        private Draftset draftset;
        private final File dataFile;
        private final File deletionFile;

        protected DeleteState(String dataFileName) {
            this.dataFile = Util.resolveDataFile(dataFileName);

            String deletionFileName = dataFileName + ".delete";
            this.deletionFile = Util.resolveDataFile(deletionFileName);
        }

        @Setup(Level.Invocation)
        public void setup() {
            this.drafter = Drafter.create();
            this.draftset = this.drafter.createDraft(User.publisher());
            this.drafter.append(this.draftset, this.dataFile);
        }

        @TearDown(Level.Invocation)
        public void tearDown() {
            this.drafter.dropDb();
        }

        public Drafter getDrafter() { return this.drafter; }
        public Draftset getDraftset() { return this.draftset; }
        public File getDeletionFile() { return this.deletionFile; }
    }

    private static void deleteTest(DeleteState state) {
        state.getDrafter().delete(state.getDraftset(), state.getDeletionFile());
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteTest_1k_1g(DeleteState_1k_1g state) {
        deleteTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteTest_1k_10g(DeleteState_1k_10g state) {
        deleteTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteTest_1k_100g(DeleteState_1k_100g state) {
        deleteTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteTest_1k_200g(DeleteState_1k_200g state) {
        deleteTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteTest_10k_1g(DeleteState_10k_1g state) {
        deleteTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteTest_10k_10g(DeleteState_10k_10g state) {
        deleteTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteTest_10k_100g(DeleteState_10k_100g state) {
        deleteTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteTest_10k_200g(DeleteState_10k_200g state) {
        deleteTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteTest_100k_1g(DeleteState_100k_1g state) {
        deleteTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteTest_100k_10g(DeleteState_100k_10g state) {
        deleteTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteTest_100k_100g(DeleteState_100k_100g state) {
        deleteTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteTest_100k_200g(DeleteState_100k_200g state) {
        deleteTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteTest_1000k_1g(DeleteState_1000k_1g state) {
        deleteTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteTest_1000k_10g(DeleteState_1000k_10g state) {
        deleteTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteTest_1000k_100g(DeleteState_1000k_100g state) {
        deleteTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteTest_1000k_200g(DeleteState_1000k_200g state) {
        deleteTest(state);
    }
}
