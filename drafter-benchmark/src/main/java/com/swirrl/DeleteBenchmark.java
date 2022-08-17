package com.swirrl;

import org.openjdk.jmh.annotations.*;

import java.io.File;
import java.util.concurrent.TimeUnit;

@Warmup(iterations = 0)
@Fork(value = 2, warmups = 0)
@Measurement(iterations = 2)
@Timeout(time = 2, timeUnit = TimeUnit.HOURS)
public class DeleteBenchmark {

    public static class DeleteState_1k_1g_0pc extends DeleteState {
        public DeleteState_1k_1g_0pc() {
            super("data_1k_1g_0pc.nq");
        }
    }

    public static class DeleteState_1k_10g_0pc extends DeleteState {
        public DeleteState_1k_10g_0pc() {
            super("data_1k_10g_0pc.nq");
        }
    }

    public static class DeleteState_1k_100g_0pc extends DeleteState {
        public DeleteState_1k_100g_0pc() {
            super("data_1k_100g_0pc.nq");
        }
    }

    public static class DeleteState_1k_200g_0pc extends DeleteState {
        public DeleteState_1k_200g_0pc() {
            super("data_1k_200g_0pc.nq");
        }
    }

    public static class DeleteState_10k_1g_0pc extends DeleteState {
        public DeleteState_10k_1g_0pc() {
            super("data_10k_1g_0pc.nq");
        }
    }

    public static class DeleteState_10k_10g_0pc extends DeleteState {
        public DeleteState_10k_10g_0pc() {
            super("data_10k_10g_0pc.nq");
        }
    }

    public static class DeleteState_10k_100g_0pc extends DeleteState {
        public DeleteState_10k_100g_0pc() {
            super("data_10k_100g_0pc.nq");
        }
    }

    public static class DeleteState_10k_200g_0pc extends DeleteState {
        public DeleteState_10k_200g_0pc() {
            super("data_10k_200g_0pc.nq");
        }
    }

    public static class DeleteState_100k_1g_0pc extends DeleteState {
        public DeleteState_100k_1g_0pc() {
            super("data_100k_1g_0pc.nq");
        }
    }

    public static class DeleteState_100k_10g_0pc extends DeleteState {
        public DeleteState_100k_10g_0pc() {
            super("data_100k_10g_0pc.nq");
        }
    }

    public static class DeleteState_100k_100g_0pc extends DeleteState {
        public DeleteState_100k_100g_0pc() {
            super("data_100k_100g_0pc.nq");
        }
    }

    public static class DeleteState_100k_200g_0pc extends DeleteState {
        public DeleteState_100k_200g_0pc() {
            super("data_100k_200g_0pc.nq");
        }
    }

    public static class DeleteState_1000k_1g_0pc extends DeleteState {
        public DeleteState_1000k_1g_0pc() {
            super("data_1000k_1g_0pc.nq");
        }
    }

    public static class DeleteState_1000k_10g_0pc extends DeleteState {
        public DeleteState_1000k_10g_0pc() {
            super("data_1000k_10g_0pc.nq");
        }
    }

    public static class DeleteState_1000k_100g_0pc extends DeleteState {
        public DeleteState_1000k_100g_0pc() {
            super("data_1000k_100g_0pc.nq");
        }
    }

    public static class DeleteState_1000k_200g_0pc extends DeleteState {
        public DeleteState_1000k_200g_0pc() {
            super("data_1000k_200g_0pc.nq");
        }
    }

    public static class DeleteState_100k_10g_1pc extends DeleteState {
        public DeleteState_100k_10g_1pc() { super("data_100k_10g_1pc.nq"); }
    }

    public static class DeleteState_100k_10g_5pc extends DeleteState {
        public DeleteState_100k_10g_5pc() { super("data_100k_10g_5pc.nq"); }
    }

    public static class DeleteState_100k_10g_10pc extends DeleteState {
        public DeleteState_100k_10g_10pc() { super("data_100k_10g_10pc.nq"); }
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
            this.drafter.append(this.draftset, this.dataFile);
        }

        @TearDown(Level.Invocation)
        public void tearDown() {
            //Util.dropTestDb();
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
    public void deleteTest_1k_1g_0pc(DeleteState_1k_1g_0pc state) {
        deleteTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteTest_1k_10g_0pc(DeleteState_1k_10g_0pc state) {
        deleteTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteTest_1k_100g_0pc(DeleteState_1k_100g_0pc state) {
        deleteTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteTest_1k_200g_0pc(DeleteState_1k_200g_0pc state) {
        deleteTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteTest_10k_1g_0pc(DeleteState_10k_1g_0pc state) {
        deleteTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteTest_10k_10g_0pc(DeleteState_10k_10g_0pc state) {
        deleteTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteTest_10k_100g_0pc(DeleteState_10k_100g_0pc state) {
        deleteTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteTest_10k_200g_0pc(DeleteState_10k_200g_0pc state) {
        deleteTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteTest_100k_1g_0pc(DeleteState_100k_1g_0pc state) {
        deleteTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteTest_100k_10g_0pc(DeleteState_100k_10g_0pc state) {
        deleteTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteTest_100k_100g_0pc(DeleteState_100k_100g_0pc state) {
        deleteTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteTest_100k_200g_0pc(DeleteState_100k_200g_0pc state) {
        deleteTest(state);
    }

//    @Benchmark
//    @BenchmarkMode(Mode.AverageTime)
//    public void deleteTest_1000k_1g_0pc(DeleteState_1000k_1g_0pc state) {
//        deleteTest(state);
//    }
//
//    @Benchmark
//    @BenchmarkMode(Mode.AverageTime)
//    public void deleteTest_1000k_10g_0pc(DeleteState_1000k_10g_0pc state) {
//        deleteTest(state);
//    }
//
//    @Benchmark
//    @BenchmarkMode(Mode.AverageTime)
//    public void deleteTest_1000k_100g_0pc(DeleteState_1000k_100g_0pc state) {
//        deleteTest(state);
//    }

//    @Benchmark
//    @BenchmarkMode(Mode.AverageTime)
//    public void deleteTest_1000k_200g_0pc(DeleteState_1000k_200g_0pc state) {
//        deleteTest(state);
//    }

    // graph-referencing tests
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteTest_100k_10g_1pc(DeleteState_100k_10g_1pc state) {
        deleteTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteTest_100k_10g_5pc(DeleteState_100k_10g_5pc state) {
        deleteTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteTest_100k_10g_10pc(DeleteState_100k_10g_10pc state) {
        deleteTest(state);
    }
}
