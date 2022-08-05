package com.swirrl;

import org.openjdk.jmh.annotations.*;

import java.io.File;
import java.net.URI;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@Warmup(iterations = 0)
@Fork(value = 2, warmups = 0)
@Measurement(iterations = 2)
@Timeout(time = 2, timeUnit = TimeUnit.HOURS)
public class DeleteGraphBenchmark {
    @State(Scope.Thread)
    public static class DeleteGraphState {
        private final File dataFile;
        private final Drafter drafter;
        private Draftset draftset;
        private URI graphToDelete;

        protected DeleteGraphState(String dataFileName) {
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
        public void setup() {
            this.draftset = this.drafter.createDraft(User.publisher());
            this.drafter.append(this.draftset, this.dataFile);

            int numGraphs = Util.getNumGraphs(this.dataFile);
            this.graphToDelete = getGraphToDelete(numGraphs);
        }

        @TearDown(Level.Invocation)
        public void tearDown() {
            this.drafter.dropDb();
        }

        private static URI getGraphToDelete(int numGraphs) {
            int n = new Random().nextInt(numGraphs) + 1;

            // WARNING: This relies on the way graphs are generated by the data file generator!
            // see data-gen.core/generate-graphs
            return Util.uri("http://example.com/graphs/" + n);
        }

        public Drafter getDrafter() { return this.drafter; }
        public Draftset getDraftset() { return this.draftset; }
        public URI graphToDelete() { return this.graphToDelete; }
    }

    public static class DeleteGraphState_1k_1g extends DeleteGraphState {
        public DeleteGraphState_1k_1g() {
            super("data_1k_1g.nq");
        }
    }

    public static class DeleteGraphState_1k_10g extends DeleteGraphState {
        public DeleteGraphState_1k_10g() { super("data_1k_10g.nq"); }
    }

    public static class DeleteGraphState_1k_100g extends DeleteGraphState {
        public DeleteGraphState_1k_100g() { super("data_1k_100g.nq"); }
    }

    public static class DeleteGraphState_1k_200g extends DeleteGraphState {
        public DeleteGraphState_1k_200g() { super("data_1k_200g.nq"); }
    }

    public static class DeleteGraphState_10k_1g extends DeleteGraphState {
        public DeleteGraphState_10k_1g() { super("data_10k_1g.nq"); }
    }

    public static class DeleteGraphState_10k_10g extends DeleteGraphState {
        public DeleteGraphState_10k_10g() { super("data_10k_10g.nq"); }
    }

    public static class DeleteGraphState_10k_100g extends DeleteGraphState {
        public DeleteGraphState_10k_100g() { super("data_10k_100g.nq"); }
    }

    public static class DeleteGraphState_10k_200g extends DeleteGraphState {
        public DeleteGraphState_10k_200g() { super("data_10k_200g.nq"); }
    }

    public static class DeleteGraphState_100k_1g extends DeleteGraphState {
        public DeleteGraphState_100k_1g() { super("data_100k_1g.nq"); }
    }

    public static class DeleteGraphState_100k_10g extends DeleteGraphState {
        public DeleteGraphState_100k_10g() { super("data_100k_10g.nq"); }
    }

    public static class DeleteGraphState_100k_100g extends DeleteGraphState {
        public DeleteGraphState_100k_100g() { super("data_100k_100g.nq"); }
    }

    public static class DeleteGraphState_100k_200g extends DeleteGraphState {
        public DeleteGraphState_100k_200g() { super("data_100k_200g.nq"); }
    }

    public static class DeleteGraphState_1000k_1g extends DeleteGraphState {
        public DeleteGraphState_1000k_1g() { super("data_1000k_1g.nq"); }
    }

    public static class DeleteGraphState_1000k_10g extends DeleteGraphState {
        public DeleteGraphState_1000k_10g() { super("data_1000k_10g.nq"); }
    }

    public static class DeleteGraphState_1000k_100g extends DeleteGraphState {
        public DeleteGraphState_1000k_100g() { super("data_1000k_100g.nq"); }
    }

    public static class DeleteGraphState_1000k_200g extends DeleteGraphState {
        public DeleteGraphState_1000k_200g() { super("data_1000k_200g.nq"); }
    }

    private static void deleteGraphTest(DeleteGraphState state) {
        state.getDrafter().deleteGraph(state.getDraftset(), state.graphToDelete());
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteGraphTest_1k_1g(DeleteGraphState_1k_1g state) {
        deleteGraphTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteGraphTest_1k_10g(DeleteGraphState_1k_10g state) { deleteGraphTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteGraphTest_1k_100g(DeleteGraphState_1k_100g state) { deleteGraphTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteGraphTest_1k_200g(DeleteGraphState_1k_200g state) { deleteGraphTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteGraphTest_10k_1g(DeleteGraphState_10k_1g state) { deleteGraphTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteGraphTest_10k_10g(DeleteGraphState_10k_10g state) { deleteGraphTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteGraphTest_10k_100g(DeleteGraphState_10k_100g state) { deleteGraphTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteGraphTest_10k_200g(DeleteGraphState_10k_200g state) { deleteGraphTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteGraphTest_100k_1g(DeleteGraphState_100k_1g state) { deleteGraphTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteGraphTest_100k_10g(DeleteGraphState_100k_10g state) { deleteGraphTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteGraphTest_100k_100g(DeleteGraphState_100k_100g state) { deleteGraphTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteGraphTest_100k_200g(DeleteGraphState_100k_200g state) { deleteGraphTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteGraphTest_1000k_1g(DeleteGraphState_1000k_1g state) { deleteGraphTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteGraphTest_1000k_10g(DeleteGraphState_1000k_10g state) { deleteGraphTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteGraphTest_1000k_100g(DeleteGraphState_1000k_100g state) { deleteGraphTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteGraphTest_1000k_200g(DeleteGraphState_1000k_200g state) { deleteGraphTest(state); }
}
