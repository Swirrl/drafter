package com.swirrl;

import org.openjdk.jmh.annotations.*;

import java.io.File;
import java.util.concurrent.TimeUnit;

@Warmup(iterations = 0)
@Fork(value = 2, warmups = 0)
@Measurement(iterations = 2)
@Timeout(time = 2, timeUnit = TimeUnit.HOURS)
public class PublishBenchmark {

    public static class PublishState_1k_1g_0pc extends PublishState {
        public PublishState_1k_1g_0pc() { super("data_1k_1g_0pc.nq"); }
    }

    public static class PublishState_1k_10g_0pc extends PublishState {
        public PublishState_1k_10g_0pc() { super("data_1k_10g_0pc.nq"); }
    }

    public static class PublishState_1k_100g_0pc extends PublishState {
        public PublishState_1k_100g_0pc() { super("data_1k_100g_0pc.nq"); }
    }

    public static class PublishState_1k_200g_0pc extends PublishState {
        public PublishState_1k_200g_0pc() { super("data_1k_200g_0pc.nq"); }
    }

    public static class PublishState_10k_1g_0pc extends PublishState {
        public PublishState_10k_1g_0pc() { super("data_10k_1g_0pc.nq"); }
    }

    public static class PublishState_10k_10g_0pc extends PublishState {
        public PublishState_10k_10g_0pc() { super("data_10k_10g_0pc.nq"); }
    }

    public static class PublishState_10k_100g_0pc extends PublishState {
        public PublishState_10k_100g_0pc() { super("data_10k_100g_0pc.nq"); }
    }

    public static class PublishState_10k_200g_0pc extends PublishState {
        public PublishState_10k_200g_0pc() { super("data_10k_200g_0pc.nq"); }
    }

    public static class PublishState_100k_1g_0pc extends PublishState {
        public PublishState_100k_1g_0pc() { super("data_100k_1g_0pc.nq"); }
    }

    public static class PublishState_100k_10g_0pc extends PublishState {
        public PublishState_100k_10g_0pc() { super("data_100k_10g_0pc.nq"); }
    }

    public static class PublishState_100k_100g_0pc extends PublishState {
        public PublishState_100k_100g_0pc() { super("data_100k_100g_0pc.nq"); }
    }

    public static class PublishState_100k_200g_0pc extends PublishState {
        public PublishState_100k_200g_0pc() { super("data_100k_200g_0pc.nq"); }
    }

    public static class PublishState_1000k_1g_0pc extends PublishState {
        public PublishState_1000k_1g_0pc() { super("data_1000k_1g_0pc.nq"); }
    }

    public static class PublishState_1000k_10g_0pc extends PublishState {
        public PublishState_1000k_10g_0pc() { super("data_1000k_10g_0pc.nq"); }
    }

    public static class PublishState_1000k_100g_0pc extends PublishState {
        public PublishState_1000k_100g_0pc() { super("data_1000k_100g_0pc.nq"); }
    }

    public static class PublishState_1000k_200g_0pc extends PublishState {
        public PublishState_1000k_200g_0pc() { super("data_1000k_200g_0pc.nq"); }
    }

    @State(Scope.Thread)
    public static class PublishState {
        private final File dataFile;
        private final Drafter drafter;
        private Draftset draftset;

        protected PublishState(String dataFileName) {
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
        }

        @TearDown(Level.Invocation)
        public void tearDown() {
            this.drafter.dropDb();
        }

        public Drafter getDrafter() { return this.drafter; }
        public Draftset getDraftset() { return this.draftset; }
    }

    private static void publishTest(PublishState state) {
        state.getDrafter().publish(state.getDraftset());
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void publishTest_1k_1g_0pc(PublishState_1k_1g_0pc state) {
        publishTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void publishTest_1k_10g_0pc(PublishState_1k_10g_0pc state) { publishTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void publishTest_1k_100g_0pc(PublishState_1k_100g_0pc state) { publishTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void publishTest_1k_200g_0pc(PublishState_1k_200g_0pc state) { publishTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void publishTest_10k_1g_0pc(PublishState_10k_1g_0pc state) { publishTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void publishTest_10k_10g_0pc(PublishState_10k_10g_0pc state) { publishTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void publishTest_10k_100g_0pc(PublishState_10k_100g_0pc state) { publishTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void publishTest_10k_200g_0pc(PublishState_10k_200g_0pc state) { publishTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void publishTest_100k_1g_0pc(PublishState_100k_1g_0pc state) { publishTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void publishTest_100k_10g_0pc(PublishState_100k_10g_0pc state) { publishTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void publishTest_100k_100g_0pc(PublishState_100k_100g_0pc state) { publishTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void publishTest_100k_200g_0pc(PublishState_100k_200g_0pc state) { publishTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void publishTest_1000k_1g_0pc(PublishState_1000k_1g_0pc state) { publishTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void publishTest_1000k_10g_0pc(PublishState_1000k_10g_0pc state) { publishTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void publishTest_1000k_100g_0pc(PublishState_1000k_100g_0pc state) { publishTest(state); }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void publishTest_1000k_200g_0pc(PublishState_1000k_200g_0pc state) { publishTest(state); }
}
