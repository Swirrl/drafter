package com.swirrl;

import org.openjdk.jmh.annotations.*;

import java.io.File;

public class PublishBenchmark {

    public static class PublishState_5k extends PublishState {
        public PublishState_5k() {
            super("data_5k.nt");
        }
    }

    public static class PublishState_50k extends PublishState {
        public PublishState_50k() {
            super("data_50k.nt");
        }
    }

    public static class PublishState_500k extends PublishState {
        public PublishState_500k() {
            super("data_500k.nt");
        }
    }

    public static class PublishState_5m extends PublishState {
        public PublishState_5m() {
            super("data_5m.nt");
        }
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

        @Setup(Level.Invocation)
        public void setup() {
            this.draftset = this.drafter.createDraft(User.publisher());
            this.drafter.append(this.draftset, Util.CENSUS_URI, this.dataFile);
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
    public void publishTest_5k(PublishState_5k state) {
        publishTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void publishTest_50k(PublishState_50k state) {
        publishTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void publishTest_500k(PublishState_500k state) {
        publishTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void publishTest_5m(PublishState_5m state) {
        publishTest(state);
    }
}
