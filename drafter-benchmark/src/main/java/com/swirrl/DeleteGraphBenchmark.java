package com.swirrl;

import org.openjdk.jmh.annotations.*;

import java.io.File;
import java.net.URI;

public class DeleteGraphBenchmark {
    @State(Scope.Thread)
    public static class DeleteGraphState {
        private final File dataFile;
        private final Drafter drafter;
        private Draftset draftset;

        protected DeleteGraphState(String dataFileName) {
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
        public URI graphToDelete() { return Util.CENSUS_URI; }
    }

    public static class DeleteGraphState_5k extends DeleteGraphState {
        public DeleteGraphState_5k() {
            super("data_5k.nt");
        }
    }

    public static class DeleteGraphState_50k extends DeleteGraphState {
        public DeleteGraphState_50k() {
            super("data_50k.nt");
        }
    }

    public static class DeleteGraphState_500k extends DeleteGraphState {
        public DeleteGraphState_500k() {
            super("data_500k.nt");
        }
    }

    public static class DeleteGraphState_5m extends DeleteGraphState {
        public DeleteGraphState_5m() {
            super("data_5m.nt");
        }
    }

    private static void deleteGraphTest(DeleteGraphState state) {
        state.getDrafter().deleteGraph(state.getDraftset(), state.graphToDelete());
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteGraphTest_5k(DeleteGraphState_5k state) {
        deleteGraphTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteGraphTest_50k(DeleteGraphState_50k state) {
        deleteGraphTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteGraphTest_500k(DeleteGraphState_500k state) {
        deleteGraphTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteGraphTest_5m(DeleteGraphState_5m state) {
        deleteGraphTest(state);
    }
}
