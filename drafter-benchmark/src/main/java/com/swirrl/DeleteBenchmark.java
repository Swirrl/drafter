package com.swirrl;

import org.openjdk.jmh.annotations.*;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

public class DeleteBenchmark {

    public static class DeleteState_5k extends DeleteState {
        public DeleteState_5k() {
            super("data_5k.nt", "data_5k_delete.nt");
        }
    }

    public static class DeleteState_50k extends DeleteState {
        public DeleteState_50k() {
            super("data_50k.nt", "data_50k_delete.nt");
        }
    }

    public static class DeleteState_500k extends DeleteState {
        public DeleteState_500k() {
            super("data_500k.nt", "data_500k_delete.nt");
        }
    }

    public static class DeleteState_5m extends DeleteState {
        public DeleteState_5m() {
            super("data_5m.nt", "data_5m_delete.nt");
        }
    }

    @State(Scope.Thread)
    public static class DeleteState {
        //private final int inputSize;
        private Drafter drafter;
        private Draftset draftset;
        private final File dataFile;
        private final File deletionFile;

        protected DeleteState(String dataFileName, String deletionFileName) {
            this.dataFile = Util.resolveDataFile(dataFileName);
            this.deletionFile = Util.resolveDataFile(deletionFileName);
        }

        @Setup(Level.Iteration)
        public void setup() throws Exception {
            this.drafter = Drafter.create();
            this.draftset = this.drafter.createDraft(User.publisher());
            this.drafter.append(this.draftset, this.dataFile);
        }

        private static Path writeDeletionFile(File sourceFile, int inputSize) throws Exception {
            Path tempFile = Files.createTempFile("drafter_bench", ".nt");
            try {
                int toDelete = inputSize / 2;
                Util.extractStatements(sourceFile, tempFile, toDelete);
                return tempFile;
            } catch (Exception ex) {
                tempFile.toFile().delete();
                throw ex;
            }
        }

        @TearDown(Level.Iteration)
        public void tearDown() {
            //this.tempFile.toFile().delete();
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
    public void deleteTest_5k(DeleteState_5k state) throws Exception {
        deleteTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteTest_50k(DeleteState_50k state) throws Exception {
        deleteTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteTest_500k(DeleteState_500k state) throws Exception {
        deleteTest(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deleteTest_5m(DeleteState_5m state) throws Exception {
        deleteTest(state);
    }
}
