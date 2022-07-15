/*
 * Copyright (c) 2014, Oracle America, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 *  * Neither the name of Oracle nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.swirrl;

import org.openjdk.jmh.annotations.*;

public class AppendBenchmark {
    @State(Scope.Thread)
    public static class DraftState {
        private Drafter drafter;
        private Draftset draftset;

        @Setup(Level.Iteration)
        public void setup() {
            this.drafter = Drafter.create();
            this.draftset = this.drafter.createDraft(User.publisher());
        }

        @TearDown(Level.Iteration)
        public void tearDown() {
            this.drafter.dropDb();
        }

        public Drafter getDrafter() { return this.drafter; }
        public Draftset getDraftset() { return this.draftset; }
    }

    private static void appendOnlyTest(DraftState state, String fileName) throws Exception {
        state.getDrafter().append(state.getDraftset(), fileName);
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
