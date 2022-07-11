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

import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;
import org.openjdk.jmh.annotations.Benchmark;
import clojure.java.api.Clojure;
import clojure.lang.IFn;

import java.io.File;

public class MyBenchmark {

    private static void require(String nsName) {
        IFn require = Clojure.var("clojure.core", "require");
        require.invoke(Clojure.read(nsName));
    }

    private static IFn keyword(String value) {
        return (IFn)Clojure.var("clojure.core", "keyword").invoke(value);
    }

    private static Object getManager() {
        SPARQLRepository repo = new SPARQLRepository("http://localhost:5820/drafter-test-db/query", "http://localhost:5820/drafter-test-db/update");
        require("drafter.manager");
        IFn createFn = Clojure.var("drafter.manager", "create-manager");
        return createFn.invoke(repo);
    }

    private static Object getAppendStateMachine() {
        require("drafter.feature.draftset-data.append");
        IFn smFn = Clojure.var("drafter.feature.draftset-data.append", "append-state-machine");
        return smFn.invoke();
    }

    private static Object appendJobContext(Object manager, Object draftsetRef) {
        require("drafter.feature.draftset-data.common");
        IFn contextFn = Clojure.var("drafter.feature.draftset-data.common", "job-context");
        return contextFn.invoke(manager, draftsetRef);
    }

    private static Object createUser(String email, String roleName) {
        require("drafter.user");
        IFn createFn = Clojure.var("drafter.user", "create-authenticated-user");
        return createFn.invoke(email, keyword(roleName));
    }

    private static Object createDraftset(Object manager) {
        // TODO: add accessor function
        Object repo = keyword("backend").invoke(manager);
        Object user = createUser("publisher@swirrl.com", "publisher");

        require("drafter.backend.draftset.operations");
        IFn createFn = Clojure.var("drafter.backend.draftset.operations", "create-draftset!");

        return createFn.invoke(repo, user);
    }

    @Benchmark
    public void appendTest() {
        Object manager = getManager();
        Object sm = getAppendStateMachine();

        Object draftset = createDraftset(manager);

        Object liveToDraftMapping = Clojure.var("clojure.core", "hash-map").invoke();
        Object source = new File("../drafter/test/resources/drafter/backend/draftset/operations_test/all_data_queries.trig");
        Object context = appendJobContext(manager, draftset);

        IFn execFn = Clojure.var("drafter.feature.draftset-data.common", "exec-state-machine-sync");
        execFn.invoke(sm, liveToDraftMapping, source, context);
    }

}
