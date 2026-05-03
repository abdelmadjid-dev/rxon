/*
 * Copyright 2026 Abdelmadjid
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.benaether.rxon.scopes;

import com.benaether.rxon.core.Stream;
import com.benaether.rxon.core.Work;

public final class ScopedWorkflows {

    private ScopedWorkflows() {}

    // ===========================================================================================
    // GENERIC WORKFLOWS (USED BY Work)
    // ===========================================================================================

    @FunctionalInterface
    public interface Workflow<I, O> {
        Work<O> apply(I input);
    }

    @FunctionalInterface
    public interface Workflow0<O> {
        Work<O> apply();
    }

    // ===========================================================================================
    // STREAM WORKFLOWS (USED BY Stream)
    // ===========================================================================================

    @FunctionalInterface
    public interface StreamWorkflow<I, O> {
        Stream<O> apply(I input);
    }

    @FunctionalInterface
    public interface StreamWorkflow0<O> {
        Stream<O> apply();
    }
}

