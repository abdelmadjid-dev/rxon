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

package com.benaether.rxon.core;

import com.benaether.rxon.scopes.Done;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Internal record to carry state through the compiled pipeline.
 * Context is treated as Object internally to support type-changing transitions.
 */
record PipelineResult<T>(
    T value,
    List<Work<Done>> compensationStack,
    boolean terminated
) {
    static <T> PipelineResult<T> of(T value) {
        return new PipelineResult<>(value, Collections.emptyList(), false);
    }

    static <T> PipelineResult<T> of(T value, List<Work<Done>> compensationStack) {
        return new PipelineResult<>(value, Collections.unmodifiableList(compensationStack), false);
    }

    static <T> PipelineResult<T> terminated(T value, List<Work<Done>> compensationStack) {
        return new PipelineResult<>(value, Collections.unmodifiableList(compensationStack), true);
    }

    PipelineResult<T> withValue(T newValue) {
        return new PipelineResult<>(newValue, compensationStack, terminated);
    }

    PipelineResult<T> pushCompensation(Work<Done> compensation) {
        List<Work<Done>> newStack = new ArrayList<>(compensationStack);
        newStack.add(0, compensation);
        return new PipelineResult<>(value, Collections.unmodifiableList(newStack), terminated);
    }

    PipelineResult<T> mergeCompensations(List<Work<Done>> otherStack) {
        if (otherStack.isEmpty()) return this;
        List<Work<Done>> newStack = new ArrayList<>(otherStack);
        newStack.addAll(compensationStack);
        return new PipelineResult<>(value, Collections.unmodifiableList(newStack), terminated);
    }
}
