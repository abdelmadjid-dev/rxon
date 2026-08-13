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

import java.lang.reflect.Constructor;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Utility to clean up stack traces by stripping RxJavaAssemblyException and wrapper exceptions from Throwable trees.
 * Fully environment-agnostic and relies purely on standard Java Throwable APIs.
 */
public final class StackTraceCleaner {

    private static final Set<String> WRAPPER_EXCEPTIONS = new HashSet<>();
    static {
        WRAPPER_EXCEPTIONS.add("io.reactivex.rxjava3.exceptions.OnErrorNotImplementedException");
        WRAPPER_EXCEPTIONS.add("io.reactivex.rxjava3.exceptions.UndeliverableException");
        WRAPPER_EXCEPTIONS.add("io.reactivex.rxjava3.exceptions.CompositeException");
    }

    private StackTraceCleaner() {}

    /**
     * Cleans the stack trace by removing RxJavaAssemblyException and wrapper exceptions from the Throwable tree.
     * @param throwable the throwable to clean
     * @return a cleaned version of the throwable
     */
    public static Throwable clean(Throwable throwable) {
        if (throwable == null) return null;

        if (throwable.getClass().getName().contains("CompositeException")) {
            try {
                java.lang.reflect.Method getExceptions = throwable.getClass().getMethod("getExceptions");
                @SuppressWarnings("unchecked")
                List<Throwable> exceptions = (List<Throwable>) getExceptions.invoke(throwable);
                if (exceptions != null && !exceptions.isEmpty()) {
                    for (Throwable e : exceptions) {
                        Throwable cleaned = clean(e);
                        if (cleaned != null && !isNoiseException(cleaned)) {
                            return cleaned;
                        }
                    }
                }
            } catch (Exception ignored) {}
        }

        if (isNoiseException(throwable)) {
            Throwable origin = findOrigin(throwable.getCause());
            return clean(origin);
        }

        Throwable directCause = throwable.getCause();
        Throwable originCause = findOrigin(directCause);
        Throwable cleanedCause = clean(originCause);

        if (directCause != cleanedCause) {
            return createCleanCopy(throwable, cleanedCause);
        }

        return throwable;
    }

    private static Throwable createCleanCopy(Throwable original, Throwable newCause) {
        if (original == null) return null;

        Throwable copy = tryInstantiateCopy(original, newCause);
        if (copy == null) {
            copy = new RuntimeException(original.getMessage() != null ? original.getMessage() : original.toString(), newCause);
        }

        copy.setStackTrace(original.getStackTrace());

        for (Throwable suppressed : original.getSuppressed()) {
            Throwable cleanedSuppressed = clean(suppressed);
            if (cleanedSuppressed != null) {
                copy.addSuppressed(cleanedSuppressed);
            }
        }

        return copy;
    }

    private static Throwable tryInstantiateCopy(Throwable original, Throwable newCause) {
        Class<?> clazz = original.getClass();
        String msg = original.getMessage();

        try {
            Constructor<?> ctor = clazz.getConstructor(String.class, Throwable.class);
            return (Throwable) ctor.newInstance(msg, newCause);
        } catch (Throwable ignored) {}

        try {
            Constructor<?> ctor = clazz.getConstructor(Throwable.class);
            return (Throwable) ctor.newInstance(newCause);
        } catch (Throwable ignored) {}

        if (newCause == null) {
            try {
                Constructor<?> ctor = clazz.getConstructor(String.class);
                return (Throwable) ctor.newInstance(msg);
            } catch (Throwable ignored) {}

            try {
                Constructor<?> ctor = clazz.getConstructor();
                return (Throwable) ctor.newInstance();
            } catch (Throwable ignored) {}
        }

        return null;
    }

    private static Throwable findOrigin(Throwable t) {
        Throwable current = t;
        while (current != null && isNoiseException(current)) {
            Throwable cause = current.getCause();
            if (cause == null || cause == current) {
                current = null;
                break;
            }
            current = cause;
        }
        return current;
    }

    private static boolean isNoiseException(Throwable t) {
        if (t == null) return false;
        String name = t.getClass().getName();
        return WRAPPER_EXCEPTIONS.contains(name) 
                || name.contains("AssemblyException") 
                || name.contains("OnAssemblyException");
    }
}
