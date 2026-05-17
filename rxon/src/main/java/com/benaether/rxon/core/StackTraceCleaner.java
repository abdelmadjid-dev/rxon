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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Utility to clean up stack traces by removing internal library frames and system noise.
 * Designed to work in both Android and pure Java environments.
 */
public final class StackTraceCleaner {
    
    private static final String[] NOISE_PACKAGES = {
            "io.reactivex.rxjava3",
            "retrofit2",
            "okhttp3",
            "hu.akarnokd.rxjava3.debug",
            "com.benaether.rxon.core",
            "com.benaether.rxon.rx",
            "java.util.concurrent",
            "java.lang.Thread",
            "java.lang.reflect.Method",
            "dalvik.system",
            "android.os",
            "android.app",
            "android.view",
            "androidx.lifecycle",
            "androidx.fragment",
            "androidx.arch.core",
            "com.android.internal"
    };

    private static final Set<String> WRAPPER_EXCEPTIONS = new HashSet<>();
    static {
        WRAPPER_EXCEPTIONS.add("io.reactivex.rxjava3.exceptions.OnErrorNotImplementedException");
        WRAPPER_EXCEPTIONS.add("io.reactivex.rxjava3.exceptions.UndeliverableException");
        WRAPPER_EXCEPTIONS.add("io.reactivex.rxjava3.exceptions.CompositeException");
    }

    private StackTraceCleaner() {}

    /**
     * Cleans the stack trace and prunes the exception chain to show only valuable information.
     * @param throwable the throwable to clean
     * @return a cleaned version of the throwable
     */
    public static Throwable clean(Throwable throwable) {
        if (throwable == null) return null;

        // 1. Process CompositeExceptions first (they carry multiple chains)
        if (throwable.getClass().getName().contains("CompositeException")) {
            try {
                java.lang.reflect.Method getExceptions = throwable.getClass().getMethod("getExceptions");
                @SuppressWarnings("unchecked")
                List<Throwable> exceptions = (List<Throwable>) getExceptions.invoke(throwable);
                if (exceptions != null) {
                    for (Throwable e : exceptions) {
                        clean(e);
                    }
                }
            } catch (Exception ignored) {}
        }

        // 2. Prune the cause chain to remove redundant wrappers
        Throwable root = pruneChain(throwable);

        // 3. Clean frames for each throwable in the pruned chain
        Throwable current = root;
        while (current != null) {
            cleanFrames(current);
            Throwable cause = current.getCause();
            if (cause == current) break;
            current = cause;
        }

        return root;
    }

    private static Throwable pruneChain(Throwable throwable) {
        Throwable current = throwable;
        // If the top-level exception is just a noise wrapper, skip it
        while (current.getCause() != null && isNoiseException(current)) {
            current = current.getCause();
        }

        // Recursively prune causes
        Throwable cause = current.getCause();
        if (cause != null && cause != current) {
            // Collapse noise in the middle of the chain
            while (cause.getCause() != null && isNoiseException(cause)) {
                cause = cause.getCause();
            }
        }

        return current;
    }

    private static void cleanFrames(Throwable t) {
        StackTraceElement[] original = t.getStackTrace();
        List<StackTraceElement> cleaned = new ArrayList<>();

        for (StackTraceElement element : original) {
            if (!isNoiseFrame(element)) {
                cleaned.add(element);
            }
        }

        if (cleaned.size() < original.length) {
            t.setStackTrace(cleaned.toArray(new StackTraceElement[0]));
        }
    }

    private static boolean isNoiseException(Throwable t) {
        String name = t.getClass().getName();
        return WRAPPER_EXCEPTIONS.contains(name) || name.contains("OnAssemblyException");
    }

    private static boolean isNoiseFrame(StackTraceElement element) {
        String className = element.getClassName();
        
        // Always keep Work DSL entry points
        if (className.startsWith("com.benaether.rxon.core.Work")) {
            return false;
        }

        // Handle synthetic frames generically
        if (className.contains("$$ExternalSynthetic") || className.contains("$lambda$")) {
            // If the synthetic class is within our library's internal packages, filter it
            if (className.contains("com.benaether.rxon") && !className.contains("Work")) {
                return true;
            }
        }

        // If it's a known noise package, it's noise
        for (String pkg : NOISE_PACKAGES) {
            if (className.startsWith(pkg)) {
                return true;
            }
        }

        return false;
    }
}
