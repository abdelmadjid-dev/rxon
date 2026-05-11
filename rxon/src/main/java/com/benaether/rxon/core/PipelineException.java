package com.benaether.rxon.core;

import com.benaether.rxon.scopes.Done;
import java.util.List;

/**
 * Internal exception used to carry context and compensation stack during pipeline failure.
 */
final class PipelineException extends RuntimeException {
    private final Object context;
    private final List<Work<Done, ?>> compensationStack;

    PipelineException(Throwable cause, Object context, List<Work<Done, ?>> compensationStack) {
        super(cause);
        this.context = context;
        this.compensationStack = compensationStack;
    }

    Object getContext() { return context; }
    List<Work<Done, ?>> getCompensationStack() { return compensationStack; }
}
