package com.benaether.rxon.core;

import com.benaether.rxon.scopes.Done;
import java.util.List;

/**
 * Internal exception used to carry context and compensation stack during pipeline failure.
 */
final class PipelineException extends RuntimeException {
    private final Object value;
    private final List<Work<Done>> compensationStack;

    PipelineException(Throwable cause, Object value, List<Work<Done>> compensationStack) {
        super(cause);
        this.value = value;
        this.compensationStack = compensationStack;
    }

    Object getValue() { return value; }
    List<Work<Done>> getCompensationStack() { return compensationStack; }
}
