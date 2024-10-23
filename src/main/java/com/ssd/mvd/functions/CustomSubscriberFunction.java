package com.ssd.mvd.functions;

@FunctionalInterface
public interface CustomSubscriberFunction<T> {
    void complete ( @lombok.NonNull final T value );
}
