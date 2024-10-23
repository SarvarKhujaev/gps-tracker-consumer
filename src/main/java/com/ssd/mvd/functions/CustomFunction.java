package com.ssd.mvd.functions;

@FunctionalInterface
public interface CustomFunction<T, R> {
    @lombok.NonNull
    @org.jetbrains.annotations.Contract( value = "_ -> _" )
    R check ( @lombok.NonNull final T value );
}
