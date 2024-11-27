package com.ssd.mvd.interfaces.entity;

@SuppressWarnings(
        value = "хранит методы для конвертации объекта при помощи ROW из БД"
)
public interface ObjectFromRowConvertInterface<T> extends EntityToCassandraConverter {
    @lombok.NonNull
    @org.jetbrains.annotations.Contract( value = "_ -> fail" )
    T generate ( final com.datastax.driver.core.GettableData gettableData );

    @lombok.NonNull
    default ObjectFromRowConvertInterface<T> generate () {
        return null;
    }
}
