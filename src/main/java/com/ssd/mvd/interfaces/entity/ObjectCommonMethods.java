package com.ssd.mvd.interfaces.entity;

import com.datastax.driver.core.UDTValue;

@SuppressWarnings(
        value = "хранит методы для конвертации объекта при помощи UDT из БД"
)
public interface ObjectCommonMethods< T > extends ObjectFromRowConvertInterface<T> {
    T generate ( final UDTValue udtValue );
}
