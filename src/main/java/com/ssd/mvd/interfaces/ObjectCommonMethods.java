package com.ssd.mvd.interfaces;

import com.datastax.driver.core.UDTValue;

/*
хранит методы для конвертации объекта при помощи UDT из БД
*/
public interface ObjectCommonMethods< T > extends ObjectFromRowConvertInterface<T> {
    T generate ( final UDTValue udtValue );
}
