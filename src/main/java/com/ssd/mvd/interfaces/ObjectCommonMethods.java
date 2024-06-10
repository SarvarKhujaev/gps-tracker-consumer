package com.ssd.mvd.interfaces;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.UDTValue;

public interface ObjectCommonMethods< T > extends EntityToCassandraConverter {
    T generate ( final UDTValue udtValue );

    default T generate () {
        return null;
    }

    default T generate ( final Row row ) {
        return null;
    }

    UDTValue fillUdtByEntityParams ( final UDTValue udtValue );
}
