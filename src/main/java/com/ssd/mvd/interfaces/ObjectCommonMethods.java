package com.ssd.mvd.interfaces;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.UDTValue;

public interface ObjectCommonMethods< T > extends EntityToCassandraConverter {
    default T generate ( final UDTValue udtValue ) {
        return null;
    };

    default T generate () {
        return null;
    }

    default T generate ( final Row row ) {
        return null;
    }

    default UDTValue fillUdtByEntityParams ( final UDTValue udtValue ) {
        return null;
    };
}
