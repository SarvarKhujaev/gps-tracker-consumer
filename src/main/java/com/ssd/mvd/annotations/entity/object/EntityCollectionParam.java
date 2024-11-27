package com.ssd.mvd.annotations.entity.object;

import com.ssd.mvd.constants.cassandra.CassandraDataTypes;
import java.lang.annotation.*;

@Target( value = ElementType.FIELD )
@Retention( value = RetentionPolicy.RUNTIME )
@Documented
public @interface EntityCollectionParam {
    String name();

    boolean isFrozen() default false;

    CassandraDataTypes collectionDataType() default CassandraDataTypes.LIST;
    CassandraDataTypes[] collectionContainingType() default { CassandraDataTypes.TEXT };
}
