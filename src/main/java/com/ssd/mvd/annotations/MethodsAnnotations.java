package com.ssd.mvd.annotations;

import com.ssd.mvd.constants.CassandraDataTypes;

import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

@Target( value = ElementType.METHOD )
@Retention( value = RetentionPolicy.RUNTIME )
public @interface MethodsAnnotations {
    String name();

    boolean canTouch() default true;
    boolean isPrimaryKey() default false;
    boolean withoutParams() default true;
    boolean isReturnEntity() default true;

    CassandraDataTypes acceptEntityType() default CassandraDataTypes.TEXT;
}
