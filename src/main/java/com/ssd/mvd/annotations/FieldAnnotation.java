package com.ssd.mvd.annotations;

import com.ssd.mvd.constants.CassandraDataTypes;
import com.ssd.mvd.inspectors.StringOperations;

import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

@Target( value = ElementType.FIELD )
@Retention( value = RetentionPolicy.RUNTIME )
public @interface FieldAnnotation {
    String name();
    String comment() default StringOperations.EMPTY;

    CassandraDataTypes cassandraType() default CassandraDataTypes.TEXT;

    boolean canTouch() default true;
    boolean isReadable() default true;
    boolean mightBeNull() default true;
    boolean isInteriorObject() default false;
    boolean hasToBeJoinedWithAstrix() default false;
}
