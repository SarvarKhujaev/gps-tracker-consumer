package com.ssd.mvd.annotations;

import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

@Target( value = ElementType.TYPE )
@Retention( value = RetentionPolicy.RUNTIME )
public @interface EntityAnnotations {
    String name();

    boolean canTouch() default true;
    boolean isReadable() default true;
    boolean isSubClass() default false;

    String[] primaryKeys() default { "uuid" };
}
