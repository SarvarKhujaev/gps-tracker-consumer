package com.ssd.mvd.annotations.entity.field;

import java.lang.annotation.*;

@Target( value = ElementType.FIELD )
@Retention( value = RetentionPolicy.RUNTIME )
@Documented
public @interface EntityIndex {
    String name();
}
