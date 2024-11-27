package com.ssd.mvd.annotations;

import java.lang.annotation.*;

@Target( value = ElementType.TYPE_USE )
@Retention( value = RetentionPolicy.CLASS )
@Documented
@SuppressWarnings(
        value = "будет содержать ссылки на доки для разных компонентов"
)
public @interface LinksToDocs {
    String[] links();
}
