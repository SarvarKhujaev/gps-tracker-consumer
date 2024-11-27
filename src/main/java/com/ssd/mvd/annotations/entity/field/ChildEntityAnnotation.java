package com.ssd.mvd.annotations.entity.field;

import com.ssd.mvd.interfaces.entity.EntityToCassandraConverter;
import java.lang.annotation.*;

@Target( value = ElementType.FIELD )
@Retention( value = RetentionPolicy.RUNTIME )
@Documented
@SuppressWarnings(
        value = """
                хранит в себе данные о том, с какой таблицей связанна текущая
                Связь происходит через уникальный параметр cущности
                """
)
public @interface ChildEntityAnnotation {
    @SuppressWarnings(
            value = """
                    название колонки через которую происходит связь,
                    указывается колонка основной таблицы
                    может содержать несколько колонок
                    """
    )
    String[] joiningColumns();

    @SuppressWarnings( value = "ссылка на основную таблицу" )
    Class< ? extends EntityToCassandraConverter> joinedTable();
}
