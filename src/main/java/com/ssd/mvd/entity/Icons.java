package com.ssd.mvd.entity;

import com.ssd.mvd.inspectors.AnnotationInspector;
import com.ssd.mvd.constants.cassandra.CassandraTables;
import com.ssd.mvd.interfaces.entity.ObjectFromRowConvertInterface;

import com.ssd.mvd.annotations.entity.field.FieldAnnotation;
import com.ssd.mvd.annotations.entity.method.MethodsAnnotations;

import com.ssd.mvd.annotations.entity.object.EntityAnnotations;
import com.ssd.mvd.annotations.entity.object.EntityConstructorAnnotation;

@EntityAnnotations(
        name = "Icons",
        comment = "Данные о патрульных машинах",
        tableName = CassandraTables.ICONS
)
public final class Icons implements ObjectFromRowConvertInterface< Icons > {
    public String getIcon1() {
        return this.icon1;
    }

    public String getIcon2() {
        return this.icon2;
    }

    @MethodsAnnotations(
            name = "icon1",
            withoutParams = false,
            isReturnEntity = false
    )
    public void setIcon1 ( final String icon1 ) {
        this.icon1 = icon1;
    }

    @MethodsAnnotations(
            name = "icon2",
            withoutParams = false,
            isReturnEntity = false
    )
    public void setIcon2 ( final String icon2 ) {
        this.icon2 = icon2;
    }

    @FieldAnnotation(
            name = "icon1",
            mightBeNull = false,
            hasToBeJoinedWithAstrix = true
    )
    private String icon1;

    @FieldAnnotation(
            name = "icon2",
            mightBeNull = false,
            hasToBeJoinedWithAstrix = true
    )
    private String icon2;

    private Icons () {}

    @EntityConstructorAnnotation
    public <T> Icons ( final Class<T> instance ) {
        AnnotationInspector.checkCallerPermission( instance, Icons.class );
    }

    @Override
    @lombok.NonNull
    public Icons generate( @lombok.NonNull final com.datastax.driver.core.GettableData gettableData ) {
        return AnnotationInspector.fillEntityParams( this, gettableData );
    }

    @Override
    @lombok.NonNull
    public Icons generate() {
        return new Icons();
    }
}