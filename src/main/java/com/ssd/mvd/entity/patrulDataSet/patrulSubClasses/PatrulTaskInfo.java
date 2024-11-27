package com.ssd.mvd.entity.patrulDataSet.patrulSubClasses;

import com.ssd.mvd.annotations.entity.field.FieldAnnotation;
import com.ssd.mvd.annotations.entity.method.MethodsAnnotations;

import com.ssd.mvd.annotations.entity.object.EntityAnnotations;
import com.ssd.mvd.annotations.entity.object.EntityConstructorAnnotation;

import com.ssd.mvd.constants.Status;
import com.ssd.mvd.constants.cassandra.CassandraTables;
import com.ssd.mvd.constants.cassandra.CassandraDataTypes;

import com.ssd.mvd.inspectors.AnnotationInspector;
import com.ssd.mvd.inspectors.dataTypesInpectors.StringOperations;
import com.ssd.mvd.interfaces.entity.ObjectFromRowConvertInterface;

@EntityAnnotations( name = "PatrulTaskInfo", isSubClass = true, tableName = CassandraTables.PATRUL_TASK_DATA )
public final class PatrulTaskInfo
        extends StringOperations
        implements ObjectFromRowConvertInterface< PatrulTaskInfo > {
    public String getTaskId() {
        return this.taskId;
    }

    public Status getStatus() {
        return this.status;
    }

    @MethodsAnnotations(
            name = "taskId",
            withoutParams = false,
            isReturnEntity = false
    )
    public void setTaskId( final String taskId ) {
        this.taskId = taskId;
    }

    @MethodsAnnotations(
            name = "status",
            withoutParams = false,
            isReturnEntity = false,
            acceptEntityType = CassandraDataTypes.STATUS
    )
    public void setStatus( final Status status ) {
        this.status = status;
    }

    @FieldAnnotation( name = "taskId", hasToBeJoinedWithAstrix = true )
    private String taskId;

    @FieldAnnotation(
            name = "status",
            comment = "busy, free by default, available or not available",
            hasToBeJoinedWithAstrix = true
    )
    private Status status;

    private PatrulTaskInfo () {}

    @EntityConstructorAnnotation
    public <T> PatrulTaskInfo ( @lombok.NonNull final Class<T> instance ) {
        AnnotationInspector.checkCallerPermission( instance, PatrulTaskInfo.class );
    }

    @Override
    @lombok.NonNull
    public PatrulTaskInfo generate () {
        return new PatrulTaskInfo();
    }

    @Override
    @lombok.NonNull
    @org.jetbrains.annotations.Contract( value = "_ -> fail" )
    public PatrulTaskInfo generate( @lombok.NonNull final com.datastax.driver.core.GettableData gettableData ) {
        return AnnotationInspector.fillEntityParams( this, gettableData );
    }
}
