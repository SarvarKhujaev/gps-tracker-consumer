package com.ssd.mvd.entity.patrulDataSet.patrulSubClasses;

import java.util.Map;
import com.datastax.driver.core.GettableData;

import com.ssd.mvd.annotations.EntityAnnotations;
import com.ssd.mvd.annotations.MethodsAnnotations;

import com.ssd.mvd.constants.Status;
import com.ssd.mvd.constants.CassandraDataTypes;

import com.ssd.mvd.inspectors.DataValidateInspector;
import com.ssd.mvd.interfaces.ObjectFromRowConvertInterface;

@EntityAnnotations( name = "PatrulTaskInfo", isSubClass = true )
public final class PatrulTaskInfo extends DataValidateInspector implements ObjectFromRowConvertInterface< PatrulTaskInfo > {
    public String getTaskId() {
        return this.taskId;
    }

    public Status getStatus() {
        return this.status;
    }

    public Map< String, String > getListOfTasks() {
        return this.listOfTasks;
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

    public void setListOfTasks( final Map< String, String > listOfTasks ) {
        this.listOfTasks = listOfTasks;
    }

    private String taskId;
    // busy, free by default, available or not available
    private Status status;
    // the list which will store ids of all tasks which have been completed by Patrul
    private Map< String, String > listOfTasks = super.newMap();

    public PatrulTaskInfo () {}

    @Override
    @lombok.NonNull
    public PatrulTaskInfo generate() {
        return new PatrulTaskInfo();
    }

    @Override
    @lombok.NonNull
    public PatrulTaskInfo generate( @lombok.NonNull final GettableData gettableData ) {
        checkAndSetParams(
                gettableData,
                row1 -> {
                    this.setListOfTasks( gettableData.getMap( "listOfTasks", String.class, String.class ) );
                    this.setStatus( Status.valueOf( gettableData.getString( "status" ) ) );
                }
        );

        return this;
    }
}
