package com.ssd.mvd.entity.patrulDataSet.patrulSubClasses;

import java.util.Map;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.UDTValue;

import com.ssd.mvd.annotations.EntityAnnotations;
import com.ssd.mvd.annotations.MethodsAnnotations;
import com.ssd.mvd.constants.CassandraDataTypes;
import com.ssd.mvd.constants.Status;
import com.ssd.mvd.interfaces.ObjectCommonMethods;
import com.ssd.mvd.inspectors.DataValidateInspector;

@EntityAnnotations( name = "PatrulTaskInfo", isSubClass = true )
public final class PatrulTaskInfo extends DataValidateInspector implements ObjectCommonMethods< PatrulTaskInfo > {
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
    public PatrulTaskInfo generate() {
        return new PatrulTaskInfo();
    }

    @Override
    public PatrulTaskInfo generate( final Row row ) {
        super.checkAndSetParams(
                row,
                row1 -> {
                    this.setListOfTasks( row.getMap( "listOfTasks", String.class, String.class ) );
                    this.setStatus( Status.valueOf( row.getString( "status" ) ) );
                }
        );

        return this;
    }

    @Override
    public PatrulTaskInfo generate( final UDTValue udtValue ) {
        super.checkAndSetParams(
                udtValue,
                udtValue1 -> {
                    this.setListOfTasks( udtValue.getMap( "listOfTasks", String.class, String.class ) );
                    this.setStatus( Status.valueOf( udtValue.getString( "status" ) ) );
                }
        );

        return this;
    }
}
