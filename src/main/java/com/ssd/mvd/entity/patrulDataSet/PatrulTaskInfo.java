package com.ssd.mvd.entity.patrulDataSet;

import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.Row;
import com.ssd.mvd.constants.Status;
import java.util.Map;

public final class PatrulTaskInfo {
    public static <T> PatrulTaskInfo generate ( final T object ) {
        return object instanceof Row
                ? new PatrulTaskInfo( (Row) object )
                : new PatrulTaskInfo( (UDTValue) object );
    }

    private PatrulTaskInfo( final Row row ) {
        this.setStatus( Status.valueOf( row.getString( "status" ) ) );
        this.setListOfTasks( row.getMap( "listOfTasks", String.class, String.class ) );
    }

    private PatrulTaskInfo( final UDTValue udtValue ) {
        this.setStatus( Status.valueOf( udtValue.getString( "status" ) ) );
        this.setListOfTasks( udtValue.getMap( "listOfTasks", String.class, String.class ) );
    }

    public String getTaskId() {
        return this.taskId;
    }

    public void setTaskId( final String taskId ) {
        this.taskId = taskId;
    }

    public Status getStatus() {
        return this.status;
    }

    public void setStatus( final Status status ) {
        this.status = status;
    }

    public Map<String, String> getListOfTasks() {
        return this.listOfTasks;
    }

    public void setListOfTasks( final Map<String, String> listOfTasks ) {
        this.listOfTasks = listOfTasks;
    }

    private String taskId;
    // busy, free by default, available or not available
    private Status status;
    // the list which will store ids of all tasks which have been completed by Patrul
    private Map< String, String > listOfTasks;
}
