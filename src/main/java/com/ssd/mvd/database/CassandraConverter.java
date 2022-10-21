package com.ssd.mvd.database;

import java.lang.reflect.Field;
import java.util.stream.Stream;
import lombok.Data;
import java.util.*;

@Data
public class CassandraConverter {
    private String result;

    private static CassandraConverter cassandraConverter = new CassandraConverter();

    public static CassandraConverter getInstance() { return cassandraConverter; }

    public Stream< Field > getFields ( Class object ) { return Arrays.stream(
                    object.getDeclaredFields() )
            .toList()
            .stream(); }

    public String getALlNames ( Class object ) {
        StringBuilder result = new StringBuilder( "( " );
        this.getFields( object )
            .forEach( field -> result.append( field.getName() ).append( ", " ) );
        return result.substring( 0, result.length() - 2 ) + " )"; }

    public String convertMapToCassandra ( Map< String, String > listOfTasks ) {
        result = "{";
        listOfTasks.keySet().forEach( s -> result += "'" + s + "' : '" + listOfTasks.get( s ) + "', " );
        return result.length() == 1 ? result + "}" : result.substring( 0, result.length() - 2 ) + "}"; }

    public String convertClassToCassandra ( Class object ) {
        StringBuilder result = new StringBuilder( "( " );
        this.getFields( object )
                .filter(
                        field -> field.getType().equals( String.class )
                                ^ field.getType().equals( Integer.class )
                                ^ field.getType().equals( Double.class )
                                ^ field.getType().equals( UUID.class )
                                ^ field.getType().equals( Long.class )
                                ^ field.getType().equals( Date.class )
                                ^ field.getType().equals( Boolean.class ) )
                .forEach( field -> {
                    result.append( field.getName() );
                    if ( field.getType().equals( String.class ) ) result.append( " text, " );
                    else if ( field.getType().equals( UUID.class ) ) result.append( " uuid, " );
                    else if ( field.getType().equals( Long.class ) ) result.append( " bigint, " );
                    else if ( field.getType().equals( Integer.class ) ) result.append( " int, " );
                    else if ( field.getType().equals( Double.class ) ) result.append( " double, " );
                    else if ( field.getType().equals( Date.class ) ) result.append( " timestamp, " );
                    else if ( field.getType().equals( Boolean.class ) ) result.append( " boolean, " ); } );
        return result.substring( 0, result.toString().length() - 2 ); }

    public String convertListToCassandra ( List< UUID > list ) {
        result = "[";
        list.forEach( s -> result += s + ", " );
        return result.length() == 1 ? result + "]" : result.substring( 0, result.length() - 2 ) + "]"; }
}
