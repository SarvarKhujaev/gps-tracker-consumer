package com.ssd.mvd.database;

import com.ssd.mvd.inspectors.LogInspector;
import java.lang.reflect.Field;

import java.util.function.Function;
import java.util.stream.Stream;
import java.util.*;

public class CassandraConverter extends LogInspector {
    private final Function< Class, Stream< Field > > getFields = object -> Arrays.stream( object.getDeclaredFields() ).toList().stream();

    protected final Function< Class, String > convertClassToCassandra = object -> {
            final StringBuilder result = new StringBuilder( "( " );
            this.getFields.apply( object )
                    .filter( field -> field.getType().equals( String.class )
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
            return result.substring( 0, result.toString().length() - 2 ); };

    protected final Function< Class, String > getALlNames = object -> {
            final StringBuilder result = new StringBuilder( "( " );
            this.getFields.apply( object ).forEach( field -> result.append( field.getName() ).append( ", " ) );
            return result.substring( 0, result.length() - 2 ) + " )"; };
}
