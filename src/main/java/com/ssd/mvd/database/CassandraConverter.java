package com.ssd.mvd.database;

import com.ssd.mvd.constants.CassandraDataTypes;
import com.ssd.mvd.inspectors.LogInspector;
import java.lang.reflect.Field;

import java.util.function.Function;
import java.util.stream.Stream;
import java.util.*;

public class CassandraConverter extends LogInspector {
    protected CassandraConverter () {}

    private final Function< Class, CassandraDataTypes > getCorrectDataType = type -> {
        if ( type.equals( String.class ) || type.equals( Enum.class ) ) {
            return CassandraDataTypes.TEXT;
        }
        else if ( type.equals( UUID.class ) ) {
            return CassandraDataTypes.UUID;
        }
        else if ( type.equals( Long.class ) || type.equals( long.class ) ) {
            return CassandraDataTypes.BIGINT;
        }
        else if ( type.equals( Integer.class ) || type.equals( int.class ) ) {
            return CassandraDataTypes.INT;
        }
        else if ( type.equals( Double.class ) || type.equals( double.class ) ) {
            return CassandraDataTypes.DOUBLE;
        }
        else if ( type.equals( Date.class ) ) {
            return CassandraDataTypes.TIMESTAMP;
        }
        else if ( type.equals( byte.class ) ) {
            return CassandraDataTypes.TINYINT;
        }
        else {
            return CassandraDataTypes.BOOLEAN;
        }
    };

    protected final Function< Class, String > convertClassToCassandra = object -> {
        final StringBuilder result = super.newStringBuilder( "( " );
        this.getFields.apply( object )
                .filter( field -> field.getType().equals( String.class )
                        ^ field.getType().equals( Integer.class )
                        ^ field.getType().equals( Double.class )
                        ^ field.getType().equals( byte.class )
                        ^ field.getType().equals( UUID.class )
                        ^ field.getType().equals( Long.class )
                        ^ field.getType().equals( Date.class )
                        ^ field.getType().equals( Boolean.class ) )
                .forEach( field -> result
                        .append( field.getName() )
                        .append( " " )
                        .append( this.getCorrectDataType.apply( field.getType() ) )
                        .append( ", " ) );

        return result.substring( 0, result.toString().length() - 2 );
    };

    protected final Function< Class, String > getALlParamsNamesForClass = object -> {
        final StringBuilder result = super.newStringBuilder( "" );
        this.getFields.apply( object ).forEach( field -> result.append( field.getName() ).append( ", " ) );
        return super.joinTextWithCorrectCollectionEnding(
                result.substring( 0, result.length() - 2 ),
                CassandraDataTypes.BOOLEAN
        );
    };

    private final Function< Class, Stream< Field > > getFields = object -> Arrays.stream( object.getDeclaredFields() ).toList().stream();
}
