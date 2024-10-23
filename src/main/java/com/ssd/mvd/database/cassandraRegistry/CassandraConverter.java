package com.ssd.mvd.database.cassandraRegistry;

import com.ssd.mvd.annotations.EntityConstructorAnnotation;
import com.ssd.mvd.interfaces.EntityToCassandraConverter;
import com.ssd.mvd.constants.CassandraDataTypes;

import com.ssd.mvd.inspectors.AnnotationInspector;
import com.ssd.mvd.inspectors.UuidInspector;
import com.ssd.mvd.inspectors.LogInspector;

import java.util.Date;
import java.util.UUID;

public class CassandraConverter extends AnnotationInspector {
    protected CassandraConverter () {}

    @EntityConstructorAnnotation(
            permission = {
                    LogInspector.class
            }
    )
    protected <T extends UuidInspector> CassandraConverter (@lombok.NonNull final Class<T> instance ) {
        super( CassandraConverter.class );

        AnnotationInspector.checkCallerPermission( instance, CassandraConverter.class );
        AnnotationInspector.checkAnnotationIsImmutable( CassandraConverter.class );
    }

    @SuppressWarnings(
            value = """
                    для экземпляра Java класса конвертирует каждый его параметр,
                    в Cassandra подобный тип данных
                    """
    )
    @lombok.NonNull
    @lombok.Synchronized
    private synchronized CassandraDataTypes getCorrectDataType (
            @lombok.NonNull final Class<?> type
    ) {
        if ( type.equals( String.class ) || type.isEnum() ) {
            return CassandraDataTypes.TEXT;
        }
        else if ( type.equals( UUID.class ) ) {
            return CassandraDataTypes.UUID;
        }
        else if ( type.equals( long.class ) ) {
            return CassandraDataTypes.BIGINT;
        }
        else if ( type.equals( int.class ) ) {
            return CassandraDataTypes.INT;
        }
        else if ( type.equals( double.class ) ) {
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
    }

    @SuppressWarnings(
            value = """
                    для экземпляра Java класса конвертирует каждый его параметр,
                    в Cassandra подобный тип данных
                    """
    )
    @lombok.NonNull
    @lombok.Synchronized
    protected final synchronized String convertClassToCassandra (
            @lombok.NonNull final Class<? extends EntityToCassandraConverter> object
    ) {
        final StringBuilder result = newStringBuilder( "( " );

        getFields( object )
                .filter( field -> field.getType().equals( String.class )
                        ^ field.getType().equals( int.class )
                        ^ field.getType().equals( double.class )
                        ^ field.getType().equals( byte.class )
                        ^ field.getType().equals( UUID.class )
                        ^ field.getType().equals( long.class )
                        ^ field.getType().equals( Date.class )
                        ^ field.getType().isEnum()
                        ^ field.getType().equals( boolean.class ) )
                .forEach( field -> result
                        .append( field.getName() )
                        .append( SPACE )
                        .append( this.getCorrectDataType( field.getType() ) )
                        .append( SPACE_WITH_COMMA ) );

        return result.substring( 0, result.toString().length() - 2 );
    }

    @lombok.NonNull
    @lombok.Synchronized
    public static synchronized String getALlParamsNamesForClass (
            @lombok.NonNull final Class< ? extends EntityToCassandraConverter > object
    ) {
        final StringBuilder result = newStringBuilder( EMPTY );
        getFields( object ).forEach( field -> result.append( field.getName() ).append( SPACE_WITH_COMMA ) );
        return joinTextWithCorrectCollectionEnding(
                result.substring( 0, result.length() - 2 ),
                CassandraDataTypes.BOOLEAN
        );
    }

    @SuppressWarnings(
            value = "принимает экземпляра Class и возвращает список названий всех его параметров"
    )
    @lombok.NonNull
    @lombok.Synchronized
    public static synchronized String getAllParamsNamesForClass (
            @lombok.NonNull final Class<? extends EntityToCassandraConverter> object
    ) {
        final StringBuilder result = newStringBuilder( EMPTY );

        getFields( object ).forEach( field -> result.append( field.getName() ).append( SPACE_WITH_COMMA ) );

        return joinTextWithCorrectCollectionEnding(
                result.substring( 0, result.length() - 2 ),
                CassandraDataTypes.BOOLEAN
        );
    }
}
