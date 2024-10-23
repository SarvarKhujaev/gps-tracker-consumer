package com.ssd.mvd.inspectors;

import com.ssd.mvd.entity.patrulDataSet.patrulSubClasses.PatrulFIOData;
import com.ssd.mvd.annotations.EntityConstructorAnnotation;

import com.ssd.mvd.constants.CassandraDataTypes;
import com.ssd.mvd.constants.CassandraCommands;

import java.util.Date;

@com.ssd.mvd.annotations.ImmutableEntityAnnotation
public class StringOperations extends UuidInspector {
    protected StringOperations () {
        super( StringOperations.class );
    }

    @EntityConstructorAnnotation( permission = CollectionsInspector.class )
    protected <T extends UuidInspector> StringOperations ( @lombok.NonNull final Class<T> instance ) {
        super( StringOperations.class );

        AnnotationInspector.checkCallerPermission( instance, StringOperations.class );
        AnnotationInspector.checkAnnotationIsImmutable( StringOperations.class );
    }

    public final static String DOT = ".";
    public final static String EMPTY = "";
    public final static String SPACE = " ";
    protected final static String SPACE_WITH_COMMA = ", ";
    protected final static String TASK_DETAILS_MESSAGE = "Your task details";
    protected final static String SPACE_WITH_DOUBLE_DOTS = " : ";

    public final static String AVRO_DATE_PATTERN = "^[0-9]{4}-[0-9]{2}-[0-9]{2}$";
    public final static String NULL_VALUE_IN_ASSERT = "NULL VALUE WAS SENT";

    protected final synchronized StringBuilder newStringBuilder () {
        return new StringBuilder( CassandraCommands.BEGIN_BATCH );
    }

    protected static synchronized StringBuilder newStringBuilder (
            final String s
    ) {
        return new StringBuilder( s );
    }

    @SuppressWarnings(
            value = """
                    принимает параметр для Cassandra, который является типом TEXТ,
                    и добавляет в начало и конец апострафы
                    """
    )
    @lombok.NonNull
    @lombok.Synchronized
    public static synchronized String joinWithAstrix ( @lombok.NonNull final Object value ) {
        /*
        принимает параметр для Cassandra, который является типом TIMESTAMP,
        и добавляет в начало и конец апострафы
        */
        return value instanceof Date ? "'" + ( (Date) value ).toInstant() + "'" : "$$" + value + "$$";
    }

    @SuppressWarnings(
            value = """
                    принимает параметр для Cassandra, который относиться к Collection,
                    и добавляет в начало и конец (), {} или []
                    в зависимости от типа коллекции
                    """
    )
    @lombok.NonNull
    @lombok.Synchronized
    protected static synchronized String joinTextWithCorrectCollectionEnding (
            final String textToJoin,
            final CassandraDataTypes cassandraDataTypes
    ) {
        return switch ( cassandraDataTypes ) {
            case MAP, SET -> "{" + textToJoin + "}";
            case LIST -> "[" + textToJoin + "]";
            default -> "(" + textToJoin + ")";
        };
    }

    @lombok.NonNull
    @lombok.Synchronized
    public static synchronized String generateID () {
        return "ID = %s".formatted( generateTimeBased() );
    }


    @lombok.NonNull
    @lombok.Synchronized
    protected final synchronized String concatNames (
            final PatrulFIOData patrulFIOData
    ) {
        return String.join(
                SPACE,
                patrulFIOData.getName(),
                patrulFIOData.getSurname(),
                patrulFIOData.getFatherName()
        );
    }
}
