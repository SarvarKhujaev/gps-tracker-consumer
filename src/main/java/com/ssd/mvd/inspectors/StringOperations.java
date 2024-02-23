package com.ssd.mvd.inspectors;

import com.ssd.mvd.constants.CassandraDataTypes;
import com.ssd.mvd.constants.CassandraCommands;
import java.util.Date;

public class StringOperations {
    protected StringBuilder newStringBuilder () {
        return new StringBuilder( CassandraCommands.BEGIN_BATCH );
    }

    protected StringBuilder newStringBuilder ( final String s ) {
        return new StringBuilder( s );
    }

    protected String removeAllDotes ( final String s ) {
        return s.replaceAll( "'", "" );
    }

    /*
    принимает параметр для Cassandra, который является типом TEXТ,
    и добавляет в начало и конец апострафы
    */
    protected String joinWithAstrix ( final Object value ) {
        return "$$" + value + "$$";
    }

    /*
    принимает параметр для Cassandra, который является типом TIMESTAMP,
    и добавляет в начало и конец апострафы
    */
    protected String joinWithAstrix ( final Date date ) {
        return "'" + date.toInstant() + "'";
    }

    /*
        принимает параметр для Cassandra, который относиться к Collection,
        и добавляет в начало и конец (), {} или []
        в зависимости от типа коллекции
    */
    protected String joinTextWithCorrectCollectionEnding (
            final String textToJoin,
            final CassandraDataTypes cassandraDataTypes
    ) {
        return switch ( cassandraDataTypes ) {
            case MAP, SET -> "{" + textToJoin + "}";
            case LIST -> "[" + textToJoin + "]";
            default -> "(" + textToJoin + ")";
        };
    }
}
