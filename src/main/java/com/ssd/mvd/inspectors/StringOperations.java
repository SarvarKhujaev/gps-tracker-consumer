package com.ssd.mvd.inspectors;

import com.ssd.mvd.entity.patrulDataSet.PatrulFIOData;
import com.ssd.mvd.constants.CassandraDataTypes;
import com.ssd.mvd.constants.CassandraCommands;

import java.util.Date;
import java.util.UUID;

public class StringOperations {
    protected StringOperations () {}

    protected final synchronized StringBuilder newStringBuilder () {
        return new StringBuilder( CassandraCommands.BEGIN_BATCH );
    }

    protected final synchronized StringBuilder newStringBuilder (
            final String s
    ) {
        return new StringBuilder( s );
    }

    /*
    принимает параметр для Cassandra, который является типом TEXТ,
    и добавляет в начало и конец апострафы
    */
    protected final synchronized String joinWithAstrix (
            final Object value
    ) {
        return "$$" + value + "$$";
    }

    /*
    принимает параметр для Cassandra, который является типом TIMESTAMP,
    и добавляет в начало и конец апострафы
    */
    protected final synchronized String joinWithAstrix (
            final Date date
    ) {
        return "'" + date.toInstant() + "'";
    }

    /*
        принимает параметр для Cassandra, который относиться к Collection,
        и добавляет в начало и конец (), {} или []
        в зависимости от типа коллекции
    */
    protected final synchronized String joinTextWithCorrectCollectionEnding (
            final String textToJoin,
            final CassandraDataTypes cassandraDataTypes
    ) {
        return switch ( cassandraDataTypes ) {
            case MAP, SET -> "{" + textToJoin + "}";
            case LIST -> "[" + textToJoin + "]";
            default -> "(" + textToJoin + ")";
        };
    }

    protected final synchronized String generateID () {
        return "ID = '%s'".formatted( UUID.randomUUID() );
    }

    protected final synchronized String concatNames (
            final PatrulFIOData patrulFIOData
    ) {
        return String.join(
                " ",
                patrulFIOData.getName(),
                patrulFIOData.getSurname(),
                patrulFIOData.getFatherName()
        );
    }
}
