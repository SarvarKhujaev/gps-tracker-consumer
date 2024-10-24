package com.ssd.mvd.database.cassandraRegistry;

import com.ssd.mvd.interfaces.DatabaseCommonMethods;
import com.ssd.mvd.inspectors.CollectionsInspector;
import com.ssd.mvd.constants.CassandraTables;

@SuppressWarnings(
        value = "создает все таблицы, типы, кодеки и пространство ключей"
)
@com.ssd.mvd.annotations.ImmutableEntityAnnotation
public sealed class CassandraTablesAndTypesRegister implements DatabaseCommonMethods permits CassandraTableRegistration {
    protected CassandraTablesAndTypesRegister() {
        this.createAllKeyspace();
    }

    @SuppressWarnings( value = "создаем все пространства в БД" )
    private void createAllKeyspace () {
        CollectionsInspector.analyze(
                CollectionsInspector.newList(
                        CassandraTables.TRACKERS,
                        CassandraTables.ESCORT
                ),
                keyspaceName -> this.completeCommand( keyspaceName.getCreateKeyspaceCommand() )
        );
    }
}
