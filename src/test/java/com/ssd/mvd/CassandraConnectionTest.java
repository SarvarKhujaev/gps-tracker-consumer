package com.ssd.mvd;

import com.ssd.mvd.database.CassandraDataControl;
import com.ssd.mvd.inspectors.EntitiesInstances;
import com.ssd.mvd.entity.patrulDataSet.Patrul;
import com.ssd.mvd.constants.CassandraTables;

import com.datastax.driver.core.utils.UUIDs;
import com.datastax.driver.core.Row;

import org.junit.jupiter.api.DisplayName;
import junit.framework.TestCase;

import java.text.MessageFormat;
import java.util.UUID;

/*
проверяем соединение с БД Cassandra
*/
public final class CassandraConnectionTest extends TestCase {
    private final UUID uuid = UUIDs.timeBased();

    @Override
    public void setUp () {
        super.setName( CassandraDataControl.class.getName() );

        /*
        Launch the Database connection
        */
        CassandraDataControl.getInstance();
    }

    @Override
    public void tearDown () {
        CassandraDataControl.getInstance().close();
    }

    /*
    checks and make sure that Cassandra Cluster was established
    and session from Cluster was initiated
    */
    @DisplayName( value = "testConnectionWasEstablished method" )
    public void testConnectionWasEstablished () {
        assertNotNull( CassandraDataControl.getInstance() );
        assertNotNull( CassandraDataControl.getInstance().getCluster() );
        assertNotNull( CassandraDataControl.getInstance().getSession() );
        assertFalse( CassandraDataControl.getInstance().getCluster().isClosed() );
        assertFalse( CassandraDataControl.getInstance().getSession().isClosed() );

        Row row = CassandraDataControl.getInstance().getRowFromTabletsKeyspace(
                EntitiesInstances.PATRUL,
                "uuid",
                this.uuid.toString()
        );

        assertNotNull( row );
        assertFalse( row.getString( "passportNumber" ).isBlank() );

        row = CassandraDataControl.getInstance().getSession().execute(
                MessageFormat.format(
                        """
                           SELECT COUNT(*)
                           FROM {0}.{1};
                           """,
                        CassandraTables.TABLETS,
                        EntitiesInstances.PATRUL
                )
        ).one();

        assertNotNull( row );
        assertTrue( row.getLong( "count" ) > 2 );
    }

    @DisplayName( value = "testPatrulInsert method" )
    public void testPatrulInsert () {
        final Patrul patrul = EntitiesInstances.PATRUL;

        assertTrue( patrul.save() );
    }

    @DisplayName( value = "testPatrulUpdate method" )
    public void testPatrulUpdate () {
        assertTrue(
                EntitiesInstances.PATRUL.generate(
                        CassandraDataControl.getInstance().getRowFromTabletsKeyspace(
                                EntitiesInstances.PATRUL,
                                "uuid",
                                this.uuid.toString()
                        )
                ).updateEntity()
        );
    }

    @DisplayName( value = "testPatrulDelete method" )
    public void testPatrulDelete () {
        assertTrue(
                EntitiesInstances.PATRUL.generate(
                        CassandraDataControl.getInstance().getRowFromTabletsKeyspace(
                                EntitiesInstances.PATRUL,
                                "uuid",
                                this.uuid.toString()
                        )
                ).delete()
        );
    }
}
