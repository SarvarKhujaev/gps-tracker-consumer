package com.ssd.mvd;

import com.ssd.mvd.database.CassandraDataControl;
import com.ssd.mvd.inspectors.EntitiesInstances;
import com.ssd.mvd.constants.CassandraTables;

import com.datastax.driver.core.GettableData;
import com.datastax.driver.core.utils.UUIDs;

import org.junit.jupiter.api.DisplayName;
import junit.framework.TestCase;

import java.text.MessageFormat;
import java.util.UUID;

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

        GettableData row = CassandraDataControl
                .getInstance()
                .getRowFromTabletsKeyspace(
                        EntitiesInstances.PATRUL.get(),
                        this.uuid.toString()
                ).get();

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
        assertTrue( EntitiesInstances.PATRUL.get().save() );
    }

    @DisplayName( value = "testPatrulUpdate method" )
    public void testPatrulUpdate () {
        assertTrue(
                EntitiesInstances.PATRUL.get().generate(
                        CassandraDataControl.getInstance().getRowFromTabletsKeyspace(
                                EntitiesInstances.PATRUL.get(),
                                this.uuid.toString()
                        ).get()
                ).updateEntity()
        );
    }

    @DisplayName( value = "testPatrulDelete method" )
    public void testPatrulDelete () {
        assertTrue(
                EntitiesInstances.PATRUL.get().generate(
                        CassandraDataControl
                                .getInstance()
                                .getRowFromTabletsKeyspace(
                                        EntitiesInstances.PATRUL.get(),
                                        this.uuid.toString()
                                ).get()
                ).delete()
        );
    }
}
