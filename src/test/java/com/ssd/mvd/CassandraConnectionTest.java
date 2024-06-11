package com.ssd.mvd;

import com.ssd.mvd.database.CassandraDataControl;
import com.ssd.mvd.entity.patrulDataSet.Patrul;
import com.ssd.mvd.constants.CassandraTables;

import com.datastax.driver.core.utils.UUIDs;
import com.datastax.driver.core.Row;

import junit.framework.TestCase;
import java.text.MessageFormat;

import java.util.List;
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

    public void testGetListOfEntities () {
        final List< Row > rows = CassandraDataControl
                .getInstance()
                .getListOfEntities(
                        CassandraTables.PATRULS,
                        "id",
                        List.of( this.uuid, this.uuid )
                );

        assertNotNull( rows );
        assertTrue( rows.isEmpty() );
    }

    /*
    checks and make sure that Cassandra Cluster was established
    and session from Cluster was initiated
    */
    public void testConnectionWasEstablished () {
        assertNotNull( CassandraDataControl.getInstance() );
        assertNotNull( CassandraDataControl.getInstance().getCluster() );
        assertNotNull( CassandraDataControl.getInstance().getSession() );
        assertFalse( CassandraDataControl.getInstance().getCluster().isClosed() );
        assertFalse( CassandraDataControl.getInstance().getSession().isClosed() );

        Row row = CassandraDataControl.getInstance().getRowFromTabletsKeyspace(
                CassandraTables.PATRULS,
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
                        CassandraTables.PATRULS
                )
        ).one();

        assertNotNull( row );
        assertTrue( row.getLong( "count" ) > 2 );
    }

    public void testPatrulInsert () {
        final Patrul patrul = Patrul.empty();

        assertTrue( patrul.save() );
    }

    public void testPatrulUpdate () {
        assertTrue(
                new Patrul(
                        CassandraDataControl.getInstance().getRowFromTabletsKeyspace(
                                CassandraTables.PATRULS,
                                "uuid",
                                this.uuid.toString()
                        )
                ).updateEntity()
        );
    }

    public void testPatrulDelete () {
        assertTrue(
                new Patrul(
                        CassandraDataControl.getInstance().getRowFromTabletsKeyspace(
                                CassandraTables.PATRULS,
                                "uuid",
                                this.uuid.toString()
                        )
                ).delete()
        );
    }
}
