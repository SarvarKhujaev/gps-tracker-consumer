package com.ssd.mvd.entity.patrulDataSet.patrulSubClasses;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.ssd.mvd.annotations.entity.object.EntityConstructorAnnotation;
import com.ssd.mvd.annotations.entity.object.EntityAnnotations;

import com.ssd.mvd.constants.cassandra.CassandraTables;
import com.ssd.mvd.entity.ConsumptionData;

import com.ssd.mvd.inspectors.CollectionsInspector;
import com.ssd.mvd.inspectors.AnnotationInspector;
import com.ssd.mvd.interfaces.entity.EntityToCassandraConverter;

import java.util.SortedMap;
import java.util.Date;
import java.util.UUID;

@EntityAnnotations(
        name = "PatrulFuelStatistics",
        isSubClass = true,
        tableName = CassandraTables.TRACKER_FUEL_CONSUMPTION
)
public final class PatrulFuelStatistics implements EntityToCassandraConverter {
    public UUID getUuid() {
        return this.uuid;
    }

    public void setUuid( final UUID uuid ) {
        this.uuid = uuid;
    }

    public double getAverageDistance() {
        return this.averageDistance;
    }

    public void setAverageDistance( final double averageDistance ) {
        this.averageDistance = averageDistance;
    }

    public double getAverageFuelConsumption() {
        return this.averageFuelConsumption;
    }

    public void setAverageFuelConsumption( final double averageFuelConsumption ) {
        this.averageFuelConsumption = averageFuelConsumption;
    }

    public SortedMap< Date, ConsumptionData > getMap() {
        return this.map;
    }

    public void setMap( final SortedMap< Date, ConsumptionData > map ) {
        this.map = map;
    }

    private UUID uuid;
    private double averageDistance = 0.0;
    private double averageFuelConsumption = 0.0;
    private SortedMap< Date, ConsumptionData > map = CollectionsInspector.newTreeMap();

    public PatrulFuelStatistics () {}

    @EntityConstructorAnnotation
    public <T> PatrulFuelStatistics ( final Class<T> instance ) {
        AnnotationInspector.checkCallerPermission( instance, PatrulFuelStatistics.class );
    }

    @lombok.NonNull
    @org.jetbrains.annotations.Contract( value = "_ -> !null" )
    public synchronized Select getEntitySelect (
            final Object ... params
    ) {
        return params.length == 1
                ? this.startSelect()
                    .column(
                            CqlIdentifier.fromCql( "min(date)" )
                    ).as( "min_date" )
                    .column(
                            CqlIdentifier.fromCql( "max(date)" )
                    ).as( "max_date" )
                    .where(
                            Relation.column(
                                    CqlIdentifier.fromCql( "imei" )
                            ).isEqualTo( QueryBuilder.literal( params[0] ) )
                    )
                : this.startSelect()
                    .column(
                            CqlIdentifier.fromCql( "min(date)" )
                    ).as( "min_date" )
                    .column(
                            CqlIdentifier.fromCql( "max(date)" )
                    ).as( "max_date" )
                    .where(
                            Relation.column(
                                    CqlIdentifier.fromCql( "imei" )
                            ).isEqualTo( QueryBuilder.literal( params[0] ) ),
                            Relation.column(
                                    CqlIdentifier.fromCql( "date" )
                            ).isGreaterThanOrEqualTo( QueryBuilder.literal( params[1] ) ),
                            Relation.column(
                                    CqlIdentifier.fromCql( "date" )
                            ).isLessThanOrEqualTo( QueryBuilder.literal( params[2] ) )
                    );
    }
}
