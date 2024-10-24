package com.ssd.mvd.inspectors;

import com.ssd.mvd.interfaces.KafkaEntitiesCommonMethods;
import com.ssd.mvd.annotations.AvroMethodAnnotation;
import com.ssd.mvd.annotations.AvroFieldAnnotation;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData;
import org.apache.avro.Schema;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.CopyOnWriteArrayList;

@SuppressWarnings(
        value = """
                отвечает за работу с интерфейсом Schema библиотеки AVRO
                """
)
public final class AvroSchemaInspector {
    private static final AtomicReference< CopyOnWriteArrayList< Schema.Field > > schemas = EntitiesInstances.generateAtomicEntity(
            CollectionsInspector.newList()
    );

    @lombok.NonNull
    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_ -> !null" )
    public static synchronized <T extends KafkaEntitiesCommonMethods> Schema generateSchema(
            @lombok.NonNull final T entity
    ) {
        schemas.getAndSet( CollectionsInspector.newList() );

        CollectionsInspector.analyze(
                AnnotationInspector
                        .getFields( entity.getClass() )
                        .filter( field -> field.isAnnotationPresent( AvroFieldAnnotation.class ) )
                        .map( field -> field.getAnnotation( AvroFieldAnnotation.class ) ),
                avroFieldAnnotation -> schemas.get().add(
                        avroFieldAnnotation.isDate()
                                ? new Schema.Field(
                                        avroFieldAnnotation.name(),
                                        Schema.create( avroFieldAnnotation.schemaType() ),
                                        avroFieldAnnotation.description(),
                                        StringOperations.AVRO_DATE_PATTERN
                                )
                                : new Schema.Field(
                                        avroFieldAnnotation.name(),
                                        Schema.create( avroFieldAnnotation.schemaType() ),
                                        avroFieldAnnotation.description()
                                )
                )
        );

        return Schema.createRecord(
                entity.getClass().getCanonicalName(),
                entity.getTopicName().name(),
                entity.getClass().getPackageName(),
                false,
                schemas.get()
        );
    }

    @lombok.NonNull
    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_ -> !null" )
    public static synchronized <T extends KafkaEntitiesCommonMethods> GenericRecord generateGenericRecord (
            @lombok.NonNull final T entity
    ) {
        final GenericRecord genericRecord = new GenericData.Record( generateSchema( entity ) );

        CollectionsInspector.analyze(
                AnnotationInspector
                        .getMethods( entity.getClass() )
                        .filter( method -> method.isAnnotationPresent( AvroMethodAnnotation.class ) ),
                method -> {
                    try {
                        genericRecord.put(
                                method.getAnnotation( AvroMethodAnnotation.class ).name(),
                                method.invoke( entity )
                        );
                    } catch ( final InvocationTargetException | IllegalAccessException e ) {
                        System.out.println( e.getMessage() );
                    }
                }
        );

        return genericRecord;
    }

    public static void close () {
        schemas.get().clear();
    }
}
