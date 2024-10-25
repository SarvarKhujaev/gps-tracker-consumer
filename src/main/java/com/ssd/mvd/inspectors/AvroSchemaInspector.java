package com.ssd.mvd.inspectors;

import com.ssd.mvd.interfaces.KafkaEntitiesCommonMethods;
import com.ssd.mvd.constants.Status;

import com.ssd.mvd.annotations.AvroMethodAnnotation;
import com.ssd.mvd.annotations.AvroFieldAnnotation;

import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.Schema;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData;

import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.DatumReader;

import java.lang.reflect.InvocationTargetException;
import java.io.ByteArrayInputStream;
import java.lang.ref.WeakReference;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

@SuppressWarnings(
        value = """
                отвечает за работу с интерфейсом Schema библиотеки AVRO
                """
)
public final class AvroSchemaInspector {
    private static final AtomicReference< CopyOnWriteArrayList< Schema.Field > > schemas = EntitiesInstances.generateAtomicEntity(
            CollectionsInspector.newList()
    );

    private final static WeakReference< Schema > STATUS_ENUM_SCHEMA = EntitiesInstances.generateWeakEntity(
            Schema.createEnum(
                    "status",
                    StringOperations.EMPTY,
                    Status.class.getPackageName(),
                    Stream.of( Status.values() )
                            .map( Status::name )
                            .toList()

            )
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
                        avroFieldAnnotation.isEnum()
                                ? new Schema.Field(
                                        avroFieldAnnotation.name(),
                                        STATUS_ENUM_SCHEMA.get()
                                )
                                : avroFieldAnnotation.isDate()
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
        final WeakReference< GenericRecord > genericRecord = EntitiesInstances.generateWeakEntity(
                new GenericData.Record( generateSchema( entity ) )
        );

        CollectionsInspector.analyze(
                AnnotationInspector
                        .getMethods( entity.getClass() )
                        .filter( method -> method.isAnnotationPresent( AvroMethodAnnotation.class ) ),
                method -> {
                    try {
                        genericRecord.get().put(
                                method.getAnnotation( AvroMethodAnnotation.class ).name(),
                                method.invoke( entity )
                        );
                    } catch ( final InvocationTargetException | IllegalAccessException e ) {
                        System.out.println( e.getMessage() );
                    }
                }
        );

        return genericRecord.get();
    }

    @lombok.NonNull
    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_, _ -> fail" )
    public static synchronized <T extends KafkaEntitiesCommonMethods> T deserialize( final byte[] data, final T instance ) {
        try ( final ByteArrayInputStream inputStream = new ByteArrayInputStream( data ) ) {
            final DatumReader< GenericRecord > datumReader = new SpecificDatumReader<>( generateGenericRecord( instance ).getSchema() );

            System.out.println(
                    datumReader.read(
                            null,
                            DecoderFactory.get().binaryDecoder( data,null )
                    ).getSchema()
            );

            return instance;
        } catch ( final Exception e ) {
            System.out.println( e.getMessage() );
            return null;
        }
    }

    public static void close () {
        schemas.get().clear();
        CustomServiceCleaner.clearReference( STATUS_ENUM_SCHEMA );
    }
}
