package com.ssd.mvd.inspectors;

import com.ssd.mvd.interfaces.EntityToCassandraConverter;
import com.ssd.mvd.constants.Errors;
import com.ssd.mvd.annotations.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Field;

import com.ssd.mvd.interfaces.ServiceCommonMethods;
import org.apache.commons.lang3.Validate;

import java.lang.ref.WeakReference;
import java.util.stream.Stream;
import java.util.*;

@com.ssd.mvd.annotations.ImmutableEntityAnnotation
public class AnnotationInspector extends LogInspector {
    protected static volatile WeakReferenceAnnotation weakReferenceAnnotation;
    protected static volatile MethodsAnnotations methodsAnnotations;
    protected static volatile FieldAnnotation fieldAnnotation;

    protected AnnotationInspector () {
        super( AnnotationInspector.class );
    }

    @EntityConstructorAnnotation()
    protected <T> AnnotationInspector ( @lombok.NonNull final Class<T> instance ) {
        super( AnnotationInspector.class );

        AnnotationInspector.checkCallerPermission( instance, AnnotationInspector.class );
        AnnotationInspector.checkAnnotationIsImmutable( AnnotationInspector.class );
    }

    @SuppressWarnings(
            value = """
                    Принимает класс и возвращает его экземпляры классов,
                    у которых есть доступ к конструктору вызванного объекта

                    Проверяет что у метода есть нужная аннотация
                    В случае ошибки вызывает Exception с подходящим сообщением
                    """
    )
    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_, _ -> fail" )
    public static synchronized <T, U> void checkCallerPermission (
            // класс который обращается
            @lombok.NonNull @com.typesafe.config.Optional final Class<T> callerInstance,
            // класс к которому обращаются
            @lombok.NonNull @com.typesafe.config.Optional final Class<U> calledInstance
    ) {
        Validate.notNull( callerInstance, StringOperations.NULL_VALUE_IN_ASSERT );
        Validate.notNull( calledInstance, StringOperations.NULL_VALUE_IN_ASSERT );

        try {
            final Constructor<U> declaredConstructor = calledInstance.getDeclaredConstructor( Class.class );
            org.springframework.util.ReflectionUtils.makeAccessible( declaredConstructor );
            declaredConstructor.setAccessible( true );

            Validate.isTrue(
                    (
                        declaredConstructor.isAnnotationPresent( EntityConstructorAnnotation.class )
                        && declaredConstructor.getParameters().length == 1
                        && Collections.frequency(
                                convertArrayToList(
                                        declaredConstructor
                                                .getAnnotation( EntityConstructorAnnotation.class )
                                                .permission()
                                ),
                                callerInstance
                        ) > 0
                    ),
                    Errors.OBJECT_IS_OUT_OF_INSTANCE_PERMISSION.translate(
                            callerInstance.getName(),
                            calledInstance.getName()
                    )
            );
        } catch ( final NoSuchMethodException e ) {
            throw new RuntimeException(e);
        }
    }

    @lombok.NonNull
    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_, _ -> !null" )
    public static synchronized String getVariable (
            @lombok.NonNull @com.typesafe.config.Optional final Class< ? > object,
            @lombok.NonNull final String paramName
    ) {
        Validate.isTrue(
                object.isAnnotationPresent( ServiceParametrAnnotation.class ),
                Errors.WRONG_TYPE_IN_ANNOTATION.translate( "ru", object.getName() )
        );

        return checkContextOrReturnDefaultValue(
                String.join(
                        DOT,
                        object.getAnnotation( ServiceParametrAnnotation.class ).mainGroupName(),
                        object.getAnnotation( ServiceParametrAnnotation.class ).propertyGroupName(),
                        paramName
                ),
                Errors.DATA_NOT_FOUND.translate(
                        "ru",
                        String.join(
                                DOT,
                                object.getAnnotation( ServiceParametrAnnotation.class ).mainGroupName(),
                                object.getAnnotation( ServiceParametrAnnotation.class ).propertyGroupName(),
                                paramName
                        )
                )
        );
    }

    @lombok.Synchronized
    @com.typesafe.config.Optional
    @org.jetbrains.annotations.Contract( value = "_ -> _" )
    protected final synchronized boolean checkAnnotations (
            @lombok.NonNull @com.typesafe.config.Optional final Object object
    ) {
        return object.getClass().isAnnotationPresent( EntityAnnotations.class )
                && object.getClass().getAnnotation( EntityAnnotations.class ).canTouch()
                && object.getClass().getAnnotation( EntityAnnotations.class ).isReadable();
    }

    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_ -> fail" )
    protected static synchronized boolean checkAnnotations (
            @lombok.NonNull final Method method
    ) {
        return checkAnnotationInitialized( method )
                && method.getAnnotation( MethodsAnnotations.class ).canTouch()
                && !method.getAnnotation( MethodsAnnotations.class ).withoutParams()
                && !method.getAnnotation( MethodsAnnotations.class ).isReturnEntity();
    }

    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_ -> fail" )
    protected final synchronized boolean checkAnnotations (
            @lombok.NonNull final Field field
    ) {
        return field.isAnnotationPresent( FieldAnnotation.class )
                && field.getAnnotation( FieldAnnotation.class ).canTouch()
                && field.getAnnotation( FieldAnnotation.class ).isReadable();
    }

    protected static void set (
            @lombok.NonNull final Field field
    ) {
        org.springframework.util.ReflectionUtils.makeAccessible( field );
        fieldAnnotation = field.getAnnotation( FieldAnnotation.class );
    }

    protected static void set (
            @lombok.NonNull final Method method
    ) {
        methodsAnnotations = method.getAnnotation( MethodsAnnotations.class );
    }

    @lombok.NonNull
    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_ -> fail" )
    protected static synchronized < T extends EntityToCassandraConverter > EntityAnnotations convertEntityToEntityAnnotation (
            @lombok.NonNull final T o
    ) {
        checkAnnotationInitialized( o.getClass() );
        return o.getClass().getAnnotation( EntityAnnotations.class );
    }

    @lombok.NonNull
    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_ -> fail" )
    protected static synchronized FieldAnnotation convertFieldToFieldAnnotation (
            @lombok.NonNull final Field field
    ) {
        checkAnnotationInitialized( field );
        return field.getAnnotation( FieldAnnotation.class );
    }

    @lombok.NonNull
    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_ -> !null" )
    protected static synchronized MethodsAnnotations convertMethodToMethodAnnotation(
            @lombok.NonNull final Method method
    ) {
        checkAnnotationInitialized( method );
        return method.getAnnotation( MethodsAnnotations.class );
    }

    @SuppressWarnings(
            value = """
                    Принимает любой Object и проверяет не является ли он Immutable
                    если все хорошо, то возвращает сам Object
                    """
    )
    @lombok.Synchronized
    @com.typesafe.config.Optional
    @org.jetbrains.annotations.Contract( value = "_ -> fail" )
    protected static synchronized < T extends EntityToCassandraConverter > T checkAnnotationIsNotImmutable (
            @lombok.NonNull final T object
    ) {
        Validate.isTrue(
                !object.getClass().isAnnotationPresent( ImmutableEntityAnnotation.class ),
                Errors.OBJECT_IS_IMMUTABLE.translate( object.getClass().getName() )
        );

        return object;
    }

    @lombok.Synchronized
    @com.typesafe.config.Optional
    @org.jetbrains.annotations.Contract( value = "_ -> fail" )
    protected static synchronized < T > void checkAnnotationIsImmutable (
            @lombok.NonNull final Class<T> object
    ) {
        Validate.isTrue(
                object.isAnnotationPresent( ImmutableEntityAnnotation.class ),
                Errors.OBJECT_IS_IMMUTABLE.translate( object.getName() )
        );
    }

    @SuppressWarnings(
            value = """
                    Принимает любой Object и проверяет есть ли у него аннотация EntityAnnotations
                    """
    )
    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_ -> fail" )
    protected static synchronized void checkAnnotationInitialized (
            @lombok.NonNull final Class< ? extends EntityToCassandraConverter > object
    ) {
        Validate.isTrue(
                object.isAnnotationPresent( EntityAnnotations.class ),
                Errors.WRONG_TYPE_IN_ANNOTATION.translate( "ru", object.getName() )
        );
    }

    @SuppressWarnings(
            value = """
                    Проверяет есть ли у метода класса аннотация MethodsAnnotations
                    """
    )
    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_ -> _" )
    protected static synchronized boolean checkAnnotationInitialized (
            @lombok.NonNull final Method method
    ) {
        return method.isAnnotationPresent( MethodsAnnotations.class );
    }

    @SuppressWarnings(
            value = """
                    Проверяет есть ли у параметра класса аннотация MethodsAnnotations
                    """
    )
    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_ -> fail" )
    private static synchronized void checkAnnotationInitialized (
            @lombok.NonNull final Field field
    ) {
        Validate.isTrue(
                field.isAnnotationPresent( FieldAnnotation.class ),
                Errors.WRONG_TYPE_IN_ANNOTATION.translate( field.getName() )
        );
    }

    @SuppressWarnings(
            value = """
                    Принимает метод, класс которому он принадледит и возвращает его значение
                    Проверяет что у метода есть нужная аннотация
                    В случае ошибки вызывает Exceptio с подходящим сообщением
                    """
    )
    @lombok.NonNull
    @lombok.Synchronized
    @com.typesafe.config.Optional
    @org.jetbrains.annotations.Contract( value = "_, _ -> !null" )
    protected static synchronized <T extends EntityToCassandraConverter> Object getMethodReturnValue(
            @lombok.NonNull final Method method,
            @lombok.NonNull final T object
    ) {
        try {
            checkAnnotationInitialized( object.getClass() );

            Validate.isTrue(
                    !checkAnnotationInitialized( method ),
                    Errors.WRONG_TYPE_IN_ANNOTATION.translate( method.getName() )
            );

            return method.invoke( object );
        } catch ( final IllegalAccessException | InvocationTargetException e ) {
            return e;
        }
    }

    @SuppressWarnings(
            value = """
                    Принимает метод, класс которому он принадледит и возвращает его значение
                    Проверяет что у метода есть нужная аннотация
                    В случае ошибки вызывает Exceptio с подходящим сообщением
                    """
    )
    @lombok.NonNull
    @lombok.Synchronized
    @com.typesafe.config.Optional
    @org.jetbrains.annotations.Contract( value = "_, _ -> fail" )
    protected static synchronized <T extends EntityToCassandraConverter> Object getFieldReturnValue(
            @lombok.NonNull final Field field,
            @lombok.NonNull final T object
    ) {
        try {
            org.springframework.util.ReflectionUtils.makeAccessible( field );
            checkAnnotationInitialized( object.getClass() );
            checkAnnotationInitialized( field );
            return field.get( object );
        } catch ( final IllegalAccessException e ) {
            throw new IllegalArgumentException( e );
        }
    }

    @lombok.Synchronized
    protected static synchronized <T extends EntityToCassandraConverter> void compareFieldWithParam (
            @lombok.NonNull final Field field,
            @lombok.NonNull @com.typesafe.config.Optional final T entity
    ) {
        set( field );
        Validate.isTrue(
                field.getName().compareTo( fieldAnnotation.name() ) == 0,
                Errors.FIELD_AND_ANNOTATION_NAME_MISMATCH.translate(
                        entity.getClass().getName(),
                        field.getName(),
                        fieldAnnotation.name()
                )
        );
    }

    @SuppressWarnings(
            value = """
                    принимает любой класс и проверяет все его параметры
                    в случае если параметр обозначен как не пустой ( mightBeNull = false )
                    то выкидывается ошибка и процесс останавливается
                    """
    )
    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_ -> fail" )
    public static synchronized <T extends EntityToCassandraConverter> void checkEntityFieldsNotEmpty (
            @lombok.NonNull @com.typesafe.config.Optional final T entity
    ) {
        getFields( entity.getClass() )
                .filter( field -> !convertFieldToFieldAnnotation( field ).mightBeNull() )
                .forEach( field -> {
                    compareFieldWithParam( field, entity );
                    Validate.isTrue(
                            (
                                    Objects.nonNull( getFieldReturnValue( field, entity ) )
                                            || convertFieldToFieldAnnotation( field )
                                            .cassandraType()
                                            .validateDataType()
                                            .check( getFieldReturnValue( field, entity ) )
                            ),
                            Errors.FIELD_MIGHT_NOT_BE_EMPTY.translate(
                                    "ru",
                                    field.getName(),
                                    entity.getClass().getName()
                            )
                    );
                } );
    }

    @SuppressWarnings(
            value = """
                    Принимает класс и возвращает его Primary Keys
                    Проверяет что у метода есть нужная аннотация
                    В случае ошибки вызывает Exceptio с подходящим сообщением
                    """
    )
    @lombok.NonNull
    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_ -> fail" )
    public static synchronized <T extends EntityToCassandraConverter> String[] getEntityPrimaryKey (
            @lombok.NonNull @com.typesafe.config.Optional final T object
    ) {
        Validate.isTrue(
                convertEntityToEntityAnnotation( object ).primaryKeys().length > 0,
                Errors.PRIMARY_KEYS_NOT_FOUND.translate( "ru", object.getClass().getName() )
        );

        return convertEntityToEntityAnnotation( object ).primaryKeys();
    }

    @SuppressWarnings(
            value = """
                    Принимает класс и возвращает его Primary Keys
                    Проверяет что у метода есть нужная аннотация
                    В случае ошибки вызывает Exceptio с подходящим сообщением
                    """
    )
    @lombok.NonNull
    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_ -> fail" )
    protected final synchronized <T extends EntityToCassandraConverter> List< Method > getEntityPrimaryKeys (
            @lombok.NonNull @com.typesafe.config.Optional final T object
    ) {
        Validate.isTrue(
                convertEntityToEntityAnnotation( object ).primaryKeys().length == 0,
                Errors.PRIMARY_KEYS_NOT_FOUND.translate( "ru", object.getClass().getName() )
        );

        return !convertEntityToEntityAnnotation( object ).isSubClass()
                ? getMethods( object.getClass() )
                        .filter( AnnotationInspector::checkAnnotationInitialized )
                        .filter( method -> convertMethodToMethodAnnotation( method ).isPrimaryKey() )
                        .toList()
                : emptyList();
    }

    @SuppressWarnings(
            value = """
                    Принимает поле класса и возвращает его дефолтное пустое значение
                    Проверяет что у метода есть нужная аннотация
                    """
    )
    @lombok.Synchronized
    @com.typesafe.config.Optional
    @org.jetbrains.annotations.Contract( value = "_ -> fail" )
    protected static synchronized Object getDefaultValue (
            @lombok.NonNull final Field field
    ) {
        return convertFieldToFieldAnnotation( field ).cassandraType().getEmptyValue();
    }

    @SuppressWarnings(
            value = """
                    Принимает метод и объект ROW из БД и вызывает нужную функцию
                    Проверяет что у метода есть нужная аннотация
                    """
    )
    @lombok.Synchronized
    @com.typesafe.config.Optional
    @org.jetbrains.annotations.Contract( value = "_, _ -> fail" )
    protected static synchronized Object getEntityValueFromMethodAndSetToEntityField(
            @lombok.NonNull final Method method,
            @lombok.NonNull final com.datastax.driver.core.GettableData gettableData
    ) {
        checkAnnotationInitialized( method );
        return ( methodsAnnotations = convertMethodToMethodAnnotation( method ) )
                .acceptEntityType()
                .getCorrectValueFromRow(
                        gettableData,
                        methodsAnnotations
                );
    }

    @SuppressWarnings(
            value = """
                    Принимает метод и объект UDTValue или ROW из БД и вызывает нужную функцию
                    Проверяет что у метода есть нужная аннотация
                    """
    )
    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_, _, _ -> fail" )
    protected static synchronized <T extends EntityToCassandraConverter, U> void fillParam (
            @lombok.NonNull @com.typesafe.config.Optional final T object,
            final U value,
            @lombok.NonNull final Method method
    ) {
        try {
            if ( checkAnnotationInitialized( method ) ) {
                method.invoke( object, value );
            }
        } catch ( final InvocationTargetException | IllegalAccessException e ) {
            System.out.println( e.getMessage() );
        }
    }

    @lombok.NonNull
    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_, _ -> fail" )
    public static synchronized <T extends EntityToCassandraConverter> T fillEntityParams (
            @lombok.NonNull @com.typesafe.config.Optional final T object,
            final com.datastax.driver.core.GettableData gettableData
    ) {
        if ( !Objects.nonNull( gettableData ) ) {
            return object;
        }

        AnnotationInspector.checkAnnotationInitialized( object.getClass() );

        checkAndSetParams(
                gettableData,
                gettableData1 -> getMethods( object.getClass() )
                        .filter( AnnotationInspector::checkAnnotations )
                        .forEach( method -> fillParam( object, getEntityValueFromMethodAndSetToEntityField( method, gettableData ), method ) )
        );

        return object;
    }

    @SuppressWarnings(
            value = """
                    Принимает экземпляр класса и возвращает список всех его параметров
                    """
    )
    @lombok.NonNull
    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_ -> !null" )
    protected static synchronized Stream< Field > getFields (
            @lombok.NonNull @com.typesafe.config.Optional final Class< ? > object
    ) {
        return convertArrayToStream( object.getDeclaredFields() );
    }

    @SuppressWarnings(
            value = """
                    Принимает экземпляр класса и возвращает список всех его методов
                    """
    )
    @lombok.NonNull
    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_ -> !null" )
    protected static synchronized Stream< Method > getMethods (
            @lombok.NonNull @com.typesafe.config.Optional final Class< ? > object
    ) {
        return convertArrayToStream( object.getDeclaredMethods() );
    }

    @SuppressWarnings(
            value = """
                    Проверяет содержит ли параметр класса,
                    экземпляр класса имплементирующего интерфейс ServiceCommonMethods
                    """
    )
    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_ -> true" )
    protected static synchronized boolean iSFieldMustBeCleaned (
            @lombok.NonNull final Field field
    ) {
        return field.getClass().getInterfaces().length == 1
                && field.getClass().getInterfaces()[0].isAssignableFrom( ServiceCommonMethods.class );
    }

    @SuppressWarnings(
            value = """
                    берет все статичные ссылки на объекты из сборника EntitiesInstances
                    и очищает каждый объект через вызов метода close из интерфейса ServiceCommonMethods
                    """
    )
    protected void clearAllEntities () {
        super.analyze(
                EntitiesInstances.instancesList,
                atomicReference -> super.analyze(
                        getFields( atomicReference.get().getClass() )
                                .filter( field -> field.isAnnotationPresent( WeakReferenceAnnotation.class ) )
                                .toList(),
                        field -> {
                            try {
                                org.springframework.util.ReflectionUtils.makeAccessible( field );
                                weakReferenceAnnotation = field.getAnnotation( WeakReferenceAnnotation.class );

                                super.logging( "Clearing: " + weakReferenceAnnotation.name() );

                                Validate.isTrue(
                                        field.getName().compareTo( weakReferenceAnnotation.name() ) == 0,
                                        Errors.FIELD_AND_ANNOTATION_NAME_MISMATCH.translate(
                                                atomicReference.get().getClass().getName(),
                                                field.getName(),
                                                weakReferenceAnnotation.name()
                                        )
                                );

                                if ( weakReferenceAnnotation.isCollection() ) {
                                    super.checkAndClear(
                                            ( (Collection<?>) field.get( atomicReference.get() ) )
                                    );
                                } else if ( weakReferenceAnnotation.isWeak() ) {
                                    clearReference(
                                            ( (WeakReference<?>) field.get( atomicReference.get() ) )
                                    );
                                } else if ( weakReferenceAnnotation.isMap() ) {
                                    super.checkAndClear(
                                            ( (Map<?, ?>) field.get( atomicReference.get() ) )
                                    );
                                }

                                field.set( atomicReference.get(), null );
                            } catch ( final IllegalAccessException e ) {
                                System.out.println( e.getMessage() );
                            }
                        }
                )
        );

        clearReference( uuid );
        AvroSchemaInspector.close();
        super.logging( AnnotationInspector.class );
    }

    @SuppressWarnings(
            value = """
                    берет все статичные ссылки на объекты из сборника EntitiesInstances
                    и очищает каждый объект через вызов метода close из интерфейса ServiceCommonMethods
                    """
    )
    protected <T extends ServiceCommonMethods> void clearEntity ( @lombok.NonNull final T entity ) {
        super.analyze(
                getFields( entity.getClass() )
                        .filter( field -> field.isAnnotationPresent( WeakReferenceAnnotation.class ) )
                        .toList(),
                field -> {
                    try {
                        org.springframework.util.ReflectionUtils.makeAccessible( field );
                        weakReferenceAnnotation = field.getAnnotation( WeakReferenceAnnotation.class );

                        super.logging( "Clearing: " + weakReferenceAnnotation.name() );

                        Validate.isTrue(
                                field.getName().compareTo( weakReferenceAnnotation.name() ) == 0,
                                Errors.FIELD_AND_ANNOTATION_NAME_MISMATCH.translate(
                                        entity.getClass().getName(),
                                        field.getName(),
                                        weakReferenceAnnotation.name()
                                )
                        );

                        if ( weakReferenceAnnotation.isCollection() ) {
                            super.checkAndClear(
                                    ( (Collection<?>) field.get( entity ) )
                            );
                        } else if ( weakReferenceAnnotation.isWeak() ) {
                            clearReference(
                                    ( (WeakReference<?>) field.get( entity ) )
                            );
                        } else if ( weakReferenceAnnotation.isMap() ) {
                            super.checkAndClear(
                                    ( (Map<?, ?>) field.get( entity ) )
                            );
                        }

                        /*
                        если параметр является ссылкой на другой объект,
                        то просто стираем его через обозначение null
                        */
                        else {
                            if ( iSFieldMustBeCleaned( field ) ) {
                                super.clearReference( entity );
                            }

                            field.set( entity, null );
                        }
                    } catch ( final IllegalAccessException e ) {
                        System.out.println( e.getMessage() );
                    }
                }
        );
    }
}
