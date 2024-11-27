package com.ssd.mvd.subscribers;

import com.ssd.mvd.functions.CustomSubscriberFunction;
import com.ssd.mvd.inspectors.LogInspector;

import org.reactivestreams.Subscription;
import org.reactivestreams.Subscriber;

@com.ssd.mvd.annotations.services.ImmutableEntityAnnotation
public final class CustomSubscriber<T> extends LogInspector implements Subscriber<T> {
    private final CustomSubscriberFunction< T > customSubscriberFunction;
    private Subscription subscription;

    public CustomSubscriber( @lombok.NonNull final CustomSubscriberFunction< T > customSubscriberFunction ) {
        this.customSubscriberFunction = customSubscriberFunction;
    }

    @Override
    public void onSubscribe( @lombok.NonNull final Subscription subscription ) {
        this.subscription = subscription;
        this.subscription.request( 1 );
    }

    @Override
    public void onNext( @lombok.NonNull final T o ) {
        this.customSubscriberFunction.complete( o );
        this.subscription.request( 1 );
    }

    @Override
    public void onError( @lombok.NonNull final Throwable throwable ) {
        super.logging( throwable );
    }

    @Override
    public void onComplete() {
        super.logging( this );
    }
}
