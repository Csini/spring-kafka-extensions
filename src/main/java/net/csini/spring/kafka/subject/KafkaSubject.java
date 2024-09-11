package net.csini.spring.kafka.subject;

import org.apache.kafka.clients.producer.RecordMetadata;

import io.reactivex.rxjava3.annotations.*;
import io.reactivex.rxjava3.core.*;

/**
 * Represents an {@link Observer} and an {@link Observable} at the same time, allowing
 * KafkaObserver to multicast RecordMetaData as events from a single source to multiple child {@code Observer}s.
 * <p>
 *
 * @param <T> the item value type
 */
public abstract class KafkaSubject<T> extends Observable<RecordMetadata> implements Observer<T> {
    /**
     * Returns true if the subject has any Observers.
     * <p>The method is thread-safe.
     * @return true if the subject has any Observers
     */
    @CheckReturnValue
    public abstract boolean hasObservers();

    /**
     * Returns true if the subject has reached a terminal state through an error event.
     * <p>The method is thread-safe.
     * @return true if the subject has reached a terminal state through an error event
     * @see #getThrowable()
     * @see #hasComplete()
     */
    @CheckReturnValue
    public abstract boolean hasThrowable();

    /**
     * Returns true if the subject has reached a terminal state through a complete event.
     * <p>The method is thread-safe.
     * @return true if the subject has reached a terminal state through a complete event
     * @see #hasThrowable()
     */
    @CheckReturnValue
    public abstract boolean hasComplete();

    /**
     * Returns the error that caused the Subject to terminate or null if the Subject
     * hasn't terminated yet.
     * <p>The method is thread-safe.
     * @return the error that caused the Subject to terminate or null if the Subject
     * hasn't terminated yet
     */
    @Nullable
    @CheckReturnValue
    public abstract Throwable getThrowable();

//    /**
//     * Wraps this Subject and serializes the calls to the onSubscribe, onNext, onError and
//     * onComplete methods, making them thread-safe.
//     * <p>The method is thread-safe.
//     * @return the wrapped and serialized subject
//     */
//    @NonNull
//    @CheckReturnValue
//    public final Subject<T> toSerialized() {
//        if (this instanceof SerializedSubject) {
//            return this;
//        }
//        return new SerializedSubject<>(this);
//    }
}
