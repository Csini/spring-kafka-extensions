package net.csini.spring.kafka.subject;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonSerializer;

import io.reactivex.rxjava3.annotations.CheckReturnValue;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.annotations.Nullable;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.internal.util.ExceptionHelper;
import io.reactivex.rxjava3.plugins.RxJavaPlugins;
import net.csini.spring.kafka.KafkaEntitySubject;
import net.csini.spring.kafka.KafkaEntityKey;
import net.csini.spring.kafka.exception.KafkaEntityException;
import net.csini.spring.kafka.mapping.JsonKeySerializer;
import net.csini.spring.kafka.util.KafkaEntityUtil;

public final class SimpleKafkaEntitySubject<T, K> extends KafkaSubject<T> implements DisposableBean, InitializingBean {

	private static final Logger LOGGER = LoggerFactory.getLogger(SimpleKafkaEntitySubject.class);

	/** The terminated indicator for the subscribers array. */
	@SuppressWarnings("rawtypes")
	static final PublishDisposable[] TERMINATED = new PublishDisposable[0];
	/** An empty subscribers array to avoid allocating it all the time. */
	@SuppressWarnings("rawtypes")
	static final PublishDisposable[] EMPTY = new PublishDisposable[0];

	/** The array of currently subscribed subscribers. */
	final AtomicReference<PublishDisposable<T, K>[]> subscribers;

	/**
	 * Comma-delimited list of host:port pairs to use for establishing the initial
	 * connections to the Kafka cluster. Applies to all components unless
	 * overridden.
	 */
	@Value(value = "${spring.kafka.bootstrap-servers:localhost:9092}")
	private List<String> bootstrapServers = new ArrayList<>(Collections.singletonList("localhost:9092"));

	private String clientid;

	private KafkaProducer<K, T> kafkaProducer;

	private Class<T> clazz;

	private String topic;

	/** The error, write before terminating and read after checking subscribers. */
	Throwable error;

	private Field keyField;

	private boolean transactional;

	/**
	 * Constructs a SimpleKafkaObserver.
	 * 
	 * @param <T> the value type
	 * @return the new SimpleKafkaObserver
	 * @throws KafkaEntityException
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	@CheckReturnValue
	@NonNull
	public static <T, K> SimpleKafkaEntitySubject<T, K> create(KafkaEntitySubject kafkaEntitySubject, String beanName)
			throws InterruptedException, ExecutionException, KafkaEntityException {
		return new SimpleKafkaEntitySubject<>(kafkaEntitySubject, beanName);
	}

	/**
	 * Constructs a SimpleKafkaObserver.
	 * 
	 * @throws KafkaEntityException
	 * 
	 */
	SimpleKafkaEntitySubject(KafkaEntitySubject kafkaEntitySubject, String beanName)
			throws InterruptedException, ExecutionException {

		subscribers = new AtomicReference<>(EMPTY);

		this.clazz = kafkaEntitySubject.entity();
		this.clientid = beanName;
		this.topic = KafkaEntityUtil.getTopicName(this.clazz);

		this.transactional = kafkaEntitySubject.transactional();

		// presents of @KafkaEntityKey is checked in KafkaEntityConfig
		for (Field field : this.clazz.getDeclaredFields()) {
			LOGGER.debug("    field  -> " + field.getName());
			if (field.isAnnotationPresent(KafkaEntityKey.class)) {
				field.setAccessible(true);
				this.keyField = field;
				break;
			}
		}

		Map<String, Object> configProps = new HashMap<>();
		configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, JsonKeySerializer.class);
		configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
		configProps.put(ProducerConfig.CLIENT_ID_CONFIG, clientid);

		if (transactional) {
			configProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, beanName + "-transactional-id");

			configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

//		configProps.put(ProducerConfig.“transaction.state.log.min.isr”, 2);
		}
		JsonSerializer<T> valueSerializer = new JsonSerializer<>();
		JsonKeySerializer<K> keySerializer = new JsonKeySerializer<>();

		this.kafkaProducer = new KafkaProducer<K, T>(configProps, keySerializer, valueSerializer);
		if (transactional) {
//	      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
//	      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
//	      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1

			LOGGER.warn("initTransactions-begin");
			this.kafkaProducer.initTransactions();
			LOGGER.warn("initTransactions-end");
		}
	}

	public Class<T> getClazz() {
		return this.clazz;
	}

//	public Class<K> getClazzKey() {
//		return this.clazzKey;
//	}

	private K extractKey(T event) throws IllegalArgumentException, IllegalAccessException {
		return (K) this.keyField.get(event);
	}

	@Override
	public void onSubscribe(Disposable d) {
		LOGGER.info("sending events");

		if (subscribers.get() == TERMINATED) {
			d.dispose();
		}

//		String topic = KafkaEntityUtil.getTopicName(getClazz());

		if (transactional) {
			this.kafkaProducer.beginTransaction();
		}
	}

	@Override
	public void onNext(T t) {
		ExceptionHelper.nullCheck(t, "onNext called with a null value.");
		CompletableFuture<RecordMetadata> completableFuture = new CompletableFuture<>();
		K key;
		try {
			key = extractKey(t);

			ProducerRecord<K, T> rec = new ProducerRecord<K, T>(topic, key, t);
			Future<RecordMetadata> send = this.kafkaProducer.send(rec);
			completableFuture.complete(send.get());
		} catch (InterruptedException | ExecutionException e) {
			completableFuture.completeExceptionally(e);
		} catch (IllegalAccessException e) {
//			throw new KafkaEntityWarapperException(e);
			completableFuture.completeExceptionally(e);
		}

		try {
			RecordMetadata r = completableFuture.get();
			for (PublishDisposable<T, K> pd : subscribers.get()) {
				pd.onNext(r);
			}
		} catch (InterruptedException | ExecutionException e) {
			onError(e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void onError(Throwable t) {
		ExceptionHelper.nullCheck(t, "onError called with a null Throwable.");
		if (subscribers.get() == TERMINATED) {
			RxJavaPlugins.onError(t);
			return;
		}
		error = t;

		for (PublishDisposable<T, K> pd : subscribers.getAndSet(TERMINATED)) {
			pd.onError(t);
		}
		if (transactional) {
			this.kafkaProducer.abortTransaction();
		}
		return;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void onComplete() {
		if (subscribers.get() == TERMINATED) {
			return;
		}
		for (PublishDisposable<T, K> pd : subscribers.getAndSet(TERMINATED)) {
			pd.onComplete();
		}
		if (transactional) {
			this.kafkaProducer.commitTransaction();
		}
	}

	@Override
	public void afterPropertiesSet() throws Exception {

	}

	@Override
	public void destroy() throws Exception {
		this.kafkaProducer.close();
	}

	@Override
	protected void subscribeActual(Observer<? super RecordMetadata> r) {
		PublishDisposable<T, K> ps = new PublishDisposable<>(r, this);
		r.onSubscribe(ps);
		if (add(ps)) {
			// if cancellation happened while a successful add, the remove() didn't work
			// so we need to do it again
			if (ps.isDisposed()) {
				remove(ps);
			}
		} else {
			Throwable ex = error;
			if (ex != null) {
				r.onError(ex);
			} else {
				r.onComplete();
			}
		}
	}

	/**
	 * Tries to add the given subscriber to the subscribers array atomically or
	 * returns false if the subject has terminated.
	 * 
	 * @param ps the subscriber to add
	 * @return true if successful, false if the subject has terminated
	 */
	boolean add(PublishDisposable<T, K> ps) {
		for (;;) {
			PublishDisposable<T, K>[] a = subscribers.get();
			if (a == TERMINATED) {
				return false;
			}

			int n = a.length;
			@SuppressWarnings("unchecked")
			PublishDisposable<T, K>[] b = new PublishDisposable[n + 1];
			System.arraycopy(a, 0, b, 0, n);
			b[n] = ps;

			if (subscribers.compareAndSet(a, b)) {
				return true;
			}
		}
	}

	/**
	 * Atomically removes the given subscriber if it is subscribed to the subject.
	 * 
	 * @param ps the subject to remove
	 */
	@SuppressWarnings("unchecked")
	void remove(PublishDisposable<T, K> ps) {
		for (;;) {
			PublishDisposable<T, K>[] a = subscribers.get();
			if (a == TERMINATED || a == EMPTY) {
				return;
			}

			int n = a.length;
			int j = -1;
			for (int i = 0; i < n; i++) {
				if (a[i] == ps) {
					j = i;
					break;
				}
			}

			if (j < 0) {
				return;
			}

			PublishDisposable<T, K>[] b;

			if (n == 1) {
				b = EMPTY;
			} else {
				b = new PublishDisposable[n - 1];
				System.arraycopy(a, 0, b, 0, j);
				System.arraycopy(a, j + 1, b, j, n - j - 1);
			}
			if (subscribers.compareAndSet(a, b)) {
				return;
			}
		}
	}

	@Override
	@CheckReturnValue
	public boolean hasObservers() {
		return subscribers.get().length != 0;
	}

	@Override
	@Nullable
	@CheckReturnValue
	public Throwable getThrowable() {
		if (subscribers.get() == TERMINATED) {
			return error;
		}
		return null;
	}

	@Override
	@CheckReturnValue
	public boolean hasThrowable() {
		return subscribers.get() == TERMINATED && error != null;
	}

	@Override
	@CheckReturnValue
	public boolean hasComplete() {
		return subscribers.get() == TERMINATED && error == null;
	}

	/**
	 * Wraps the actual subscriber, tracks its requests and makes cancellation to
	 * remove itself from the current subscribers array.
	 *
	 * @param <T> the value type
	 */
	static final class PublishDisposable<T, K> extends AtomicBoolean implements Disposable {

		private static final long serialVersionUID = 3562861878281475070L;
		/** The actual subscriber. */
		final Observer<? super RecordMetadata> downstream;
		/** The subject state. */
		final SimpleKafkaEntitySubject<T, K> parent;

		/**
		 * Constructs a PublishSubscriber, wraps the actual subscriber and the state.
		 * 
		 * @param actual the actual subscriber
		 * @param parent the parent PublishProcessor
		 */
		PublishDisposable(Observer<? super RecordMetadata> actual, SimpleKafkaEntitySubject<T, K> parent) {
			this.downstream = actual;
			this.parent = parent;
		}

		public void onNext(RecordMetadata r) {
			if (!get()) {
				downstream.onNext(r);
			}
		}

		public void onError(Throwable t) {
			if (get()) {
				RxJavaPlugins.onError(t);
			} else {
				downstream.onError(t);
			}
		}

		public void onComplete() {
			if (!get()) {
				downstream.onComplete();
			}
		}

		@Override
		public void dispose() {
			if (compareAndSet(false, true)) {
				parent.remove(this);
			}
		}

		@Override
		public boolean isDisposed() {
			return get();
		}
	}
}
