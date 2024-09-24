package net.csini.spring.kafka.observable;

import java.lang.reflect.Field;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsResult;
import org.apache.kafka.common.KafkaFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;

import io.reactivex.rxjava3.annotations.CheckReturnValue;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.plugins.RxJavaPlugins;
import net.csini.spring.kafka.KafkaEntityObservable;
import net.csini.spring.kafka.KafkaEntityKey;
import net.csini.spring.kafka.exception.KafkaEntityException;

public class SimpleKafkaEntityObservable<T, K> extends Observable<T> implements DisposableBean, InitializingBean {

	private static final Logger LOGGER = LoggerFactory.getLogger(SimpleKafkaEntityObservable.class);

	/**
	 * Comma-delimited list of host:port pairs to use for establishing the initial
	 * connections to the Kafka cluster. Applies to all components unless
	 * overridden.
	 */
	@Value(value = "${spring.kafka.bootstrap-servers:localhost:9092}")
	private List<String> bootstrapServers = new ArrayList<>(Collections.singletonList("localhost:9092"));

	private String groupid;

	private Class<T> clazz;

	private Class<K> clazzKey;

	private final KafkaEntityPollingRunnable<T, K> pollingRunnable;

	private String beanName;

	SimpleKafkaEntityObservable(KafkaEntityObservable kafkaEntityObservable, String beanName)
			throws KafkaEntityException {
		this.clazz = kafkaEntityObservable.entity();
		this.groupid = /* getTopicName() + "-observer-" + */ beanName;

		// presents of @KafkaEntityKey is checked in KafkaEntityConfig
		for (Field field : this.clazz.getDeclaredFields()) {
			LOGGER.debug("    field  -> " + field.getName());
			if (field.isAnnotationPresent(KafkaEntityKey.class)) {
				field.setAccessible(true);
				this.clazzKey = (Class<K>) field.getType();
				break;
			}
		}

		subscribers = new AtomicReference<>(EMPTY);

		this.beanName = beanName;

		this.pollingRunnable = new KafkaEntityPollingRunnable<T, K>(groupid, clazz, clazzKey, subscribers,
				bootstrapServers, beanName);

		start();
	}

	public void start() throws KafkaEntityException {

		if (this.pollingRunnable.getStarted().get()) {
			LOGGER.warn("already started...");
			return;
		}

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("starting " + this.beanName + "...");
		}

		Thread pollingThread = new Thread(this.pollingRunnable);
		pollingThread.setName(beanName + "Thread");
		pollingThread.start();

		LOGGER.info("waiting the consumer to start in " + beanName + "Thread...");
		LocalDateTime then = LocalDateTime.now();
		while (!this.pollingRunnable.getStarted().get()) {
			if (ChronoUnit.SECONDS.between(then, LocalDateTime.now()) >= 300) {
//			break;
				throw new KafkaEntityException(beanName, "KafkaConsumer could not start in 300 sec.");
			}
		}
	}

	@Override
	public void afterPropertiesSet() throws Exception {

	}

	@Override
	public void destroy() throws Exception {

		LOGGER.warn("deleting a Consumer Group for " + this.beanName);

		this.pollingRunnable.getStopped().set(true);

		LOGGER.info("waiting polling to stop");
		LocalDateTime then = LocalDateTime.now();
		while (this.pollingRunnable.getStarted().get()) {
			if (ChronoUnit.SECONDS.between(then, LocalDateTime.now()) >= 20) {
				break;
			}
		}

		final Properties properties = new Properties();
		properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "1000");
		properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000");

		try (AdminClient adminClient = AdminClient.create(properties);) {
			String consumerGroupToBeDeleted = this.groupid;
			DeleteConsumerGroupsResult deleteConsumerGroupsResult = adminClient
					.deleteConsumerGroups(Arrays.asList(consumerGroupToBeDeleted));

			KafkaFuture<Void> resultFuture = deleteConsumerGroupsResult.all();
			resultFuture.get();
		} catch (Exception e) {
			// we cannot do anything at this point
			LOGGER.error(e.getMessage());
		}

	}

	/** The terminated indicator for the subscribers array. */
	@SuppressWarnings("rawtypes")
	static final KafkaEntityObservableDisposable[] TERMINATED = new KafkaEntityObservableDisposable[0];
	/** An empty subscribers array to avoid allocating it all the time. */
	@SuppressWarnings("rawtypes")
	static final KafkaEntityObservableDisposable[] EMPTY = new KafkaEntityObservableDisposable[0];

	/** The array of currently subscribed subscribers. */
	final AtomicReference<KafkaEntityObservableDisposable<T, K>[]> subscribers;

	/** The error, write before terminating and read after checking subscribers. */
	Throwable error;

	/**
	 * Constructs a SimpleKafkaObservableHandler.
	 * 
	 * @param <T> the value type
	 * @return the new SimpleKafkaObservableHandler
	 * @throws KafkaEntityException
	 */
	@CheckReturnValue
	@NonNull
	public static <T, K> SimpleKafkaEntityObservable<T, K> create(KafkaEntityObservable kafkaEntityObservable,
			String beanName) throws KafkaEntityException {
		return new SimpleKafkaEntityObservable<T, K>(kafkaEntityObservable, beanName);
	}

	@Override
	protected void subscribeActual(Observer<? super T> t) {
		KafkaEntityObservableDisposable<T, K> ps = new KafkaEntityObservableDisposable<>(t, this);
		t.onSubscribe(ps);
		if (add(ps)) {
			// if cancellation happened while a successful add, the remove() didn't work
			// so we need to do it again
			if (ps.isDisposed()) {
				remove(ps);
			}
		} else {
			Throwable ex = error;
			if (ex != null) {
				t.onError(ex);
			} else {
				t.onComplete();
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
	boolean add(KafkaEntityObservableDisposable<T, K> ps) {
		for (;;) {
			KafkaEntityObservableDisposable<T, K>[] a = subscribers.get();
			if (a == TERMINATED) {
				return false;
			}

			int n = a.length;
			@SuppressWarnings("unchecked")
			KafkaEntityObservableDisposable<T, K>[] b = new KafkaEntityObservableDisposable[n + 1];
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
	void remove(KafkaEntityObservableDisposable<T, K> ps) {
		for (;;) {
			KafkaEntityObservableDisposable<T, K>[] a = subscribers.get();
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

			KafkaEntityObservableDisposable<T, K>[] b;

			if (n == 1) {
				b = EMPTY;
			} else {
				b = new KafkaEntityObservableDisposable[n - 1];
				System.arraycopy(a, 0, b, 0, j);
				System.arraycopy(a, j + 1, b, j, n - j - 1);
			}
			if (subscribers.compareAndSet(a, b)) {
				return;
			}
		}
	}

	/**
	 * Wraps the actual subscriber, tracks its requests and makes cancellation to
	 * remove itself from the current subscribers array.
	 *
	 * @param <T> the value type
	 */
	static final class KafkaEntityObservableDisposable<T, K> extends AtomicBoolean implements Disposable {

		private static final long serialVersionUID = 3562861878281475070L;
		/** The actual subscriber. */
		final Observer<? super T> downstream;
		/** The subject state. */
		final SimpleKafkaEntityObservable<T, K> parent;

		/**
		 * Constructs a PublishSubscriber, wraps the actual subscriber and the state.
		 * 
		 * @param actual the actual subscriber
		 * @param parent the parent PublishProcessor
		 */
		KafkaEntityObservableDisposable(Observer<? super T> actual, SimpleKafkaEntityObservable<T, K> parent) {
			this.downstream = actual;
			this.parent = parent;
		}

		public void onNext(T t) {
			if (!get()) {
				downstream.onNext(t);
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
