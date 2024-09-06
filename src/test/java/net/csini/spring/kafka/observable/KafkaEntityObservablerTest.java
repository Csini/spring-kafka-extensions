package net.csini.spring.kafka.observable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.observables.ConnectableObservable;
import net.csini.spring.kafka.config.KafkaEntityConfig;
import net.csini.spring.kafka.entity.Place;

@SpringBootTest(classes = { SpringKafkaEntityObservableTestApplication.class, KafkaEntityConfig.class,
		ExampleKafkaEntityObserver.class, KafkaProducerConfig.class })
public class KafkaEntityObservablerTest {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaEntityObservablerTest.class);

	@Autowired
	private ExampleKafkaEntityObserver observer;

	@Autowired
	private KafkaTemplate<String, Place> kafkaProducer;

	private String TOPIC = "net.csini.spring.kafka.entity.Place";

	@Test
	public void test_sendEvent() throws Exception {
		List<Place> eventList = new ArrayList<>();
		List<Place> eventListOther = new ArrayList<>();
		List<Place> eventListBefore = new ArrayList<>();
		List<Place> eventListThird = new ArrayList<>();

		Observable<Place> productObservableBefore = observer.getPlaceObservableBefore();
		@NonNull
		Disposable connectBefore = productObservableBefore.subscribe(r -> {
			LOGGER.info("received-Before: " + r);
			eventListBefore.add(r);
		});

		Observable<Place> productObservable = observer.getPlaceObservable();
		@NonNull
		Disposable connect1 = productObservable.subscribe(r -> {
			LOGGER.info("received: " + r);
			eventList.add(r);
		});

		int sendingFirstCount = 2;
		CountDownLatch sentCounter = new CountDownLatch(sendingFirstCount);
		// send events
		publishMessages(sentCounter, 100, sendingFirstCount);
		LOGGER.warn("sentCounter.await()");
//		Thread.sleep(20000);
		sentCounter.await();
		LOGGER.warn("waiting 40_0000");
		Thread.sleep(40_000);
		connect1.dispose();

		Observable<Place> productObservableOther = observer.getPlaceObservableOther();
		@NonNull
		Disposable connect2 = productObservableOther.subscribe(r -> {
			LOGGER.info("received-other: " + r);
			eventListOther.add(r);
		});

		LOGGER.warn("waiting 30_0000");
		Thread.sleep(30_000);
		connect2.dispose();

		Observable<Place> productObservableThird = observer.getPlaceObservableThird();
		@NonNull
		Disposable connectThird = productObservableThird.subscribe(r -> {
			LOGGER.info("received-Third: " + r);
			eventListThird.add(r);
		});

		int sendingSecondCount = 3;
		CountDownLatch sentCounterSecond = new CountDownLatch(sendingSecondCount);
		// send events
		publishMessages(sentCounterSecond, 200, sendingSecondCount);
		LOGGER.warn("sentCounterSecond.await()");
//		Thread.sleep(20000);
		sentCounterSecond.await();

		LOGGER.warn("waiting 40_0000");
		Thread.sleep(40_000);

		System.out.println("eventList      : " + eventList);
		System.out.println("eventListOther : " + eventListOther);
		System.out.println("eventListBefore: " + eventListBefore);
		System.out.println("eventListThird : " + eventListThird);
		Assertions.assertEquals(sendingFirstCount, eventList.size());
		Assertions.assertEquals(0, eventListOther.size());
		Assertions.assertEquals(sendingFirstCount + sendingSecondCount, eventListBefore.size());
		Assertions.assertEquals(sendingSecondCount, eventListThird.size());

		connectThird.dispose();
		connectBefore.dispose();
	}

	void publishMessages(CountDownLatch sentCounter, int i, int count) throws Exception {

		LOGGER.warn("publishMessages");

		for (int placeInt = 0; placeInt < count; placeInt++) {

			Place p2 = new Place("p" + (i++) + "id");
			ProducerRecord<String, Place> p2Record = new ProducerRecord<>(TOPIC, p2.id(), p2);
			sendPlace(sentCounter, p2Record);
		}
	}

	private void sendPlace(CountDownLatch sentCounter, ProducerRecord<String, Place> record)
			throws InterruptedException, ExecutionException {
		CompletableFuture<SendResult<String, Place>> sendingIntoTheFuture = kafkaProducer.send(record);

		sendingIntoTheFuture.get();
		while (!sendingIntoTheFuture.isDone()) {
			LOGGER.info("waiting");
		}
		sentCounter.countDown();
	}

}
