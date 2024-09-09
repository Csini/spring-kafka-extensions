package net.csini.spring.kafka.observable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.ProducerRecord;
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
import net.csini.spring.kafka.config.KafkaEntityConfig;
import net.csini.spring.kafka.entity.Place;

@SpringBootTest(classes = { SpringKafkaEntityObservableTestApplication.class, KafkaEntityConfig.class,
		ExampleKafkaEntityObservableService.class, KafkaProducerConfig.class })
public class KafkaEntityObservablerTest {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaEntityObservablerTest.class);

	@Autowired
	private ExampleKafkaEntityObservableService obs;

	@Autowired
	private KafkaTemplate<String, Place> kafkaProducer;

	private String TOPIC = "net.csini.spring.kafka.entity.Place";

	@Test
	public void test_sendEvent() throws Exception {
		List<Place> eventList = new ArrayList<>();
		List<Place> eventListOther = new ArrayList<>();
		List<Place> eventListBefore = new ArrayList<>();
		List<Place> eventListThird = new ArrayList<>();

		int sendingFirstCount = 2;
		int sendingSecondCount = 3;
		
		CountDownLatch beforeCounter = new CountDownLatch(sendingFirstCount+sendingSecondCount);
		Observable<Place> productObservableBefore = obs.getPlaceObservableBefore();
		@NonNull
		Disposable connectBefore = productObservableBefore.subscribe(r -> {
			LOGGER.info("received-Before: " + r);
			eventListBefore.add(r);
			
			beforeCounter.countDown();
		});

		CountDownLatch receivedCounter = new CountDownLatch(sendingFirstCount);
		Observable<Place> productObservable = obs.getPlaceObservable();
		@NonNull
		Disposable connect1 = productObservable.subscribe(r -> {
			LOGGER.info("received: " + r);
			eventList.add(r);
			receivedCounter.countDown();
		});

		
		LOGGER.warn("waiting 30_0000");
		Thread.sleep(30_000);
		
		CountDownLatch sentCounter = new CountDownLatch(sendingFirstCount);
		// send events
		publishMessages(sentCounter, 100, sendingFirstCount);
		LOGGER.warn("sentCounter.await()");
//		Thread.sleep(20000);
		sentCounter.await();
		LOGGER.warn("waiting maximum 40_0000");
		receivedCounter.await(40, TimeUnit.SECONDS);
		connect1.dispose();

		Observable<Place> productObservableOther = obs.getPlaceObservableOther();
		@NonNull
		Disposable connect2 = productObservableOther.subscribe(r -> {
			LOGGER.info("received-other: " + r);
			eventListOther.add(r);
		});

		LOGGER.warn("waiting 30_0000");
		Thread.sleep(30_000);
		connect2.dispose();

		CountDownLatch thirdCounter = new CountDownLatch(sendingSecondCount);
		Observable<Place> productObservableThird = obs.getPlaceObservableThird();
		@NonNull
		Disposable connectThird = productObservableThird.subscribe(r -> {
			LOGGER.info("received-Third: " + r);
			eventListThird.add(r);
			thirdCounter.countDown();
		});

		CountDownLatch sentCounterSecond = new CountDownLatch(sendingSecondCount);
		// send events
		publishMessages(sentCounterSecond, 200, sendingSecondCount);
		LOGGER.warn("sentCounterSecond.await()");
//		Thread.sleep(20000);
		sentCounterSecond.await();
		
		LOGGER.warn("waiting maximum 10_0000");
		thirdCounter.await(10, TimeUnit.SECONDS);

		LOGGER.warn("waiting maximum 120_0000");
		beforeCounter.await(120, TimeUnit.SECONDS);

		System.out.println("eventList      : " + eventList);
		System.out.println("eventListOther : " + eventListOther);
		System.out.println("eventListBefore: " + eventListBefore);
		System.out.println("eventListThird : " + eventListThird);
		Assertions.assertEquals(sendingFirstCount, eventList.size(), "eventList");
		Assertions.assertEquals(0, eventListOther.size(), "eventListOther");
		Assertions.assertEquals(sendingFirstCount + sendingSecondCount, eventListBefore.size(), "eventListBefore");
		Assertions.assertEquals(sendingSecondCount, eventListThird.size(), "eventListThird");

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
