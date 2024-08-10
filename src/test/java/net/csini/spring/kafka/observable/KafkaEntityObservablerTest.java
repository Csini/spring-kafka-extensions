package net.csini.spring.kafka.observable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

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
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.observables.ConnectableObservable;
import net.csini.spring.kafka.config.KafkaEntityConfig;
import net.csini.spring.kafka.entity.Place;
import net.csini.spring.kafka.entity.Product;

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


		ConnectableObservable<Place> productObservable = observer.getPlaceObservable();
		productObservable.subscribe(r -> {
			LOGGER.info("received: " + r);
			eventList.add(r);
		});
		
		@NonNull
		Disposable connect1 = productObservable.connect();
		
		CountDownLatch sentCounter = new CountDownLatch(2);
		//send events
		publishMessages(sentCounter, 100);
		
//		Thread.sleep(20000);
		sentCounter.await();
		Thread.sleep(60000);
		connect1.dispose();
		
		ConnectableObservable<Place> productObservableOther = observer.getPlaceObservableOther();
		productObservable.subscribe(r -> {
			LOGGER.info("received-other: " + r);
			eventListOther.add(r);
		});
		
		@NonNull
		Disposable connect2 = productObservableOther.connect();
		
		Thread.sleep(60000);
		connect2.dispose();
		
		System.out.println("eventList     : " + eventList);
		System.out.println("eventListOther: " + eventListOther);
		Assertions.assertEquals(2, eventList.size());
		Assertions.assertEquals(0, eventListOther.size());
		
	}

	void publishMessages(CountDownLatch sentCounter, int i) throws Exception {

		LOGGER.debug("publishMessages");

		Place p1 = new Place("p"+i+"id");
		ProducerRecord<String, Place> p1Record = new ProducerRecord<>(TOPIC, p1.id(), p1);
		sendPlace(sentCounter, p1Record);
		
		Place p2 = new Place("p"+(i++)+"id");
		ProducerRecord<String, Place> p2Record = new ProducerRecord<>(TOPIC, p2.id(), p2);
		sendPlace(sentCounter, p2Record);
	}

	private void sendPlace(CountDownLatch sentCounter, ProducerRecord<String, Place> record)
			throws InterruptedException, ExecutionException {
		CompletableFuture<SendResult<String, Place>> sendingIntoTheFuture = kafkaProducer.send(record);
		
		sendingIntoTheFuture.get();
		while(!sendingIntoTheFuture.isDone()) {
			LOGGER.info("waiting");
		}
		sentCounter.countDown();
	}

}
