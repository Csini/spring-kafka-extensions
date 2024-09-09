package net.csini.spring.kafka.observer;

import java.time.Duration;
import java.time.temporal.TemporalUnit;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Observer;
import net.csini.spring.kafka.config.KafkaEntityConfig;
import net.csini.spring.kafka.entity.City;

@SpringBootTest(classes = { SpringKafkaEntityObserverTestApplication.class, KafkaEntityConfig.class,
		ExampleKafkaEntityObserverService.class, KafkaTemplateConfig.class })
public class KafkaEntityObserverTest {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaEntityObserverTest.class);

	@Autowired
	private ExampleKafkaEntityObserverService obs;

	public static final String TOPIC = "net.csini.spring.kafka.entity.City";

	@Test
	public void test_sendEvent() throws Exception {
		List<City> eventList = obs.getInput();

		Observer<City> productObservable = obs.getCityObserver();

		Observable.fromIterable(eventList).subscribe(productObservable);
		
		LOGGER.warn("waiting maximum 30_0000");
		obs.getReceivedCounter().await(30, TimeUnit.SECONDS);

		obs.assertCount();
	}

}
