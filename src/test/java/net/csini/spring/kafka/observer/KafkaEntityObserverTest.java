package net.csini.spring.kafka.observer;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Observer;
import net.csini.spring.kafka.config.KafkaEntityConfig;
import net.csini.spring.kafka.entity.City;
import net.csini.spring.kafka.entity.util.TopicUtil;

@SpringBootTest(classes = { TopicUtil.class, SpringKafkaEntityObserverTestConfiguration.class,
		SpringKafkaEntityObserverTestApplication.class, KafkaEntityConfig.class, KafkaTemplateConfig.class,
		ExampleKafkaEntityObserverService.class })
//@TestPropertySource("/application.yml")
//@EmbeddedKafka(partitions = 1, bootstrapServersProperty = "spring.kafka.bootstrap-servers", brokerProperties = {
//		"log.dir=target/kafka-log", "auto.create.topics.enable=${kafka.broker.topics-enable:true}" })
public class KafkaEntityObserverTest {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaEntityObserverTest.class);

	@Autowired
	private ExampleKafkaEntityObserverService obs;

	public static final String TOPIC = "net.csini.spring.kafka.entity.City";

	@Test
	public void test_sendCity() throws Exception {

		List<City> eventList = obs.getInput();

		Observer<City> productObservable = obs.getCityObserver();

		LOGGER.warn("waiting 30_0000");
		Thread.sleep(30_000);

		Observable.fromIterable(eventList).subscribe(productObservable);

		LOGGER.warn("waiting maximum 30_0000");
		obs.getReceivedCounter().await(30, TimeUnit.SECONDS);

		Assertions.assertEquals(obs.getInput().size(), obs.getCount());
	}
}
