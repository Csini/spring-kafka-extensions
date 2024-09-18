package net.csini.spring.kafka.observer;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Observer;
import net.csini.spring.kafka.config.KafkaEntityConfig;
import net.csini.spring.kafka.entity.City;
import net.csini.spring.kafka.entity.util.TopicUtil;

@SpringBootTest(classes = { TopicUtil.class, SpringKafkaEntityObserverTestConfiguration.class,
		SpringKafkaEntityObserverTestApplication.class, KafkaEntityConfig.class, KafkaTemplateConfig.class,
		CityListener.class, ExampleKafkaEntityObserverService.class, ExampleKafkaEntityObserverNoTransactionService.class })
//@TestPropertySource("/application.yml")
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = { "listeners=PLAINTEXT://localhost:9092", "port=9092",
		"offsets.topic.replication.factor=1", "offset.storage.replication.factor=1",
		"transaction.state.log.replication.factor=1", "transaction.state.log.min.isr=1" })
public class KafkaEntityObserverTest {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaEntityObserverTest.class);

	@Autowired
	private ExampleKafkaEntityObserverService obs;
	
	@Autowired
	private ExampleKafkaEntityObserverNoTransactionService obsNoTr;
	
	@Autowired
	private CityListener cityListener;

	public static final String TOPIC = "net.csini.spring.kafka.entity.City";

	@Test
	public void test_sendCity() throws Exception {

		List<City> eventList = obs.getInput();
		cityListener.init(eventList.size());

		Observer<City> productObservable = obs.getCityObserver();

		LOGGER.warn("waiting 30_0000");
		Thread.sleep(30_000);

		Observable.fromIterable(eventList).subscribe(productObservable);
		

		LOGGER.warn("waiting maximum 30_0000");
		cityListener.getReceivedCounter().await(30, TimeUnit.SECONDS);

		Assertions.assertEquals(obs.getInput().size(), cityListener.getCount());
	}
	
	@Test
	public void test_sendCity_no_transaction() throws Exception {

		List<City> eventList = obsNoTr.getInput();
		cityListener.init(eventList.size());

		Observer<City> productObservable = obsNoTr.getCityObserver();

		LOGGER.warn("waiting 30_0000");
		Thread.sleep(30_000);

		Observable.fromIterable(eventList).subscribe(productObservable);

		LOGGER.warn("waiting maximum 30_0000");
		cityListener.getReceivedCounter().await(30, TimeUnit.SECONDS);

		Assertions.assertEquals(obsNoTr.getInput().size(), cityListener.getCount());
	}
}
