package net.csini.spring.kafka.config;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.SingletonBeanRegistry;
import org.springframework.beans.factory.support.DefaultSingletonBeanRegistry;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;

import jakarta.annotation.PostConstruct;
import net.csini.spring.kafka.KafkaEntity;
import net.csini.spring.kafka.KafkaEntityException;
import net.csini.spring.kafka.KafkaEntityObservable;
import net.csini.spring.kafka.KafkaEntityObserver;
import net.csini.spring.kafka.KafkaEntityProducer;
import net.csini.spring.kafka.Topic;
import net.csini.spring.kafka.observable.SimpleKafkaEntityObservable;
import net.csini.spring.kafka.observer.SimpleKafkaEntityObserver;
import net.csini.spring.kafka.producer.SimpleKafkaProducer;

@Configuration
public class KafkaEntityConfig {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaEntityConfig.class);

	@Value(value = "${spring.kafka.bootstrap-servers:localhost:9092}")
	private List<String> bootstrapServers = new ArrayList<>(Collections.singletonList("localhost:9092"));

	@Autowired
	private ApplicationContext applicationContext;

	@PostConstruct
	public String getAllBeans() throws NoSuchMethodException, SecurityException, InstantiationException,
			IllegalAccessException, IllegalArgumentException, InvocationTargetException, KafkaEntityException,
			InterruptedException, ExecutionException {

		ConfigurableApplicationContext configContext = (ConfigurableApplicationContext) applicationContext;
		SingletonBeanRegistry beanRegistry = configContext.getBeanFactory();

		StringBuilder result = new StringBuilder();
		String[] allBeans = applicationContext.getBeanDefinitionNames();
		for (String beanName : allBeans) {
			result.append(beanName).append("\n");

			if ("kafkaEntityConfig".equals(beanName)) {
				continue;
			}

			Object bean = applicationContext.getBean(beanName);
			LOGGER.trace(" bean -> " + bean.getClass());
			for (Field field : bean.getClass().getDeclaredFields()) {
				LOGGER.trace("    field  -> " + field.getName());
				if (field.isAnnotationPresent(KafkaEntityProducer.class)) {

					KafkaEntityProducer kafkaEntityProducer = field.getAnnotation(KafkaEntityProducer.class);
					LOGGER.debug("registering " + field.getName() + " in " + bean.getClass());

					Class entity = kafkaEntityProducer.entity();

					if (!entity.isAnnotationPresent(KafkaEntity.class)) {
						throw new KafkaEntityException(entity.getName() + " must be a @KafkaEntity");
					}

					autoCreateTopic(entity);

					Class<? extends SimpleKafkaProducer> creatorClass = SimpleKafkaProducer.class;
					Constructor<? extends SimpleKafkaProducer> creatorCtor = creatorClass
							.getConstructor(KafkaEntityProducer.class);
					SimpleKafkaProducer<?, ?> newInstance = creatorCtor.newInstance(kafkaEntityProducer);
//					beanRegistry.registerSingleton(bean.getClass().getName() + "." + field.getName(), newInstance);

					applicationContext.getAutowireCapableBeanFactory().autowireBean(newInstance);

					newInstance.afterPropertiesSet();

					field.setAccessible(true);
					field.set(bean, newInstance);
				} else if (field.isAnnotationPresent(KafkaEntityObservable.class)) {

					KafkaEntityObservable kafkaEntityObservable = field.getAnnotation(KafkaEntityObservable.class);
					LOGGER.debug("registering " + field.getName() + " in " + bean.getClass() + " as Observable");

					Class entity = kafkaEntityObservable.entity();

					if (!entity.isAnnotationPresent(KafkaEntity.class)) {
						throw new KafkaEntityException(entity.getName() + " must be a @KafkaEntity");
					}

					autoCreateTopic(entity);

					Class<SimpleKafkaEntityObservable> clazz = SimpleKafkaEntityObservable.class;
					Method method = clazz.getMethod("create", KafkaEntityObservable.class, String.class);

					String newBeanName = bean.getClass().getName() + "#" + field.getName();
					Object obj = method.invoke(null, kafkaEntityObservable, newBeanName);
					SimpleKafkaEntityObservable<?, ?> newInstance = (SimpleKafkaEntityObservable<?, ?>) obj;
					DefaultSingletonBeanRegistry registry = (DefaultSingletonBeanRegistry) applicationContext
							.getAutowireCapableBeanFactory();
					registry.registerDisposableBean(newBeanName, newInstance);
					field.setAccessible(true);
					field.set(bean, newInstance);
				} else if (field.isAnnotationPresent(KafkaEntityObserver.class)) {

					KafkaEntityObserver kafkaEntityObserver = field.getAnnotation(KafkaEntityObserver.class);
					LOGGER.debug("registering " + field.getName() + " in " + bean.getClass() + " as Observer");

					Class entity = kafkaEntityObserver.entity();

					if (!entity.isAnnotationPresent(KafkaEntity.class)) {
						throw new KafkaEntityException(entity.getName() + " must be a @KafkaEntity");
					}

					autoCreateTopic(entity);

					Class<SimpleKafkaEntityObserver> clazz = SimpleKafkaEntityObserver.class;
					Method method = clazz.getMethod("create", KafkaEntityObserver.class, String.class);

					String newBeanName = bean.getClass().getName() + "#" + field.getName();
					Object obj = method.invoke(null, kafkaEntityObserver, newBeanName);
					SimpleKafkaEntityObserver<?, ?> newInstance = (SimpleKafkaEntityObserver<?, ?>) obj;
					DefaultSingletonBeanRegistry registry = (DefaultSingletonBeanRegistry) applicationContext
							.getAutowireCapableBeanFactory();
					registry.registerDisposableBean(newBeanName, newInstance);
					field.setAccessible(true);
					field.set(bean, newInstance);
				}
			}
		}
		String string = result.toString();
		LOGGER.trace("postConstruct-getAllBeans(): " + string);

		return string;
	}

	private String getTopicName(Class entity) {
		Topic topic = extractTopic(entity);
		if (topic != null) {
			return topic.name();
		}
		return entity.getName();
	}

	private Topic extractTopic(Class entity) {
		return (Topic) entity.getAnnotation(Topic.class);
	}

	private void autoCreateTopic(Class entity) throws InterruptedException, ExecutionException {
		String topic = getTopicName(entity);
		Map<String, Object> conf = new HashMap<>();
		conf.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		conf.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
		try (AdminClient admin = AdminClient.create(conf);) {

			ListTopicsResult listTopics = admin.listTopics();
			Set<String> names = listTopics.names().get();
			boolean contains = names.contains(topic);
			if (!contains) {
				List<NewTopic> topicList = new ArrayList<NewTopic>();
				Map<String, String> configs = new HashMap<String, String>();
				int partitions = 1;
				Short replication = 1;
				NewTopic newTopic = new NewTopic(topic, partitions, replication).configs(configs);
				topicList.add(newTopic);
				admin.createTopics(topicList);
			}
		}
	}
}