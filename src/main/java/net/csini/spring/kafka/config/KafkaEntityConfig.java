package net.csini.spring.kafka.config;

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
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.support.DefaultSingletonBeanRegistry;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;

import jakarta.annotation.PostConstruct;
import net.csini.spring.kafka.KafkaEntity;
import net.csini.spring.kafka.KafkaEntityObservable;
import net.csini.spring.kafka.KafkaEntityObserver;
import net.csini.spring.kafka.KafkaEntitySubject;
import net.csini.spring.kafka.Key;
import net.csini.spring.kafka.Topic;
import net.csini.spring.kafka.exception.KafkaEntityException;
import net.csini.spring.kafka.observable.SimpleKafkaEntityObservable;
import net.csini.spring.kafka.observer.SimpleKafkaEntityObserver;
import net.csini.spring.kafka.subject.SimpleKafkaEntitySubject;

@Configuration
public class KafkaEntityConfig {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaEntityConfig.class);

	@Value(value = "${spring.kafka.bootstrap-servers:localhost:9092}")
	private List<String> bootstrapServers = new ArrayList<>(Collections.singletonList("localhost:9092"));

	@Autowired
	private ApplicationContext applicationContext;

	private List<KafkaEntityException> errors = new ArrayList<>();

	@PostConstruct
	public String getAllBeans() {

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
				try {
					if (field.isAnnotationPresent(KafkaEntityObservable.class)) {

						registerKafkaEntityObservableBean(bean, field);

					} else if (field.isAnnotationPresent(KafkaEntityObserver.class)) {

						registerKafkaEntityObserverBean(bean, field);

					} else if (field.isAnnotationPresent(KafkaEntitySubject.class)) {

						registerKafkaEntitySubjectBean(bean, field);
					}
				} catch (KafkaEntityException e) {
					LOGGER.error("Error by registering spring-kafka-extension Bean", e);
					errors.add(e);
				} catch (Exception e) {
					KafkaEntityException kafkaEx = new KafkaEntityException(beanName, e);
					LOGGER.error("Error by registering spring-kafka-extension Bean", kafkaEx);
					errors.add(kafkaEx);
				}
			}
		}
		String string = result.toString();
		LOGGER.trace("postConstruct-getAllBeans(): " + string);

		return string;
	}

	private void registerKafkaEntityObservableBean(Object bean, Field field)
			throws KafkaEntityException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
		KafkaEntityObservable kafkaEntityObservable = field.getAnnotation(KafkaEntityObservable.class);

		Class entity = kafkaEntityObservable.entity();

		String newBeanName = bean.getClass().getName() + "#" + field.getName();
		LOGGER.debug("registering " + newBeanName + " as Observable");
		handleKafkaEntity(newBeanName, entity);

		Class<SimpleKafkaEntityObservable> clazz = SimpleKafkaEntityObservable.class;
		Method method = clazz.getMethod("create", KafkaEntityObservable.class, String.class);

		Object obj = method.invoke(null, kafkaEntityObservable, newBeanName);
		SimpleKafkaEntityObservable<?, ?> newInstance = (SimpleKafkaEntityObservable<?, ?>) obj;

		registerBean(bean, field, newBeanName, newInstance);
	}

	private void registerKafkaEntityObserverBean(Object bean, Field field)
			throws KafkaEntityException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
		KafkaEntityObserver kafkaEntityObserver = field.getAnnotation(KafkaEntityObserver.class);

		Class entity = kafkaEntityObserver.entity();

		String newBeanName = bean.getClass().getName() + "#" + field.getName();
		LOGGER.debug("registering " + newBeanName + " as Observer");
		handleKafkaEntity(newBeanName, entity);

		Class<SimpleKafkaEntityObserver> clazz = SimpleKafkaEntityObserver.class;
		Method method = clazz.getMethod("create", KafkaEntityObserver.class, String.class);

		Object obj = method.invoke(null, kafkaEntityObserver, newBeanName);
		SimpleKafkaEntityObserver<?, ?> newInstance = (SimpleKafkaEntityObserver<?, ?>) obj;

		registerBean(bean, field, newBeanName, newInstance);
	}

	private void registerKafkaEntitySubjectBean(Object bean, Field field)
			throws KafkaEntityException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
		KafkaEntitySubject kafkaEntitySubject = field.getAnnotation(KafkaEntitySubject.class);

		Class entity = kafkaEntitySubject.entity();

		String newBeanName = bean.getClass().getName() + "#" + field.getName();
		LOGGER.debug("registering " + newBeanName + " as Subject");
		handleKafkaEntity(newBeanName, entity);

		Class<SimpleKafkaEntitySubject> clazz = SimpleKafkaEntitySubject.class;
		Method method = clazz.getMethod("create", KafkaEntitySubject.class, String.class);

		Object obj = method.invoke(null, kafkaEntitySubject, newBeanName);
		SimpleKafkaEntitySubject<?, ?> newInstance = (SimpleKafkaEntitySubject<?, ?>) obj;

		registerBean(bean, field, newBeanName, newInstance);
	}

	private void registerBean(Object bean, Field field, String newBeanName, DisposableBean newInstance)
			throws IllegalAccessException {
		DefaultSingletonBeanRegistry registry = (DefaultSingletonBeanRegistry) applicationContext
				.getAutowireCapableBeanFactory();
		registry.registerDisposableBean(newBeanName, newInstance);
		field.setAccessible(true);
		field.set(bean, newInstance);
	}

	private void handleKafkaEntity(String beanName, Class entity) throws KafkaEntityException {
		if (!entity.isAnnotationPresent(KafkaEntity.class)) {
			throw new KafkaEntityException(beanName, entity.getName() + " must be a @KafkaEntity");
		}

		boolean foundKeyAnnotation = false;
		for (Field field : entity.getDeclaredFields()) {
			LOGGER.trace("    field  -> " + field.getName());
			if (field.isAnnotationPresent(Key.class)) {
				foundKeyAnnotation = true;
				break;
			}
		}

		if (!foundKeyAnnotation) {
			throw new KafkaEntityException(beanName, entity.getName() + " @Key is mandatory in @KafkaEntity");
		}

		Topic topic = extractTopic(entity);
		try {
			checkTopic(entity, topic);
		} catch (InterruptedException | ExecutionException e) {
			throw new KafkaEntityException(beanName, e);
		}
	}

//	private String getTopicName(Class entity) {
//		Topic topic = extractTopic(entity);
//		return getTopicName(entity, topic);
//	}

	private String getTopicName(Class entity, Topic topic) {
		if (topic != null) {
			return topic.name();
		}
		return entity.getName();
	}

	private Topic extractTopic(Class entity) {
		return (Topic) entity.getAnnotation(Topic.class);
	}

	private void checkTopic(Class entity, Topic topic) throws InterruptedException, ExecutionException, KafkaEntityException {
		Map<String, Object> conf = new HashMap<>();
		conf.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		conf.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
		try (AdminClient admin = AdminClient.create(conf);) {

			ListTopicsResult listTopics = admin.listTopics();
			Set<String> names = listTopics.names().get();
			LOGGER.debug("names: " + names);
			String topicName = getTopicName(entity, topic);
			boolean contains = names.contains(topicName);
			if (!contains) {
				
				throw new KafkaEntityException(topicName, "Topic " + topicName + " does not exist in " + bootstrapServers);
				
//				Map<String, String> configs = new HashMap<String, String>();
//				int partitions = 1;
//				Short replication = 1;
//				if (topic != null) {
//					partitions = topic.numPartitions();
//					replication = topic.replicationFactor();
//				}
//				NewTopic newTopic = new NewTopic(topicName, partitions, replication).configs(configs);
//				LOGGER.warn("autocreating " + newTopic);
//				CreateTopicsResult topicsResult = admin.createTopics(List.of(newTopic));
//
//				topicsResult.config(topicName).get();
//				
//				admin.describeTopics(List.of(topicName)).allTopicNames().get().get(topicName).partitions()
//						.forEach(p -> {
//							p.replicas().size();
//						});
//
////				LOGGER.warn("waiting 10_0000");
////				Thread.sleep(10_000);
			}
		}
	}

	public List<KafkaEntityException> getErrors() {
		return Collections.unmodifiableList(errors);
	}

	public void throwFirstError() throws KafkaEntityException {
		if (!errors.isEmpty()) {
			throw errors.get(0);
		}
	}

}