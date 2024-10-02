package net.csini.spring.kafka.config;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.support.DefaultSingletonBeanRegistry;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;

import net.csini.spring.kafka.KafkaEntity;
import net.csini.spring.kafka.KafkaEntityKey;
import net.csini.spring.kafka.KafkaEntityObservable;
import net.csini.spring.kafka.KafkaEntityObserver;
import net.csini.spring.kafka.KafkaEntitySubject;
import net.csini.spring.kafka.exception.KafkaEntityException;
import net.csini.spring.kafka.observable.SimpleKafkaEntityObservable;
import net.csini.spring.kafka.observer.SimpleKafkaEntityObserver;
import net.csini.spring.kafka.subject.SimpleKafkaEntitySubject;
import net.csini.spring.kafka.util.KafkaEntityUtil;

/**
 * This Bean creates and registers all KafkaEntity Bean at starting a
 * spring-boot app
 * 
 * @author Csini
 */
@Configuration
public class KafkaEntityConfig implements InitializingBean, DisposableBean {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaEntityConfig.class);

	private List<String> bootstrapServers = new ArrayList<>(Collections.singletonList("localhost:9092"));

	private ApplicationContext applicationContext;

	private Set<String> beanNames = new HashSet<>();

	/**
	 * use this constructor if you want to create an instance manually
	 * 
	 * @param applicationContext spring applicationcontext
	 * @param bootstrapServers   kafka bootstrap URL, default is looking at spirng
	 *                           application property
	 *                           spring.kafka.bootstrap-servers, if it is empty than
	 *                           "localhost:9092"
	 */
	public KafkaEntityConfig(@Autowired ApplicationContext applicationContext,
			@Value(value = "${spring.kafka.bootstrap-servers:localhost:9092}") List<String> bootstrapServers) {
		super();
		this.applicationContext = applicationContext;
		if (bootstrapServers != null && !bootstrapServers.isEmpty()) {
			this.bootstrapServers = Collections.unmodifiableList(bootstrapServers);
		}
	}

	private List<KafkaEntityException> errors = new ArrayList<>();

	@Override
	public void afterPropertiesSet() {

		StringBuilder result = new StringBuilder();
		String[] allBeans = applicationContext.getBeanDefinitionNames();
		DefaultSingletonBeanRegistry registry = (DefaultSingletonBeanRegistry) applicationContext
				.getAutowireCapableBeanFactory();
		for (String beanName : allBeans) {
			result.append(beanName).append("\n");

			if ("kafkaEntityConfig".equals(beanName)) {
				continue;
			}

			Object bean = applicationContext.getBean(beanName);
			if (LOGGER.isTraceEnabled()) {
				LOGGER.trace(" bean -> " + bean.getClass());
			}
			for (Field field : bean.getClass().getDeclaredFields()) {
				if (LOGGER.isTraceEnabled()) {
					LOGGER.trace("    field  -> " + field.getName());
				}
				try {

					if (field.isAnnotationPresent(KafkaEntityObservable.class)
							|| field.isAnnotationPresent(KafkaEntityObserver.class)
							|| field.isAnnotationPresent(KafkaEntitySubject.class)) {
						String newBeanName = bean.getClass().getName() + "#" + field.getName();
						if (!registry.containsSingleton(newBeanName)) {
							DisposableBean newInstance = null;
							if (field.isAnnotationPresent(KafkaEntityObservable.class)) {

								KafkaEntityObservable kafkaEntityObservable = field
										.getAnnotation(KafkaEntityObservable.class);

								newInstance = registerKafkaEntityObservableBean(bean, newBeanName,
										kafkaEntityObservable);

							} else if (field.isAnnotationPresent(KafkaEntityObserver.class)) {
								KafkaEntityObserver kafkaEntityObserver = field
										.getAnnotation(KafkaEntityObserver.class);

								newInstance = registerKafkaEntityObserverBean(bean, newBeanName, kafkaEntityObserver);

							} else if (field.isAnnotationPresent(KafkaEntitySubject.class)) {
								KafkaEntitySubject kafkaEntitySubject = field.getAnnotation(KafkaEntitySubject.class);

								newInstance = registerKafkaEntitySubjectBean(bean, newBeanName, kafkaEntitySubject);
							}
							registerBean(registry, bean, field, newBeanName, newInstance);
						}
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
		if (LOGGER.isTraceEnabled()) {
			LOGGER.trace("postConstruct-getAllBeans(): " + string);
		}

	}

	private DisposableBean registerKafkaEntityObservableBean(Object bean, String newBeanName,
			KafkaEntityObservable kafkaEntityObservable)
			throws KafkaEntityException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {

		Class entity = kafkaEntityObservable.entity();

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("registering " + newBeanName + " as Observable");
		}
		handleKafkaEntity(newBeanName, entity);

		Class<SimpleKafkaEntityObservable> clazz = SimpleKafkaEntityObservable.class;
		Method method = clazz.getMethod("create", KafkaEntityObservable.class, String.class);

		Object obj = method.invoke(null, kafkaEntityObservable, newBeanName);
		SimpleKafkaEntityObservable<?, ?> newInstance = (SimpleKafkaEntityObservable<?, ?>) obj;

		return newInstance;
	}

	private DisposableBean registerKafkaEntityObserverBean(Object bean, String newBeanName,
			KafkaEntityObserver kafkaEntityObserver)
			throws KafkaEntityException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {

		Class entity = kafkaEntityObserver.entity();

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("registering " + newBeanName + " as Observer");
		}
		handleKafkaEntity(newBeanName, entity);

		Class<SimpleKafkaEntityObserver> clazz = SimpleKafkaEntityObserver.class;
		Method method = clazz.getMethod("create", KafkaEntityObserver.class, String.class);

		Object obj = method.invoke(null, kafkaEntityObserver, newBeanName);
		SimpleKafkaEntityObserver<?, ?> newInstance = (SimpleKafkaEntityObserver<?, ?>) obj;

		return newInstance;
	}

	private DisposableBean registerKafkaEntitySubjectBean(Object bean, String newBeanName,
			KafkaEntitySubject kafkaEntitySubject)
			throws KafkaEntityException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {

		Class entity = kafkaEntitySubject.entity();

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("registering " + newBeanName + " as Subject");
		}
		handleKafkaEntity(newBeanName, entity);

		Class<SimpleKafkaEntitySubject> clazz = SimpleKafkaEntitySubject.class;
		Method method = clazz.getMethod("create", KafkaEntitySubject.class, String.class);

		Object obj = method.invoke(null, kafkaEntitySubject, newBeanName);
		SimpleKafkaEntitySubject<?, ?> newInstance = (SimpleKafkaEntitySubject<?, ?>) obj;

		return newInstance;
	}

	private void registerBean(DefaultSingletonBeanRegistry registry, Object bean, Field field, String newBeanName,
			DisposableBean newInstance) throws IllegalAccessException {
		registry.registerSingleton(newBeanName, newInstance);
		registry.registerDisposableBean(newBeanName, newInstance);
		field.setAccessible(true);
		field.set(bean, newInstance);
		beanNames.add(newBeanName);
	}

	private void handleKafkaEntity(String beanName, Class entity) throws KafkaEntityException {
		if (!entity.isAnnotationPresent(KafkaEntity.class)) {
			throw new KafkaEntityException(beanName, entity.getName() + " must be a @KafkaEntity");
		}

		boolean foundKeyAnnotation = false;
		for (Field field : entity.getDeclaredFields()) {
			if (LOGGER.isTraceEnabled()) {
				LOGGER.trace("    field  -> " + field.getName());
			}
			if (field.isAnnotationPresent(KafkaEntityKey.class)) {
				foundKeyAnnotation = true;
				break;
			}
		}

		if (!foundKeyAnnotation) {
			throw new KafkaEntityException(beanName, entity.getName() + " @" + KafkaEntityKey.class.getSimpleName()
					+ " is mandatory in @" + KafkaEntity.class.getSimpleName());
		}

		try {
			checkTopic(entity);
		} catch (InterruptedException | ExecutionException e) {
			throw new KafkaEntityException(beanName, e);
		}
	}

	private void checkTopic(Class entity) throws InterruptedException, ExecutionException, KafkaEntityException {
		Map<String, Object> conf = new HashMap<>();
		conf.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		conf.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
		try (AdminClient admin = AdminClient.create(conf);) {

			ListTopicsResult listTopics = admin.listTopics();
			Set<String> names = listTopics.names().get();
			if (LOGGER.isTraceEnabled()) {
				LOGGER.trace("names: " + names);
			}
			String topicName = KafkaEntityUtil.getTopicName(entity);
			boolean contains = names.contains(topicName);
			if (!contains) {
				throw new KafkaEntityException(topicName,
						"Topic " + topicName + " does not exist in " + bootstrapServers);
			}
		}
	}

	/**
	 * get the exceptions, which were created while afterPropertiesSet()
	 * 
	 * @return list of KafkaEntityException
	 */
	public List<KafkaEntityException> getErrors() {
		return Collections.unmodifiableList(errors);
	}

	/**
	 * throws the first error, which happenened at afterPropertiesSet() if any
	 * happpened
	 * 
	 * @throws KafkaEntityException the first exception
	 */
	public void throwFirstError() throws KafkaEntityException {
		if (!errors.isEmpty()) {
			throw errors.get(0);
		}
	}

	@Override
	public void destroy() throws Exception {

		DefaultSingletonBeanRegistry registry = (DefaultSingletonBeanRegistry) applicationContext
				.getAutowireCapableBeanFactory();

		beanNames.stream().forEach(beanName -> {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("destroying " + beanName);
			}
			registry.destroySingleton(beanName);
		});
	}

}