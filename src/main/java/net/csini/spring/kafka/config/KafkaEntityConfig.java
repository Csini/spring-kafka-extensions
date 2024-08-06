package net.csini.spring.kafka.config;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.SingletonBeanRegistry;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;

import io.reactivex.rxjava3.core.Observable;
import jakarta.annotation.PostConstruct;
import net.csini.spring.kafka.KafkaEntity;
import net.csini.spring.kafka.KafkaEntityException;
import net.csini.spring.kafka.KafkaEntityObservable;
import net.csini.spring.kafka.KafkaEntityProducer;
import net.csini.spring.kafka.observable.SimpleKafkaObservableHandler;
import net.csini.spring.kafka.producer.SimpleKafkaProducer;

@Configuration
public class KafkaEntityConfig {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaEntityConfig.class);

	@Autowired
	private ApplicationContext applicationContext;

	@PostConstruct
	public String getAllBeans() throws NoSuchMethodException, SecurityException, InstantiationException,
			IllegalAccessException, IllegalArgumentException, InvocationTargetException, KafkaEntityException {

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

					KafkaEntityObservable kafkaEntityObserverable = field.getAnnotation(KafkaEntityObservable.class);
					LOGGER.debug("registering " + field.getName() + " in " + bean.getClass() + " as Observable");

					Class entity = kafkaEntityObserverable.entity();

					if (!entity.isAnnotationPresent(KafkaEntity.class)) {
						throw new KafkaEntityException(entity.getName() + " must be a @KafkaEntity");
					}

					Class<? extends SimpleKafkaObservableHandler> creatorClass = SimpleKafkaObservableHandler.class;
					Constructor<? extends SimpleKafkaObservableHandler> creatorCtor = creatorClass
							.getConstructor(KafkaEntityObservable.class);
					SimpleKafkaObservableHandler<?, ?> handler = creatorCtor.newInstance(kafkaEntityObserverable);
//					beanRegistry.registerSingleton(bean.getClass().getName() + "." + field.getName(), newInstance);
//					Observable<?> newInstance = Observable.create(handler);
					
					applicationContext.getAutowireCapableBeanFactory().autowireBean(handler);
					
					Observable<?> newInstance = handler.createObservable();

					applicationContext.getAutowireCapableBeanFactory().autowireBean(newInstance);

//					ObservableOnSubscribe<?> handler = emitter -> {
//
//						//TODO
////					     Future<Object> future = executor.schedule(() -> {
////					          emitter.onNext("Hello");
////					          emitter.onNext("World");
////					          emitter.onComplete();
////					          return null;
////					     }, 1, TimeUnit.SECONDS);
//
////					     emitter.setCancellable(() -> future.cancel(false));
////					     emitter.
//					};
//
//					
//					applicationContext.getAutowireCapableBeanFactory().autowireBean(newInstance);
//					
					field.setAccessible(true);
					field.set(bean, newInstance);
				}
			}
		}
		String string = result.toString();
		LOGGER.trace("postConstruct-getAllBeans(): " + string);

		return string;
	}

}