package net.csini.spring.kafka.config;

import static org.assertj.core.api.Assertions.entry;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.SingletonBeanRegistry;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;

import jakarta.annotation.PostConstruct;
import net.csini.spring.kafka.KafkaEntity;
import net.csini.spring.kafka.KafkaEntityException;
import net.csini.spring.kafka.KafkaEntityProducer;
import net.csini.spring.kafka.Topic;
import net.csini.spring.kafka.producer.SimpleKafkaProducerImpl;

@Configuration
public class KafkaEntityConfig {

//	@Bean
//	KafkaEntityConfigBean kafkaEntityConfigBean() {
//		return new KafkaEntityConfigBean(findBootClass());
//	}

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
			System.out.println(" bean -> " + bean.getClass());
			for (Field field : bean.getClass().getDeclaredFields()) {
				System.out.println("    field  -> " + field.getName());
				if (field.isAnnotationPresent(KafkaEntityProducer.class)) {
					
					KafkaEntityProducer kafkaEntityProducer = field.getAnnotation(KafkaEntityProducer.class);
					System.out.println("registering " + field.getName() + " in " + bean.getClass());

					Class entity = kafkaEntityProducer.entity();
					

					if(!entity.isAnnotationPresent(KafkaEntity.class)) {
						throw new KafkaEntityException(entity.getName() + " must be a @KafkaEntity");
					}
					
					Class<? extends SimpleKafkaProducerImpl> creatorClass = SimpleKafkaProducerImpl.class;
					Constructor<? extends SimpleKafkaProducerImpl> creatorCtor = creatorClass
							.getConstructor(KafkaEntityProducer.class);
					SimpleKafkaProducerImpl<?, ?> newInstance = creatorCtor.newInstance(kafkaEntityProducer);
//					beanRegistry.registerSingleton(bean.getClass().getName() + "." + field.getName(), newInstance);

					applicationContext.getAutowireCapableBeanFactory().autowireBean(newInstance);
					
					field.setAccessible(true);
					field.set(bean, newInstance);
				}
			}
		}
		String string = result.toString();
		System.out.println("xxxxxxx: " + string);

		return string;
	}

}