package net.csini.spring.kafka.config;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.SingletonBeanRegistry;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;

import jakarta.annotation.PostConstruct;
import net.csini.spring.kafka.KafkaEntityProducer;
import net.csini.spring.kafka.producer.SimpleKafkaProducerImpl;

@Configuration
public class KafkaEntityConfig {

	@Autowired
	private ApplicationContext context;

	public String findBootClass() {
//		Map<String, Object> annotatedBeans = context.getBeansWithAnnotation(SpringBootApplication.class);
//		return annotatedBeans.isEmpty() ? null : annotatedBeans.values().toArray()[0].getClass().getName();
		return "";
	}

//	@Bean
//	KafkaEntityConfigBean kafkaEntityConfigBean() {
//		return new KafkaEntityConfigBean(findBootClass());
//	}

	@Autowired
	private ApplicationContext applicationContext;

	@PostConstruct
	public String getAllBeans() throws NoSuchMethodException, SecurityException, InstantiationException,
			IllegalAccessException, IllegalArgumentException, InvocationTargetException {

		ConfigurableApplicationContext configContext = (ConfigurableApplicationContext) applicationContext;
		SingletonBeanRegistry beanRegistry = configContext.getBeanFactory();

		StringBuilder result = new StringBuilder();
		String[] allBeans = applicationContext.getBeanDefinitionNames();
		for (String beanName : allBeans) {
			result.append(beanName).append("\n");

			if ("kafkaEntityConfig".equals(beanName)) {
				continue;
			}

			Object bean = context.getBean(beanName);
			System.out.println(" bean -> " + bean.getClass());
			for (Field field : bean.getClass().getDeclaredFields()) {
				System.out.println("    field  -> " + field.getName());
				if (field.isAnnotationPresent(KafkaEntityProducer.class)) {
					
					KafkaEntityProducer kafkaEntityProducer = field.getAnnotation(KafkaEntityProducer.class);
					System.out.println("registering " + field.getName() + " in " + bean.getClass());

					Class<? extends SimpleKafkaProducerImpl> creatorClass = SimpleKafkaProducerImpl.class;
					Constructor<? extends SimpleKafkaProducerImpl> creatorCtor = creatorClass
							.getConstructor(Class.class);
					SimpleKafkaProducerImpl<?> newInstance = creatorCtor.newInstance(kafkaEntityProducer.entity());
					beanRegistry.registerSingleton(bean.getClass().getName() + "." + field.getName(), newInstance);

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