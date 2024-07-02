package net.csini.spring.kafka.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.springframework.beans.BeansException;
import org.springframework.beans.FatalBeanException;
import org.springframework.beans.factory.annotation.AnnotatedBeanDefinition;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.beans.factory.support.GenericBeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.annotation.MergedAnnotation;
import org.springframework.core.annotation.MergedAnnotations;
import org.springframework.core.annotation.MergedAnnotations.SearchStrategy;
import org.springframework.core.type.filter.AnnotationTypeFilter;

import net.csini.spring.kafka.EnableKafkaEntities;
import net.csini.spring.kafka.KafkaEntityProducer;
import net.csini.spring.kafka.producer.SimpleKafkaProducerImpl;

public class KafkaEntityConfigBean implements BeanDefinitionRegistryPostProcessor {

	private String bootClass;

	public KafkaEntityConfigBean(String bootClass) {
		this.bootClass = bootClass;
	}

	@Override
	public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) throws BeansException {

//		https://docs.spring.io/spring/docs/current/javadoc-api/org/springframework/core/annotation/AnnotationUtils.html
//		Class<?> clazz = AnnotationUtils.findAnnotationDeclaringClass(Target.class, null);

		String basePackage = "net.csini.kafka";
//		try {
//			basePackage = MergedAnnotations.from(Class.forName(bootClass), SearchStrategy.SUPERCLASS)
//					.get(EnableKafkaEntities.class, MergedAnnotation::isDirectlyPresent)
//					.getString(EnableKafkaEntities.BASEPACKAGES);
//		} catch (NoSuchElementException | ClassNotFoundException e) {
//			throw new FatalBeanException("BootClass nor found", e);
//		}

		System.out.println("basePackage: " + basePackage);

//		if(!basePackageOptional.isPresent() || basePackageOptional.isEmpty()) {
//			throw new FatalBeanException("EnableKafkaEntities Annotation - basepackage is missing");
//		}

		ClassPathScanningCandidateComponentProvider provider = new ClassPathScanningCandidateComponentProvider(false);
		provider.addIncludeFilter(new AnnotationTypeFilter(KafkaEntityProducer.class));

		Set<BeanDefinition> beanDefs = provider.findCandidateComponents(basePackage);
		List<String> annotatedBeans = new ArrayList<>();
		for (BeanDefinition bd : beanDefs) {
			if (bd instanceof AnnotatedBeanDefinition) {
				Map<String, Object> annotAttributeMap = ((AnnotatedBeanDefinition) bd).getMetadata()
						.getAnnotationAttributes(KafkaEntityProducer.class.getCanonicalName());
				annotatedBeans.add(annotAttributeMap.get("name").toString());
			}
		}

//		Assertions.assertEquals(1, annotatedBeans.size());
//		Assertions.assertEquals("SampleAnnotatedClass", annotatedBeans.get(0));

		// bean creation

		for (String annotatedBean : annotatedBeans) {
			GenericBeanDefinition bd = new GenericBeanDefinition();
			bd.setBeanClass(SimpleKafkaProducerImpl.class);
//		bd.getPropertyValues().add("strProp", "my string property");
			System.out.println("annotatedBean: " + annotatedBean);
			registry.registerBeanDefinition(annotatedBean, bd);
		}

	}

	@Override
	public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
		// no op
	}
}