package net.csini.spring.kafka.observable;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import net.csini.spring.kafka.entity.util.TopicUtil;

@Configuration
public class SpringKafkaEntityObservableTestConfiguration implements InitializingBean{

	@Autowired
	private TopicUtil topicUtil;
	
	@Override
	public void afterPropertiesSet() throws Exception {

		topicUtil.createTopic(KafkaEntityObservableTest.TOPIC);
	}

}
