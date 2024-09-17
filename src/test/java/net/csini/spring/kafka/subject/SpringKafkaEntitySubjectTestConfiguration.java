package net.csini.spring.kafka.subject;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import net.csini.spring.kafka.entity.Student;
import net.csini.spring.kafka.entity.User;
import net.csini.spring.kafka.entity.util.TopicUtil;

@Configuration
public class SpringKafkaEntitySubjectTestConfiguration implements InitializingBean{

	@Autowired
	private TopicUtil topicUtil;
	
	@Override
	public void afterPropertiesSet() throws Exception {

		// !!!
//		topicUtil.createTopic(Product.class.getName());
		topicUtil.createTopic("PRODUCT");
		
		topicUtil.createTopic(User.class.getName());
		
		topicUtil.createTopic(Student.class.getName());
	}
}
