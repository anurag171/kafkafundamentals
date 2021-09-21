package com.anurag.kafka.consumer;

import java.util.HashMap;
import java.util.Map;

public class KafkaConsumerDemo {

	public static void main(String[] args) {
		String[] topics = new String[] {"my_first_iot_topic","second_topic"};
				
		ConsumerHelper.subscribe(ConsumerHelper.createConsumer(ConsumerHelper.init()), topics);
		

	}

}
