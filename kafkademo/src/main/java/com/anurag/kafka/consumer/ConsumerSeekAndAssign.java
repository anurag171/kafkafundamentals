package com.anurag.kafka.consumer;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerSeekAndAssign {

	public static void main(String[] args) {
		Properties properties = ConsumerHelper.init();
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, "seekandassign");
		KafkaConsumer<String,String> consumer = ConsumerHelper.createConsumer(properties);
		String[] topics = new String[] {"my_first_iot_topic","second_topic"};
		int Partition = 0; 
		ConsumerHelper.seekAndAssign(topics,consumer,Partition,11L,10);
	}

}
