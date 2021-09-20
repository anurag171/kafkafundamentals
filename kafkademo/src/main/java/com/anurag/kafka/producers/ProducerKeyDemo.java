package com.anurag.kafka.producers;

import java.util.concurrent.ThreadLocalRandom;

public class ProducerKeyDemo {

	public static void main(String[] args) {		
		int count =1;
		while(count<=5) {
			String value = String.valueOf(ThreadLocalRandom.current().nextInt(1, 100000));
			ProducerHelper.sendMessageWithKey(ProducerHelper.createProducer(ProducerHelper.init()),
					"my_first_iot_topic", "key_"+value,"Message_"+value);
			count++;
		}
		System.out.println("Done");

	}

}
