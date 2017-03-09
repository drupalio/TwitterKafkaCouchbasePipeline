package com.manolo.kafka.twitter;

import java.util.Properties;
import java.util.Arrays;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class KafkaTwitterCouchbaseConsumer {
		
	public static void main(String[] args) throws Exception {
		
		if(args.length == 0){
			System.out.println("Enter topic name");
			return;
		}
		
		String topicName = args[0];
		
		String clusterAddress = System.getProperty("clusterAddress", "localhost");
		String password = System.getProperty("password");
		String bucketName = System.getProperty("bucketName", "twitter");
		 
		Cluster cluster = CouchbaseCluster.create(clusterAddress);
		Bucket bucket = password == null ? cluster.openBucket(bucketName) : cluster.openBucket(bucketName, password); 		
		
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "test");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		
		KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(props);

		consumer.subscribe(Arrays.asList(topicName));

		System.out.println("Subscribed to topic " + topicName);

		while (true) {
			ConsumerRecords<byte[], byte[]> records = consumer.poll(100);
			for (ConsumerRecord<byte[], byte[]> record : records){
				try {
					bucket.insert(JsonDocument.create(new String(record.key(), "UTF-8"), JsonObject.fromJson(new String(record.value(), "UTF-8"))));
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
	
}