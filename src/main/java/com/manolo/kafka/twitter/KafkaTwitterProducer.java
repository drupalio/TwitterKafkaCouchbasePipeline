package com.manolo.kafka.twitter;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

import twitter4j.*;
import twitter4j.conf.*;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.gson.Gson;

public class KafkaTwitterProducer {

	public static void main(String[] args) throws Exception {
		
		LinkedBlockingQueue<Message> queue = new LinkedBlockingQueue<Message>(1000);

		if(args.length < 5){
			System.out.println(
					"Usage: KafkaTwitterProducer <twitter-consumer-key>" + 
							"<twitter-consumer-secret> <twitter-access-token>" + 
							"<twitter-access-token-secret>" + 
					"<topic-name> <twitter-search-keywords>");
			return;
		}

		String consumerKey = args[0].toString();
		String consumerSecret = args[1].toString();
		String accessToken = args[2].toString();
		String accessTokenSecret = args[3].toString();
		String topicName = args[4].toString();
		int timeConnected = Integer.parseInt(args[5].toString());
		String[] arguments = args.clone();
		String[] keyWords = Arrays.copyOfRange(arguments, 6, arguments.length);

		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
		.setOAuthConsumerKey(consumerKey)
		.setOAuthConsumerSecret(consumerSecret)
		.setOAuthAccessToken(accessToken)
		.setOAuthAccessTokenSecret(accessTokenSecret);

		TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
		
		StatusListener listener = new StatusListener() {

			@Override
			public void onStatus(Status status) {      
				queue.offer(new Message(status));
			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
				// System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
			}

			@Override
			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
				// System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
			}

			@Override
			public void onScrubGeo(long userId, long upToStatusId) {
				// System.out.println("Got scrub_geo event userId:" + userId + "upToStatusId:" + upToStatusId);
			}      

			@Override
			public void onStallWarning(StallWarning warning) {
				// System.out.println("Got stall warning:" + warning);
			}

			@Override
			public void onException(Exception ex) {
				ex.printStackTrace();
			}
		};
		
		twitterStream.addListener(listener);

		FilterQuery query = new FilterQuery().track(keyWords);
		twitterStream.filter(query);

		Thread.sleep(5000);

		//Add Kafka producer config settings
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);

		props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

		Producer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(props);
		int i = 0;

		long ini = System.currentTimeMillis();
		while((System.currentTimeMillis() - ini) < (timeConnected*1000)) {
			Message msg = queue.poll();
			if (msg == null) {
				Thread.sleep(100);
			}
			else {
				Gson gson = new Gson();
				producer.send(new ProducerRecord<byte[], byte[]>(topicName, (msg.getUserId() + "::" + msg.getId()).getBytes("UTF-8"), gson.toJson(msg, Message.class).getBytes("UTF-8")));
			}
		}
		
		producer.close();
		Thread.sleep(5000);
		twitterStream.shutdown();
		
	}

}