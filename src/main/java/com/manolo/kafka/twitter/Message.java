package com.manolo.kafka.twitter;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.TimeZone;

import twitter4j.HashtagEntity;
import twitter4j.Status;
import twitter4j.UserMentionEntity;

public class Message {
	
	private String text;
	private String userId;
	private String date;
	private String source;
	private long id;
	private String hashtags[];
	private String usermentions[];
	
	public Message(){
		
	}
	
	public Message(Status status){
		
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
		date = dateFormat.format(status.getCreatedAt());
		userId = status.getUser().getName();
		text = status.getText();
		source = status.getSource();
		id = status.getId();		
		HashtagEntity htags[] = status.getHashtagEntities();
		hashtags = new String[htags.length];
		for(int i = 0; i < hashtags.length; i++){
			hashtags[i] = htags[i].getText();
		}
		UserMentionEntity ume[] = status.getUserMentionEntities();
		usermentions = new String[ume.length];
		for(int i = 0; i < usermentions.length; i++){
			usermentions[i] = ume[i].getName();
		}
		
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String[] getHashtags() {
		return hashtags;
	}

	public void setHashtags(String[] hashtags) {
		this.hashtags = hashtags;
	}

	public String[] getUsermentions() {
		return usermentions;
	}

	public void setUsermentions(String[] usermentions) {
		this.usermentions = usermentions;
	}

	@Override
	public String toString() {
		return "Message [text=" + text + ", userId=" + userId + ", date=" + date + ", source=" + source + ", id=" + id
				+ ", hashtags=" + Arrays.toString(hashtags) + ", usermentions=" + Arrays.toString(usermentions) + "]";
	}
	
	
}
