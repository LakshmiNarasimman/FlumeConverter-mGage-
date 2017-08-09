package com.custom.flume.interceptor;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.JMSException;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.jms.JMSMessageConverter;

public class Test_FioranoData implements JMSMessageConverter{

	public List<Event> convert(javax.jms.Message message) throws JMSException {
		Enumeration propertyNames = message.getPropertyNames();
		Map<String, Object> body = new HashMap<String, Object>();
		while (propertyNames.hasMoreElements()) {
			String name = (String) propertyNames.nextElement();
			body.put(name, message.getObjectProperty(name));
		}
		List<Event> events = new ArrayList<Event>(1);
		Event event = new SimpleEvent();
		event.setBody(body.values().toString().replaceAll(","," ").replaceAll("\\r|\\n", " ").trim().getBytes());
		events.add(event);
		return events;
	}
	public static class Builder implements JMSMessageConverter.Builder {
		public void configure(Context context) {
		}

		public JMSMessageConverter build(Context arg0) {
			return new Test_FioranoData();
		}
	}

}
