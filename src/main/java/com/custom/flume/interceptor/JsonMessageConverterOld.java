package com.custom.flume.interceptor;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.ObjectMessage;
import javax.jms.TextMessage;
import javax.xml.*;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.event.JSONEvent;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.jms.JMSMessageConverter;
import org.json.JSONArray;
import org.json.JSONObject;



import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

public class JsonMessageConverterOld implements JMSMessageConverter {

	private final Type listType = new TypeToken<List<JSONEvent>>() {

	}.getType();
	
	private final Gson gson = new GsonBuilder().disableHtmlEscaping().create();
	private String charset = "UTF-8";
private	ArrayList<String> ignoreList = new ArrayList<String>();

	{
		ignoreList .add("ESBX__SYSTEM__CARRY_FORWARD_CONTEXT");
		ignoreList .add("ESBX__SYSTEM__OUT_TIME");
		ignoreList .add("ESBX__SYSTEM__INPUT_PORT");
	}
	
	public List<Event> convert(javax.jms.Message message) throws JMSException {
		//Event e=new JSONEvent();
//				ignoreList .add("ESBX__SYSTEM__CARRY_FORWARD_CONTEXT");
//				ignoreList .add("ESBX__SYSTEM__CARRY_FORWARD_CONTEXT");
		 Event event = new SimpleEvent();		
		 Enumeration propertyNames = message.getPropertyNames();
		 Map<String, Object> body = new HashMap<String, Object>();
		 //HashMap<String, Object> values = new HashMap<String, Object>();
         //Event event = new JSONEvent();
		 while(propertyNames.hasMoreElements())
		 {
			 String name = (String) propertyNames.nextElement();
			 
			 if(ignoreList.indexOf(name)>=0)
				 continue;
			 //values.put(name, message.getObjectProperty(name));
			  //String value = message.getStringProperty(name);
		      body.put(name, message.getObjectProperty(name));
		 }
		 List<Event> events = new ArrayList<Event>(1);

		 System.out.println("Values : '"+body+"'");
		// Gson gson = new Gson();
		// String json = gson.toJson(values, HashMap.class);
		// event.setBody(json.getBytes());
		 
		 //final Map<String, String> headers = new HashMap<String, String>();
		 //headers.put("eventId", UUID.randomUUID().toString());
		 //event.setHeaders(headers);
		
		 
		 //JSONObject obj=new JSONObject(json);
		 
		 //JSONArray jsonArr=new JSONArray();
		// jsonArr.put(obj);
		//System.out.println(jsonArr);
//		 
//		 System.out.println("JSON String '"+json+"'");
		 
//		if(headers.containsKey("ESBX__SYSTEM__CARRY_FORWARD_CONTEXT"))
//		{
//		headers.remove("ESBX__SYSTEM__CARRY_FORWARD_CONTEXT");
//		}
//		else if(headers.containsKey("ESBX__SYSTEM__OUT_TIME"))
//		{
//		headers.remove("ESBX__SYSTEM__OUT_TIME");
//		}
//		
//		else if(headers.containsKey("ESBX__SYSTEM__INPUT_PORT"))
//		{
//		headers.remove("ESBX__SYSTEM__INPUT_PORT");
//		}
		//
		// else
		// {
		// //Preconditions.checkState(message instanceof
//		 ObjectMessage,"Excepted a text message,but the message received"+"was not Text");
		//List<JSONEvent> events=gson.fromJson(json, listType);
		 //List<JSONEvent> events=gson.fromJson(((MapMessage)message).getJMSType(), listType);
//		 List<JSONEvent> events=gson.fromJson(jsonArr.toString(), listType);
//		// // TODO Auto-generated method stub
//		 for(JSONEvent e:events)
//		 {
//			 System.out.println(e.getHeaders().toString());
//			 System.out.println(e.getBody().toString());
//		 }
		events.add(event);
		return events;
		// }
		//Event e = null;
		//Map<String, String> headers = e.getHeaders();
		//for (String key : headers.keySet()) {
		//	System.out.println("key :'" + key + "', value : '"
		//			+ headers.get(key));
		//}
		
		//message.getStringProperty("");
		
	}

//	private List<Event> convertToNormalEvents(List<JSONEvent> events) {
//		// Map<String, String> headers = ((Event) events).getHeaders();
//
//		List<Event> newEvents = new ArrayList<Event>(events.size());
//		System.out.println("Convert to Normal Event"+newEvents);
//
//		for (JSONEvent e : events) {
//
//			//((JSONEvent) e).setCharset(charset);
//			//e.setCharset(charset);
//			System.out.println(e.getHeaders());
//			System.out.println(e.getBody());
//			//System.out.println(((Event) e).getHeaders().toString()+""+((Event) e).getBody().toString());
//			newEvents.add(EventBuilder.withBody(e.getBody(), e.getHeaders()));
//			
//
//		}
//
//		return newEvents;
//	}

	public static class Builder implements JMSMessageConverter.Builder {
		public void configure(Context context) {
			// TODO Auto-generated method stub
		
		}

		public JMSMessageConverter build(Context arg0) {
			// TODO Auto-generated method stub
			return new JsonMessageConverterOld();
		}
	}

}
