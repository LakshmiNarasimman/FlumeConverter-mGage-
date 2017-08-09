package com.custom.flume.interceptor;

import java.util.HashMap;

import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import com.google.common.base.Splitter;
import com.google.gson.Gson;

public class EventBodyInterceptor implements Interceptor {
	
	 private Map<String, String> splitToMap(String in) {
	        return Splitter.on(",").withKeyValueSeparator("=").split(in);
	    }
	 
	public Event intercept(Event event) {
		//byte[] body=event.getBody().toString().getBytes();
		
		Map<String, String> body1 = new HashMap<String, String>();
	    String body = new String(event.getBody());
		body1=splitToMap(body);
	  
		for (Map.Entry<String, String> entry : body1.entrySet()) {
		   // System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
			event.setBody(entry.getValue().toString().getBytes());
		}
		
	    
		return event;
	}

	public List<Event> intercept(List<Event> events) {
		for (Event event:events) {
		      intercept(event);
		    }
		    return events;
	}
	
	public static class Builder implements Interceptor.Builder
    {
        public void configure(Context context) {
        }

        public Interceptor build() {
            return new EventBodyInterceptor();
        }
    }

	public void close() {
		
	}

	public void initialize() {
		
	}

	

}
