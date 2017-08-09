package com.custom.flume.interceptor;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.Map.Entry;

import javax.jms.JMSException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.jms.JMSMessageConverter;
import org.springframework.util.StringUtils;


public class FilterFull_SMS implements JMSMessageConverter {

	private ArrayList<String> requiredListFull = new ArrayList<String>();

	{
		requiredListFull.add("sdate");
		requiredListFull.add("longmsg_base_mid");
		requiredListFull.add("fullmsg");
		requiredListFull.add("ctime");
	}
	public List<Event> convert(javax.jms.Message message) throws JMSException {
		Event event = new SimpleEvent();
		Enumeration propertyNames = message.getPropertyNames();
		Map<String, Object> body = new HashMap<String, Object>();
		while (propertyNames.hasMoreElements()) {
			String name = (String) propertyNames.nextElement();
			if (requiredListFull.indexOf(name) >= 0)
				body.put(name, message.getObjectProperty(name));
		}
		if(!body.containsKey("sdate"))
		{
			body.put("sdate", "0");
		}
		if(!body.containsKey("longmsg_base_mid"))
		{
			body.put("longmsg_base_mid", "0");
		}
		if(!body.containsKey("fullmsg"))
		{
			body.put("fullmsg", "");
		}
		if(!body.containsKey("ctime"))
		{
			body.put("ctime", "");
		}
		LinkedHashMap<String, Object> testFullMsg = new LinkedHashMap<String, Object>();
		testFullMsg.put("sdate",body.get("sdate"));
		testFullMsg.put("longmsg_base_mid",body.get("longmsg_base_mid"));
		testFullMsg.put("fullmsg",body.get("fullmsg"));
		testFullMsg.put("ctime",body.get("ctime"));
		for(Entry<String, Object> entry:testFullMsg.entrySet())
	     {
			/*if (entry.getValue().toString().isEmpty()) {
				testFullMsg.put(entry.getKey(), "NULL");
			}*/
			if(entry.getKey()=="ctime")
			{
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			TimeZone timeZone=TimeZone.getTimeZone("IST");
			sdf.setTimeZone(timeZone);
			Date date= new Date();
			String formattedDate = sdf.format(date);
			testFullMsg.put("ctime", formattedDate);
			}
			testFullMsg.put(entry.getKey(), entry.getValue().toString().replaceAll("\\r|\\n", " "));
	     }
		
	/*	String valueString = String.join(",".trim(),testFullMsg.values().toString().trim());
		System.out.println(valueString);*/
		//String csvFormated = StringUtils.arrayToCommaDelimitedString(testFullMsg.values().toArray());
		List<Event> events = new ArrayList<Event>(1);
		//System.out.println("Formated Body : '" + csvFormated.toString().replace("[","").replace("]","").trim()+ "'");
		StringBuilder sb1 = new StringBuilder();
		Iterator it = testFullMsg.values().iterator();
		sb1.append(it.next());
		//if (!it.hasNext()) sb2.append("");
		while (it.hasNext()) sb1.append("^").append(it.next());
		System.out.println(sb1);
		event.setBody(sb1.toString().getBytes());
		events.add(event);
		String i=null;
		return events;
		
	}
	public static class Builder implements JMSMessageConverter.Builder {
		public void configure(Context context) {
		}

		public JMSMessageConverter build(Context arg0) {
			return new FilterFull_SMS();
		}
	}
}
