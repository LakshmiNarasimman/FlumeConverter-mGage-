package com.custom.flume.interceptor;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.TimeZone;

import javax.jms.JMSException;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.jms.JMSMessageConverter;

public class Test_SmsSubmission implements JMSMessageConverter {

	static LinkedHashMap<String, Object> defaultMap;
	static {
		TimeZone timeZone2 = TimeZone.getTimeZone("IST");
		TimeZone.setDefault(timeZone2);

		defaultMap = createDefaultHashMap();
	}
	// Creating the required list
	private ArrayList<String> requiredListSub = new ArrayList<String>();
	{
		requiredListSub.add("sdate");
		requiredListSub.add("esmeaddr");
		requiredListSub.add("send");
		requiredListSub.add("cust_send");
		requiredListSub.add("dest");
		requiredListSub.add("msg");
		requiredListSub.add("stime");
		requiredListSub.add("alpha");
		requiredListSub.add("udhi");
		requiredListSub.add("dcs");
		requiredListSub.add("mid");
		requiredListSub.add("routeid");
		requiredListSub.add("dlr_type");
		requiredListSub.add("priority");
		requiredListSub.add("apptype");
		requiredListSub.add("credits");
		requiredListSub.add("bypass_mcc_mnc");
		requiredListSub.add("file_id");
		requiredListSub.add("wid");
		requiredListSub.add("operator");
		requiredListSub.add("circle");
		requiredListSub.add("cust_mid");
		requiredListSub.add("cust_ip");
		requiredListSub.add("pcode");
		requiredListSub.add("acode");
		requiredListSub.add("pid");
		requiredListSub.add("aid");
		requiredListSub.add("msg_source");
		requiredListSub.add("ie_nos");
		requiredListSub.add("ie_sref");
		requiredListSub.add("sche_date_time");
		requiredListSub.add("fps_id");
		requiredListSub.add("msg_type");
		requiredListSub.add("acc_type");
		requiredListSub.add("msgtag");
		requiredListSub.add("pattern_id");
		requiredListSub.add("platform");
		requiredListSub.add("ctime");
		requiredListSub.add("reason");
		requiredListSub.add("status_flag");
		requiredListSub.add("status_id");
		requiredListSub.add("udh");
		requiredListSub.add("sts");
		requiredListSub.add("actual_ts");
		requiredListSub.add("smsc_id");
		requiredListSub.add("bindata");
		requiredListSub.add("featurecd");
		requiredListSub.add("msgclass");
		requiredListSub.add("logicid");
		requiredListSub.add("vp");
		requiredListSub.add("country");
		requiredListSub.add("longmsg_base_mid");
		requiredListSub.add("tag1");
		requiredListSub.add("tag2");
		requiredListSub.add("tag3");
		requiredListSub.add("tag4");
		requiredListSub.add("tag5");
		requiredListSub.add("message_journey");
		requiredListSub.add("filename");
		requiredListSub.add("plat_latency_org_millis");
		requiredListSub.add("plat_latency_sla_millis");
		requiredListSub.add("interim");
		requiredListSub.add("component_latency");
		//requiredListSub.add("dtime_format");

	}

	public List<Event> convert(javax.jms.Message message) throws JMSException {
		
		Enumeration propertyNames = message.getPropertyNames();

		LinkedHashMap<String, Object> body = (LinkedHashMap<String, Object>) defaultMap.clone();

		while (propertyNames.hasMoreElements()) {
			String name = (String) propertyNames.nextElement();
			if (requiredListSub.indexOf(name) >= 0){
				body.put(name, message.getObjectProperty(name));
			}
		}
 
		{

			{
			String stime=	(String) body.get("stime");
			if(!"".equals(stime)){
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
				sdf.setLenient(false);
				String stsformatter1 = (String) body.get("stime");
				try {
					Date formattedDate1 = sdf.parse(stsformatter1);
					String strDate1 = sdf.format(formattedDate1);
					body.put("stime", strDate1);
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
			}

			{
				String sdatetime=	(String) body.get("sche_date_time");
				if(!"".equals(sdatetime)){
					SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
					sdf2.setLenient(false);
					String stsformatter1 = (String) body.get("sche_date_time");
					try {
						Date formattedDate1 = sdf2.parse(stsformatter1);
						String strDate1 = sdf2.format(formattedDate1);
						body.put("sche_date_time", strDate1);
					} catch (ParseException e) {
						e.printStackTrace();
					}
				}
				
			}

			{
				String ststime= body.get("sts").toString();
				if(!"".equals(ststime)){
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
					sdf.setLenient(false);
					String stsformatter = body.get("sts").toString();
					Long convertedLong = Long.parseLong(stsformatter);
					String formattedDate = sdf.format(convertedLong);
					try {
						Date formattedDate1 = sdf.parse(formattedDate);
						String strDate1 = sdf.format(formattedDate1);
						body.put("sts", strDate1);
					} catch (ParseException e) {
						e.printStackTrace();
					}
				}
				
			}
			{
				String acttime=	body.get("actual_ts").toString();
				if(!"".equals(acttime)){
					SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
					sdf1.setLenient(false);
					String stsformatter1 = body.get("actual_ts").toString();
					Long convertedLong1 = Long.parseLong(stsformatter1);
					String formattedDate = sdf1.format(new Date(convertedLong1));
					try {
						Date formattedDate1 = sdf1.parse(formattedDate);
						String strDate1 = sdf1.format(formattedDate1);
						body.put("actual_ts", strDate1);
					} catch (ParseException e) {
						e.printStackTrace();
					}
					
				}
				
			}

		}

		for (Entry<String, Object> entry : body.entrySet()) {
			
			if (entry.getKey() == "ctime") {
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				TimeZone timeZone = TimeZone.getTimeZone("IST");
				sdf.setTimeZone(timeZone);
				Date date = new Date();
				String formattedDate = sdf.format(date);
				body.put("ctime", formattedDate);
			}
			body.put(entry.getKey(), entry.getValue().toString().replaceAll("\\r|\\n", " "));
		}

		List<Event> events = new ArrayList<Event>(1);
		StringBuilder sb1 = new StringBuilder();
		Iterator it = body.values().iterator();
		sb1.append(it.next());
		while (it.hasNext())
			sb1.append("^").append(it.next());


		Event event = new SimpleEvent();
		event.setBody(sb1.toString().getBytes());
		events.add(event);
		return events;
	}

	private static LinkedHashMap<String, Object> createDefaultHashMap() {
		LinkedHashMap<String, Object> map = new LinkedHashMap<String, Object>();

		map.put("sdate", "");
		map.put("esmeaddr", "0");
		map.put("send", "");
		map.put("cust_send", "");
		map.put("dest", "0");
		map.put("msg", "");
		map.put("stime", "");
		map.put("alpha", "0");
		map.put("udhi", "0");
		map.put("dcs", "0");
		map.put("mid", "0");
		map.put("routeid", "");
		map.put("dlr_type", "0");
		map.put("priority", "0");
		map.put("apptype", "");
		map.put("credits", "0");
		map.put("bypass_mcc_mnc", "0");
		map.put("file_id", "");
		map.put("wid", "0");
		map.put("operator", "");
		map.put("circle", "");
		map.put("cust_mid", "");
		map.put("cust_ip", "");
		map.put("pcode", "");
		map.put("acode", "");
		map.put("pid", "0");
		map.put("aid", "0");
		map.put("msg_source", "");
		map.put("ie_nos", "0");
		map.put("ie_sref", "0");
		map.put("sche_date_time", "");
		map.put("fps_id", "0");
		map.put("msg_type", "0");
		map.put("acc_type", "0");
		map.put("msgtag", "");
		map.put("pattern_id", "0");
		map.put("platform", "");
		map.put("ctime", "");
		map.put("reason", "");
		map.put("status_flag", "");
		map.put("status_id", "");
		map.put("udh", "");
		map.put("sts", "");
		map.put("actual_ts", "");
		map.put("smsc_id", "");
		map.put("bindata", "");
		map.put("featurecd", "");
		map.put("msgclass", "");
		map.put("logicid", "0");
		map.put("vp", "0");
		map.put("country", "");
		map.put("longmsg_base_mid", "0");
		map.put("tag1", "");
		map.put("tag2", "");
		map.put("tag3", "");
		map.put("tag4", "");
		map.put("tag5", "");
		map.put("message_journey", "");
		map.put("filename", "");
		map.put("plat_latency_org_millis", "0");
		map.put("plat_latency_sla_millis", "0");
		map.put("interim", "0");
		map.put("component_latency", "");

		return map;

	}

	public static class Builder implements JMSMessageConverter.Builder {
		public void configure(Context context) {
		}

		public JMSMessageConverter build(Context arg0) {
			return new Test_SmsSubmission();
		}
	}
}
