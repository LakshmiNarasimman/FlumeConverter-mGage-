package com.custom.flume.interceptor;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;

import javax.jms.JMSException;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.jms.JMSMessageConverter;


public class FilterSms_Submission implements JMSMessageConverter {

	//Creating the required list
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
		requiredListSub.add("dtime_format");
	
	}
	
	public List<Event> convert(javax.jms.Message message) throws JMSException {
		Enumeration propertyNames = message.getPropertyNames();
		Map<String, Object> body = new HashMap<String, Object>();
		while (propertyNames.hasMoreElements()) {
			String name = (String) propertyNames.nextElement();
			if (requiredListSub.indexOf(name) >= 0)
				body.put(name, message.getObjectProperty(name));
		}
		
		//Validating the fields of message key
		{
			if(!body.containsKey("sdate"))
			{
				body.put("sdate", "");
			}
			if(!body.containsKey("esmeaddr"))
			{
				body.put("esmeaddr", "0");
			}
			if(!body.containsKey("send"))
			{
				body.put("send", "");
			}
			if(!body.containsKey("cust_send"))
			{
				body.put("cust_send", "");
			}
			if(!body.containsKey("dest"))
			{
				body.put("dest", "0");
			}
			if(!body.containsKey("msg"))
			{
				body.put("msg", "");
			}
			System.out.println("''stime'" + body.get("stime"));
			if(!body.containsKey("stime"))
			{
				body.put("stime", "");
			}
			else
			{
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
				TimeZone timeZone1=TimeZone.getTimeZone("IST");
				sdf.setTimeZone(timeZone1);
				String stsformatter1 = body.get("stime").toString();
				//String formattedDate1 = sdf.format(stsformatter1);
				
				try {
					Date formattedDate1 = sdf.parse(stsformatter1);
					String strDate1 = sdf.format(formattedDate1);
					body.put("stime", strDate1);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
			
			if(!body.containsKey("alpha"))
			{
				body.put("alpha", "0");
			}
			if(!body.containsKey("udhi"))
			{
				body.put("udhi", "0");
			}
			if(!body.containsKey("dcs"))
			{
				body.put("dcs", "0");
			}
			if(!body.containsKey("mid"))
			{
				body.put("mid", "0");
			}
			if(!body.containsKey("routeid"))
			{
				body.put("routeid", "");
			}
			if(!body.containsKey("dlr_type"))
			{
				body.put("dlr_type", "0");
			}
			if(!body.containsKey("priority"))
			{
				body.put("priority", "0");
			}
			if(!body.containsKey("apptype"))
			{
				body.put("apptype", "");
			}
			if(!body.containsKey("credits"))
			{
				body.put("credits", "0");
			}
			if(!body.containsKey("bypass_mcc_mnc"))
			{
				body.put("bypass_mcc_mnc", "0");
			}
			if(!body.containsKey("file_id"))
			{
				body.put("file_id", "");
			}
			if(!body.containsKey("wid"))
			{
				body.put("wid", "0");
			}
			if(!body.containsKey("operator"))
			{
				body.put("operator", "");
			}
			if(!body.containsKey("circle"))
			{
				body.put("circle", "");
			}
			if(!body.containsKey("cust_mid"))
			{
				body.put("cust_mid", "");
			}
			if(!body.containsKey("cust_ip"))
			{
				body.put("cust_ip", "");
			}
			if(!body.containsKey("pcode"))
			{
				body.put("pcode", "");
			}
			if(!body.containsKey("acode"))
			{
				body.put("acode", "");
			}
			if(!body.containsKey("pid"))
			{
				body.put("pid", "0");
			}
			if(!body.containsKey("aid"))
			{
				body.put("aid", "0");
			}
			if(!body.containsKey("msg_source"))
			{
				body.put("msg_source", "");
			}
			if(!body.containsKey("ie_nos"))
			{
				body.put("ie_nos", "0");
			}
			if(!body.containsKey("ie_sref"))
			{
				body.put("ie_sref", "0");
			}
			System.out.println("''sche_date_time'" + body.get("sche_date_time"));
			if(!body.containsKey("sche_date_time"))
			{
				body.put("sche_date_time", "");
			}
			else
			{
				SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				TimeZone timeZone2=TimeZone.getTimeZone("IST");
				sdf2.setTimeZone(timeZone2);
				String stsformatter2 = body.get("actual_ts").toString();
				String formattedDate2 = sdf2.format(stsformatter2);
				body.put("actual_ts", formattedDate2);
			}
			
			if(!body.containsKey("fps_id"))
			{
				body.put("fps_id", "0");
			}
			if(!body.containsKey("msg_type"))
			{
				body.put("msg_type", "0");
			}
			if(!body.containsKey("acc_type"))
			{
				body.put("acc_type", "0");
			}
			if(!body.containsKey("msgtag"))
			{
				body.put("msgtag", "");
			}
			if(!body.containsKey("pattern_id"))
			{
				body.put("pattern_id", "0");
			}
			if(!body.containsKey("platform"))
			{
				body.put("platform", "");
			}
			if(!body.containsKey("ctime"))
			{
				body.put("ctime", "");
			}
			if(!body.containsKey("reason"))
			{
				body.put("reason", "");
			}
			if(!body.containsKey("status_flag"))
			{
				body.put("status_flag", "");
			}
			if(!body.containsKey("status_id"))
			{
				body.put("status_id", "");
			}
			if(!body.containsKey("udh"))
			{
				body.put("udh", "");
			}
			System.out.println("''sts'" + body.get("sts"));
			if(!body.containsKey("sts"))
			{
				body.put("sts", "");
			}
			else
			{
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
				TimeZone timeZone=TimeZone.getTimeZone("IST");
				sdf.setTimeZone(timeZone);
				String stsformatter = body.get("sts").toString();
				Long convertedLong = Long.parseLong(stsformatter);
				String formattedDate = sdf.format(convertedLong);
				body.put("sts", formattedDate);
			}
			System.out.println("''actual_ts'" + body.get("actual_ts"));
			if(!body.containsKey("actual_ts"))
			{
				body.put("actual_ts", "");
			}
			else
			{
				SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
				TimeZone timeZone1=TimeZone.getTimeZone("IST");
				sdf1.setTimeZone(timeZone1);
				String stsformatter1 = body.get("actual_ts").toString();
				Long convertedLong1 = Long.parseLong(stsformatter1);
				String formattedDate1 = sdf1.format(convertedLong1);
				body.put("actual_ts", formattedDate1);
			}
			
			if(!body.containsKey("smsc_id"))
			{
				body.put("smsc_id", "");
			}
			if(!body.containsKey("bindata"))
			{
				body.put("bindata", "");
			}
			if(!body.containsKey("featurecd"))
			{
				body.put("featurecd", "");
			}
			if(!body.containsKey("msgclass"))
			{
				body.put("msgclass", "");
			}
			if(!body.containsKey("logicid"))
			{
				body.put("logicid", "0");
			}
			if(!body.containsKey("vp"))
			{
				body.put("vp", "0");
			}
			if(!body.containsKey("country"))
			{
				body.put("country", "");
			}
			if(!body.containsKey("longmsg_base_mid"))
			{
				body.put("longmsg_base_mid", "0");
			}
			if(!body.containsKey("tag1"))
			{
				body.put("tag1", "");
			}
			if(!body.containsKey("tag2"))
			{
				body.put("tag2", "");
			}
			if(!body.containsKey("tag3"))
			{
				body.put("tag3", "");
			}
			if(!body.containsKey("tag4"))
			{
				body.put("tag4", "");
			}
			if(!body.containsKey("tag5"))
			{
				body.put("tag5", "");
			}
			if(!body.containsKey("message_journey"))
			{
				body.put("message_journey", "");
			}
			if(!body.containsKey("filename"))
			{
				body.put("filename", "");
			}
			if(!body.containsKey("plat_latency_org_millis"))
			{
				body.put("plat_latency_org_millis", "0");
			}
			if(!body.containsKey("plat_latency_sla_millis"))
			{
				body.put("plat_latency_sla_millis", "0");
			}
			if(!body.containsKey("interim"))
			{
				body.put("interim", "0");
			}
			if(!body.containsKey("component_latency"))
			{
				body.put("component_latency", "");
			}
		}
		//Modified Body
		LinkedHashMap<String, Object> formatedmbody = new LinkedHashMap<String, Object>();
		{
			formatedmbody.put("sdate",body.get("sdate"));
			formatedmbody.put("esmeaddr",body.get("esmeaddr"));
			formatedmbody.put("send",body.get("send"));
			formatedmbody.put("cust_send",body.get("cust_send"));
			formatedmbody.put("dest",body.get("dest"));
			formatedmbody.put("msg",body.get("msg"));
			formatedmbody.put("stime",body.get("stime"));
			formatedmbody.put("alpha",body.get("alpha"));
			formatedmbody.put("udhi",body.get("udhi"));
			formatedmbody.put("dcs",body.get("dcs"));
			formatedmbody.put("mid",body.get("mid"));
			formatedmbody.put("routeid",body.get("routeid").toString().trim());
			formatedmbody.put("dlr_type",body.get("dlr_type"));
			formatedmbody.put("priority",body.get("priority"));
			formatedmbody.put("apptype",body.get("apptype"));
			formatedmbody.put("credits",body.get("credits"));
			formatedmbody.put("bypass_mcc_mnc",body.get("bypass_mcc_mnc"));
			formatedmbody.put("file_id",body.get("file_id"));
			formatedmbody.put("wid",body.get("wid"));
			formatedmbody.put("operator",body.get("operator"));
			formatedmbody.put("circle",body.get("circle"));
			formatedmbody.put("cust_mid",body.get("cust_mid"));
			formatedmbody.put("cust_ip",body.get("cust_ip"));
			formatedmbody.put("pcode",body.get("pcode"));
			formatedmbody.put("acode",body.get("acode"));
			formatedmbody.put("pid",body.get("pid"));
			formatedmbody.put("aid",body.get("aid"));
			formatedmbody.put("msg_source",body.get("msg_source"));
			formatedmbody.put("ie_nos",body.get("ie_nos"));
			formatedmbody.put("ie_sref",body.get("ie_sref"));
			formatedmbody.put("sche_date_time",body.get("sche_date_time"));
			formatedmbody.put("fps_id",body.get("fps_id"));
			formatedmbody.put("msg_type",body.get("msg_type"));
			formatedmbody.put("acc_type",body.get("acc_type"));
			formatedmbody.put("msgtag",body.get("msgtag"));
			formatedmbody.put("pattern_id",body.get("pattern_id"));
			formatedmbody.put("platform",body.get("platform"));
			formatedmbody.put("ctime",body.get("ctime"));
			formatedmbody.put("reason",body.get("reason"));
			formatedmbody.put("status_flag",body.get("status_flag"));
			formatedmbody.put("status_id",body.get("status_id"));
			formatedmbody.put("udh",body.get("udh"));
			formatedmbody.put("sts",body.get("sts"));
			formatedmbody.put("actual_ts",body.get("actual_ts"));
			formatedmbody.put("smsc_id",body.get("smsc_id"));
			formatedmbody.put("bindata",body.get("bindata"));
			formatedmbody.put("featurecd",body.get("featurecd"));
			formatedmbody.put("msgclass",body.get("msgclass"));
			formatedmbody.put("logicid",body.get("logicid"));
			formatedmbody.put("vp",body.get("vp"));
			formatedmbody.put("country",body.get("country"));
			formatedmbody.put("longmsg_base_mid",body.get("longmsg_base_mid"));
			formatedmbody.put("tag1",body.get("tag1"));
			formatedmbody.put("tag2",body.get("tag2"));
			formatedmbody.put("tag3",body.get("tag3"));
			formatedmbody.put("tag4",body.get("tag4"));
			formatedmbody.put("tag5",body.get("tag5"));
			formatedmbody.put("message_journey",body.get("message_journey"));
			formatedmbody.put("filename",body.get("filename"));
			formatedmbody.put("plat_latency_org_millis",body.get("plat_latency_org_millis"));
			formatedmbody.put("plat_latency_sla_millis",body.get("plat_latency_sla_millis"));
			formatedmbody.put("interim",body.get("interim"));
			formatedmbody.put("component_latency",body.get("component_latency"));
		}
		for(Entry<String, Object> entry:formatedmbody.entrySet())
	     {
			/*if (entry.getValue().toString().isEmpty()) {
				formatedmbody.put(entry.getKey(), "NULL");
			}*/
			if(entry.getKey()=="ctime")
			{
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				TimeZone timeZone=TimeZone.getTimeZone("IST");
				sdf.setTimeZone(timeZone);
				Date date= new Date();
				String formattedDate = sdf.format(date);
				formatedmbody.put("ctime", formattedDate);
			}
			formatedmbody.put(entry.getKey(), entry.getValue().toString().replaceAll("\\r|\\n", " "));
	     }

	    // System.out.println(formatedmbody);
	     
		/*String valueString = String.join(",",formatedmbody.values().toString().trim());
		System.out.println(valueString);*/
		//String csvFormated = StringUtils.arrayToCommaDelimitedString(formatedmbody.values().toArray());
		List<Event> events = new ArrayList<Event>(1);
		//System.out.println("Formated Body : '" + csvFormated.toString().replace("[","").replace("]","").trim()+ "'");
		
		//replacing , with ^ symbol
		StringBuilder sb1 = new StringBuilder();
		Iterator it = formatedmbody.values().iterator();
		sb1.append(it.next());
		//if (!it.hasNext()) sb2.append("");
		while (it.hasNext()) sb1.append("^").append(it.next());
		
		System.out.println(sb1);
		
		Event event = new SimpleEvent();
		event.setBody(sb1.toString().getBytes());
		//event.setBody(csvFormated.toString().replace("[","").replace("]","").trim().getBytes());
		events.add(event);
		//event.setBody(body.toString().getBytes());
		return events;
	}
	public static class Builder implements JMSMessageConverter.Builder {
		public void configure(Context context) {
		}

		public JMSMessageConverter build(Context arg0) {
			return new FilterSms_Submission();
		}
	}
}

