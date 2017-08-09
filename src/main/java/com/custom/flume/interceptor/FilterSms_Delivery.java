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


public class FilterSms_Delivery implements JMSMessageConverter {
	//Creating the required list
	private ArrayList<String> requiredListSub = new ArrayList<String>();
	{		
		requiredListSub.add("sdate");
		requiredListSub.add("esmeaddr");
		requiredListSub.add("send");
		requiredListSub.add("dest");
		requiredListSub.add("stime");
		requiredListSub.add("mid");
		requiredListSub.add("routeid");
		requiredListSub.add("dlr_type");
		requiredListSub.add("priority");
		requiredListSub.add("file_id");
		requiredListSub.add("operator");
		requiredListSub.add("circle");
		requiredListSub.add("cust_mid");
		requiredListSub.add("pcode");
		requiredListSub.add("acode");
		requiredListSub.add("pid");
		requiredListSub.add("aid");
		requiredListSub.add("msg_source");
		requiredListSub.add("ie_nos");
		requiredListSub.add("ie_sref");
		requiredListSub.add("fps_id");
		requiredListSub.add("msg_type");
		requiredListSub.add("platform");
		requiredListSub.add("rp");
		requiredListSub.add("dtime");
		requiredListSub.add("status_id");
		requiredListSub.add("reason");
		requiredListSub.add("vmsc");
		requiredListSub.add("imsi");
		requiredListSub.add("term_operator");
		requiredListSub.add("term_circle");
		requiredListSub.add("ctime");
		requiredListSub.add("status_flag");
		requiredListSub.add("actual_dtime");
		requiredListSub.add("sts");
		requiredListSub.add("actual_ts");
		requiredListSub.add("operator_rcvd_time");
		requiredListSub.add("carrier_ack");
		requiredListSub.add("country");
		requiredListSub.add("carrier_full_dn");
		requiredListSub.add("carrier_sys_id");
		requiredListSub.add("err");
		requiredListSub.add("org_err");
		requiredListSub.add("delive_latency_org_sec");
		requiredListSub.add("delive_latency_sla_sec");
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
			if(!body.containsKey("dest"))
			{
				body.put("dest", "0");
			}
			if(!body.containsKey("stime"))
			{
				body.put("stime", "");
			}
			else
			{
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				TimeZone timeZone1=TimeZone.getTimeZone("IST");
				sdf.setTimeZone(timeZone1);
				String stsformatter1 = body.get("stime").toString();
				try {
					Date formattedDate1 = sdf.parse(stsformatter1);
					String strDate1 = sdf.format(formattedDate1);
					body.put("stime", strDate1);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
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
			if(!body.containsKey("file_id"))
			{
				body.put("file_id", "");
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
			if(!body.containsKey("fps_id"))
			{
				body.put("fps_id", "0");
			}
			if(!body.containsKey("msg_type"))
			{
				body.put("msg_type", "0");
			}
			if(!body.containsKey("platform"))
			{
				body.put("platform", "");
			}
			if(!body.containsKey("rp"))
			{
				body.put("rp", "0");
			}
			if(!body.containsKey("dtime"))
			{
				body.put("dtime", "");
			}
			else
			{
				String dtimeFromat = (String) body.get("dtime_format");
				SimpleDateFormat sdf2 = new SimpleDateFormat(dtimeFromat);
				SimpleDateFormat printsdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
				sdf2.setLenient(false);
				TimeZone timeZone1=TimeZone.getTimeZone("IST");
				sdf2.setTimeZone(timeZone1);
				String stsformatter2 = (String) body.get("dtime");
//				Long convertedLong1 = Long.parseLong(stsformatter2);
				Date formattedDate2=null;
				try {
					formattedDate2 = sdf2.parse(stsformatter2);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				body.put("dtime", printsdf .format(formattedDate2));
			}
			if(!body.containsKey("status_id"))
			{
				body.put("status_id", "");
			}
			if(!body.containsKey("reason"))
			{
				body.put("reason", "");
			}
			if(!body.containsKey("vmsc"))
			{
				body.put("vmsc", "0");
			}
			if(!body.containsKey("imsi"))
			{
				body.put("imsi", "0");
			}
			if(!body.containsKey("term_operator"))
			{
				body.put("term_operator", "");
			}
			if(!body.containsKey("term_circle"))
			{
				body.put("term_circle", "");
			}
			if(!body.containsKey("ctime"))
			{
				body.put("ctime", "");
			}
			if(!body.containsKey("status_flag"))
			{
				body.put("status_flag", "");
			}
			if(!body.containsKey("actual_dtime"))
			{
				body.put("actual_dtime", "");
			}
			else
			{
				
				String dtimeFromat = (String) body.get("dtime_format");
				SimpleDateFormat sdf2 = new SimpleDateFormat(dtimeFromat);
				SimpleDateFormat printsdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
				sdf2.setLenient(false);
				TimeZone timeZone1=TimeZone.getTimeZone("IST");
				sdf2.setTimeZone(timeZone1);
				String stsformatter2 = (String) body.get("actual_dtime");
				//Long convertedLong1 = Long.parseLong(stsformatter2);
				Date formattedDate2=null;
				try {
					formattedDate2 = sdf2.parse(stsformatter2);
				} catch (ParseException e) {
					e.printStackTrace();
				}
				body.put("actual_dtime", printsdf .format(formattedDate2));
			}
			
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
			
			//System.out.println("Operator Received Time :'"+body.get("operator_rcvd_time")+"'");
			if(!body.containsKey("operator_rcvd_time"))
			{
				body.put("operator_rcvd_time", "");
			}
			else
			{
				/*SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				TimeZone timeZone=TimeZone.getTimeZone("IST");
				sdf.setTimeZone(timeZone);
				Date date= new Date();
				String formattedDate = sdf.format(date);
				body.put("operator_rcvd_time", formattedDate);*/
				String dtimeFromat = (String) body.get("dtime_format");
				SimpleDateFormat sdf2 = new SimpleDateFormat(dtimeFromat);
				SimpleDateFormat printsdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
				sdf2.setLenient(false);
				TimeZone timeZone1=TimeZone.getTimeZone("IST");
				sdf2.setTimeZone(timeZone1);
				String stsformatter2 = (String) body.get("operator_rcvd_time");
				//Long convertedLong1 = Long.parseLong(stsformatter2);
				Date formattedDate2=null;
				try {
					formattedDate2 = sdf2.parse(stsformatter2);
				} catch (ParseException e) {
					String stsFormater3 =stsformatter2.concat("00");
					try {
						formattedDate2 = sdf2.parse(stsFormater3);
					} catch (ParseException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					//e.printStackTrace();
				}
				body.put("operator_rcvd_time", printsdf .format(formattedDate2));
			}
			
			if(!body.containsKey("carrier_ack"))
			{
				body.put("carrier_ack", "");
			}
			if(!body.containsKey("country"))
			{
				body.put("country", "");
			}
			if(!body.containsKey("carrier_full_dn"))
			{
				body.put("carrier_full_dn", "");
			}
			if(!body.containsKey("carrier_sys_id"))
			{
				body.put("carrier_sys_id", "");
			}
			if(!body.containsKey("err"))
			{
				body.put("err", "");
			}
			if(!body.containsKey("org_err"))
			{
				body.put("org_err", "");
			}
			if(!body.containsKey("delive_latency_org_sec"))
			{
				body.put("delive_latency_org_sec", "0");
			}
			if(!body.containsKey("delive_latency_sla_sec"))
			{
				body.put("delive_latency_sla_sec", "0");
			}
			if(!body.containsKey("component_latency"))
			{
				body.put("component_latency", "");
			}
		}
		
		LinkedHashMap<String, Object> testHashMap = new LinkedHashMap<String, Object>();
		testHashMap.put("sdate",body.get("sdate"));
		testHashMap.put("esmeaddr",body.get("esmeaddr"));
		testHashMap.put("send",body.get("send"));
		testHashMap.put("dest",body.get("dest"));
		testHashMap.put("stime",body.get("stime"));
		testHashMap.put("mid",body.get("mid"));
		testHashMap.put("routeid",body.get("routeid").toString().trim());
		testHashMap.put("dlr_type",body.get("dlr_type"));
		testHashMap.put("priority",body.get("priority"));
		testHashMap.put("file_id",body.get("file_id"));
		testHashMap.put("operator",body.get("operator"));
		testHashMap.put("circle",body.get("circle"));
		testHashMap.put("cust_mid",body.get("cust_mid"));
		testHashMap.put("pcode",body.get("pcode"));
		testHashMap.put("acode",body.get("acode"));
		testHashMap.put("pid",body.get("pid"));
		testHashMap.put("aid",body.get("aid"));
		testHashMap.put("msg_source",body.get("msg_source"));
		testHashMap.put("ie_nos",body.get("ie_nos"));
		testHashMap.put("ie_sref",body.get("ie_sref"));
		testHashMap.put("fps_id",body.get("fps_id"));
		testHashMap.put("msg_type",body.get("msg_type"));
		testHashMap.put("platform",body.get("platform"));
		testHashMap.put("rp",body.get("rp"));
		testHashMap.put("dtime",body.get("dtime"));
		testHashMap.put("status_id",body.get("status_id"));
		testHashMap.put("reason",body.get("reason"));
		testHashMap.put("vmsc",body.get("vmsc"));
		testHashMap.put("imsi",body.get("imsi"));
		testHashMap.put("term_operator",body.get("term_operator"));
		testHashMap.put("term_circle",body.get("term_circle"));
		testHashMap.put("ctime",body.get("ctime"));
		testHashMap.put("status_flag",body.get("status_flag"));
		testHashMap.put("actual_dtime",body.get("actual_dtime"));
		testHashMap.put("sts",body.get("sts"));
		testHashMap.put("actual_ts",body.get("actual_ts"));
		testHashMap.put("operator_rcvd_time",body.get("operator_rcvd_time"));
		testHashMap.put("carrier_ack",body.get("carrier_ack"));
		testHashMap.put("country",body.get("country"));
		testHashMap.put("carrier_full_dn",body.get("carrier_full_dn"));
		testHashMap.put("carrier_sys_id",body.get("carrier_sys_id"));
		testHashMap.put("err",body.get("err"));
		testHashMap.put("org_err",body.get("org_err"));
		testHashMap.put("delive_latency_org_sec",body.get("delive_latency_org_sec"));
		testHashMap.put("delive_latency_sla_sec",body.get("delive_latency_sla_sec"));
		testHashMap.put("component_latency",body.get("component_latency"));
		
		for(Entry<String, Object> entry:testHashMap.entrySet())
	     {
			/*if (entry.getValue().toString().isEmpty()) {
				testHashMap.put(entry.getKey(), "NULL");
			}*/
			if(entry.getKey()=="ctime")
			{
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				TimeZone timeZone=TimeZone.getTimeZone("IST");
				sdf.setTimeZone(timeZone);
				Date date= new Date();
				String formattedDate = sdf.format(date);
				testHashMap.put("ctime", formattedDate);
			}
			testHashMap.put(entry.getKey(), entry.getValue().toString().replaceAll("\\r|\\n", " "));
	     }
		List<Event> events = new ArrayList<Event>(1);
		Event event = new SimpleEvent();
		StringBuilder sb1 = new StringBuilder();
		Iterator it = testHashMap.values().iterator();
		sb1.append(it.next());
		while (it.hasNext()) sb1.append("^").append(it.next());
		System.out.println(sb1);
		event.setBody(sb1.toString().getBytes());
		events.add(event);
		return events;
	}
	public static class Builder implements JMSMessageConverter.Builder {
		public void configure(Context context) {
		}

		public JMSMessageConverter build(Context arg0) {
			return new FilterSms_Delivery();
		}
	}
}

