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

public class Test_SmsDeliveries implements JMSMessageConverter {

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

		LinkedHashMap<String, Object> body = (LinkedHashMap<String, Object>) defaultMap.clone();

		while (propertyNames.hasMoreElements()) {
			String name = (String) propertyNames.nextElement();
			if (requiredListSub.indexOf(name) >= 0) {
				body.put(name, message.getObjectProperty(name));
			}
		}

		{

			{
				String stime = (String) body.get("stime");
				if (!"".equals(stime)) {
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
				String sdtime = body.get("dtime").toString();
				if (!"".equals(sdtime)) {
					String dtimeFromat = (String) body.get("dtime_format");
					SimpleDateFormat sdf2 = new SimpleDateFormat(dtimeFromat);
					SimpleDateFormat printsdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
					sdf2.setLenient(false);
					String stsformatter1 = (String) body.get("dtime");
					Date formattedDate2=null;
					try {
						formattedDate2 = sdf2.parse(stsformatter1);
						body.put("dtime", printsdf .format(formattedDate2));
					} catch (ParseException e) {
						e.printStackTrace();
					}
				}

			}
			
			{
				String acdtime = body.get("actual_dtime").toString();
				if (!"".equals(acdtime)) {
					String dtimeFromat = (String) body.get("dtime_format");
					SimpleDateFormat sdf2 = new SimpleDateFormat(dtimeFromat);
					SimpleDateFormat printsdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
					sdf2.setLenient(false);
					String stsformatter1 = (String) body.get("actual_dtime");
					Date formattedDate2=null;
					try {
						formattedDate2 = sdf2.parse(stsformatter1);
						body.put("actual_dtime", printsdf .format(formattedDate2));
					} catch (ParseException e) {
						e.printStackTrace();
					}
				}

			}
			
			{
				String acdtime = (String)body.get("operator_rcvd_time");
				if (!"".equals(acdtime)) {
					String dtimeFromat = (String) body.get("dtime_format");
					SimpleDateFormat sdf2 = new SimpleDateFormat(dtimeFromat);
					SimpleDateFormat printsdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
					sdf2.setLenient(false);
					String stsformatter1 = (String) body.get("operator_rcvd_time");
					Date formattedDate2=null;
					try {
						formattedDate2 = sdf2.parse(stsformatter1);
						body.put("operator_rcvd_time", printsdf .format(formattedDate2));
					} catch (ParseException e) {
						String stsFormater3 =stsformatter1.concat("00");
						try {
							formattedDate2 = sdf2.parse(stsFormater3);
							body.put("operator_rcvd_time", printsdf .format(formattedDate2));
						} catch (ParseException e1) {
							body.put("operator_rcvd_time", stsformatter1);
						}
					}
					
				}

			}
			
			{
				String ststime = body.get("sts").toString();
				if (!"".equals(ststime)) {
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
				String acttime = body.get("actual_ts").toString();
				if (!"".equals(acttime)) {
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
			//body.remove("dtime_format");
		}
		body.remove("dtime_format");
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
		map.put("dest", "0");
		map.put("stime", "");
		map.put("mid", "0");
		map.put("routeid", "");
		map.put("dlr_type", "0");
		map.put("priority", "0");
		map.put("file_id", "");
		map.put("operator", "");
		map.put("circle", "");
		map.put("cust_mid", "");
		map.put("pcode", "");
		map.put("acode", "");
		map.put("pid", "0");
		map.put("aid", "0");
		map.put("msg_source", "");
		map.put("ie_nos", "0");
		map.put("ie_sref", "0");
		map.put("fps_id", "0");
		map.put("msg_type", "0");
		map.put("platform", "");
		map.put("rp", "0");
		map.put("dtime", "");
		map.put("status_id", "");
		map.put("reason", "");
		map.put("vmsc", "0");
		map.put("imsi", "0");
		map.put("term_operator", "");
		map.put("term_circle", "");
		map.put("ctime", "");
		map.put("status_flag", "");
		map.put("actual_dtime", "");
		map.put("sts", "");
		map.put("actual_ts", "");
		map.put("operator_rcvd_time", "");
		map.put("carrier_ack", "");
		map.put("country", "");
		map.put("carrier_full_dn", "");
		map.put("carrier_sys_id", "");
		map.put("err", "");
		map.put("org_err", "");
		map.put("delive_latency_org_sec", "0");
		map.put("delive_latency_sla_sec", "0");
		map.put("component_latency", "");
		return map;

	}

	public static class Builder implements JMSMessageConverter.Builder {
		public void configure(Context context) {
		}

		public JMSMessageConverter build(Context arg0) {
			return new Test_SmsDeliveries();
		}
	}
}
