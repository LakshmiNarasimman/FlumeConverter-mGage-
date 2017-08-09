package com.Testing;

import java.sql.Timestamp;


import java.text.ParseException;
import java.text.SimpleDateFormat;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TimeZone;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.json.JSONObject;
import org.springframework.util.StringUtils;

import com.google.gson.Gson;

public class TestHashMap {

	/**
	 * @param args
	 * @throws ParseException 
	 */
	public static void main(String[] args) throws ParseException {
		String body="{msg=Test message, country=India, acc_type=0, pcode=unitst2, msg_source=php, cust_mid=, mid=2161221204457035600, stime=2017-06-22 12:04:45:112, pid=80000200000000, acode=unitst2, dest=919884227203, operator=Vodafone, platform=CHN, bypass_mcc_mnc=1, wid=919945272877, pattern_id=98, dcs=0, actual_ts=1498113285291, alpha=1, cust_send=alerts, logicid=1, fps_id=1, sdate=2017-06-22, udhi=0, apptype=SMS, cust_ip=10.20.50.20, priority=3, tag5=alerts, routeid=RY, sts=1498113285291, interim=1, esmeaddr=80000200000000, msg_type=0, vp=40800, circle=Chennai, aid=80000200000000, bindata=, send=alerts, longmsg_base_mid=2161221204457035600}";
		long timeStamp=1372339860;
		java.util.Date dateTime=new java.util.Date((long)timeStamp*1000);
		System.out.println(dateTime);
		
		final long unixTime = 1372339860;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		TimeZone timeZone=TimeZone.getTimeZone("IST");
		sdf.setTimeZone(timeZone);
		//Date dt = sdf.parse(1707031907L);
		System.out.println("texdyhhhhhhhhhhhhh"+sdf.format(1707031907L));
		//String formattedDate=sdf.format(1498113285291L);
		//System.out.println("Formated Date:"+formattedDate);
			Map<String,Object> Sbody=new HashMap<String,Object>();
			Sbody.put("testing", "Test messgae");
			//Sbody.put("country","India");
			//Sbody.put("acc_type","0");
			Sbody.put("sts","1498113285291");
			
			System.out.println(Sbody);
			for(Map.Entry<String, Object> e:Sbody.entrySet())
			{
				if(e.getKey()=="sts")
				{
					System.out.println("sts value="+e.getValue());
					//System.out.println(formattedDtms);
					
				}
			}
			
			ArrayList<String> requiredList = new ArrayList<String>();
			{
				requiredList.add("msg");
				requiredList.add("country");
				requiredList.add("acc_type");
				requiredList.add("sts");
//				requiredList.add("sdate");
//				requiredList.add("timestamp");
//				//requiredList.add("msgtest");
//				requiredList.add("msgtime");
//				requiredList.add("msgloc");
//				requiredList.add("msgdel");
			}
			if(!Sbody.containsKey("msg"))
			{
				Sbody.put("msg", "0");
			}
			if(!Sbody.containsKey("country"))
			{
				Sbody.put("country", "India");
			}
			if(!Sbody.containsKey("acc_type"))
			{
				Sbody.put("acc_type", "0");
			}
			if(!Sbody.containsKey("sts"))
			{
				Sbody.put("sts", "0");
			}
			if(!Sbody.containsKey("testing"))
			{
				Sbody.put("testing", "0");
			}
			System.out.println("===========================>");
			if(Sbody.containsKey("sts"))
			{
				//String stsformatter = Sbody.values().toString();
				//String stsformatter=(String) Sbody.get("sts");
				String stsformatter=Sbody.get("sts").toString();
				Long convertedLong = Long.parseLong(stsformatter);
				String formattedDate=sdf.format(0);
				System.out.println(formattedDate);
				Sbody.put("sts", formattedDate);
				System.out.println(Sbody+"xzdfsdfsdf");
			}
			
			System.out.println("===========================>");
			/*if(!Sbody.containsKey("pps"))
			{
				Sbody.put("pps", "0");
				System.out.println(Sbody);
			}*/
			
			
//		for (String l : requiredList) {
//			if (!Sbody.containsKey(l)) {
//				Sbody.put(l, "0");
//				// System.out.println("Sbody containing all the fields in the required list");
//			}
//
//		}
			System.out.println(Sbody);
			
	LinkedHashMap<String, Object> testHashMap = new LinkedHashMap<String, Object>();
	testHashMap.put("msg",Sbody.get("msg"));
	testHashMap.put("country",Sbody.get("country"));
	testHashMap.put("acc_type",Sbody.get("acc_type"));
	testHashMap.put("sts",Sbody.get("sts"));
	testHashMap.put("testing",Sbody.get("testing"));
	System.out.println("===================>");
	System.out.println("Linked HashMap:"+testHashMap.values().toString().trim());
	System.out.println("===================>");
	
	System.out.println("csvcsvcsv===================>");
	String csv = StringUtils.arrayToCommaDelimitedString(testHashMap.values().toArray());
	System.out.println(csv);
	System.out.println("csvcsvcsv===================>");
	StringBuilder sb2 = new StringBuilder();
	Iterator it = testHashMap.values().iterator();
	sb2.append(it.next());
	//if (!it.hasNext()) sb2.append("");
	while (it.hasNext()) sb2.append("^").append(it.next());
   
   
	
		SimpleDateFormat sdf22 = new SimpleDateFormat("HH:mm:ss");
		String stsformatter1 = "1500443389902";
		String startTime="1500459184183";
		String endTime="1500459229978";
		long convertedLong1 = Long.parseLong(startTime);
		long convertedLong2 = Long.parseLong(endTime);
		String startDate = sdf22.format(new Date(convertedLong1));
		String endDate = sdf22.format(new Date(convertedLong2));
		System.out.println(startDate);
		System.out.println(endDate);
		
		String time1 = startDate;
		String time2 = endDate;

		SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss");
		Date date1 = format.parse(time1);
		Date date2 = format.parse(time2);
		long difference = date2.getTime() - date1.getTime(); 
		System.out.println(difference);
		Map<String, Object> testMap = new HashMap<String, Object>();
		Date d=new Date();
      	System.out.println( sdf22.format(d));
		
		
	}
	
	

}
