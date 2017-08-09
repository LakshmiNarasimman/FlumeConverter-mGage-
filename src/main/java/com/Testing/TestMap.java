package com.Testing;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.Map.Entry;

import javax.jms.JMSException;

public class TestMap {

	/**
	 * @param args
	 * @throws ParseException 
	 */
	static LinkedHashMap<String, Object> defaultMap;
	static {
		defaultMap=createHashMap();
	}
	static javax.jms.Message setMessage() throws JMSException
	{
		javax.jms.Message message = null;
		
		message.setObjectProperty("Name", "Lakshmi");
		
		return message;
	}
	
	public static void main(String[] args) throws ParseException, JMSException {
		javax.jms.Message msgSample=setMessage();
		Enumeration propertyNames = msgSample.getPropertyNames();
		String name = (String) propertyNames.nextElement();
		System.out.println(name);
		LinkedHashMap<String, Object> msg=(LinkedHashMap<String, Object>) defaultMap.clone();
		if(msg.get("apple").equals(""))
		{
			msg.put("apple", 0);
		}
        System.out.println(msg);
        LinkedHashMap<String, Object> body = new LinkedHashMap<String, Object>();
        ArrayList<String> requiredListSub = new ArrayList<String>();
        {
        	requiredListSub.add("apple");
        	requiredListSub.add("orange");
        	requiredListSub.add("banana");
        
        }
        if (requiredListSub.indexOf(msg) >= 0)
        {
        	
        }
        
		// TODO Auto-generated method stub
//		 Map<String, Object> fruits = new HashMap<String, Object>();
//	        fruits.put("apple", 1);
//	        fruits.put("orange", 2);
//	        fruits.put("banana", 3);
//	        fruits.put("watermelon","" );
//	        fruits.put("test", "I am at office \n I will \r leave at 7.30");
//	        System.out.println(fruits);
//	     /*for(Entry<String, Object> entry:fruits.entrySet())
//	     {
//	    	 System.out.println(entry.getKey().toString());
//	    	 if(fruits.containsKey(entry.getKey()))
//	    	 {
//	    		 System.out.println(entry.getValue().toString());
//	    		 if(entry.getValue().equals(""))
//	    		 {
//	    			 //entry.getValue().toString().replace("", "null");
//	    			 fruits.put(entry.getKey(), "null");
//	    			 
//	    		 }
//	    	 }
//	     }
//	     System.out.println(fruits);*/
//	     
//	     for(Entry<String, Object> entry:fruits.entrySet())
//	     {
//			/*if (entry.getValue() == "") {
//				fruits.put(entry.getKey(), null);
//			}*/
//			if (entry.getValue().toString().isEmpty()) {
//				fruits.put(entry.getKey(), "NULL");
//			}
//			if(entry.getKey().toString().equals("test"))
//			{
//				System.out.println(entry.getValue().toString().replaceAll("\\r|\\n", " "));
//				fruits.put(entry.getKey(), entry.getValue().toString().replaceAll("\\r|\\n", " "));
//			}
//					
//	     }
//
//	     System.out.println(fruits);
//	     
//	     SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//			TimeZone timeZone=TimeZone.getTimeZone("IST");
//			sdf.setTimeZone(timeZone);
//			sdf.setLenient(false);
//			String stsformatter = "1707031948";
//			Long convertedLong = Long.parseLong(stsformatter);
//			String formattedDate = sdf.format(convertedLong);
//			System.out.println(formattedDate);
//			
//		long val = 1707031948;
//		Date date = new Date(val);
//		SimpleDateFormat df2 = new SimpleDateFormat("yyMMddHHmmss");
//		df2.setLenient(false);
//		String dateText = df2.format(date.getTime());
//		Date dt = df2.parse(dateText);
//		String strDate1 = df2.format(dt);
//		System.out.println(dateText);
//		
//		long unixSeconds = 1707041538;
//		Date date1 = new Date(unixSeconds*10L);
//		SimpleDateFormat sdff = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); // the format of your date
//		sdf.setTimeZone(TimeZone.getTimeZone("IST")); // give a timezone reference for formating (see comment at the bottom
//		String formattedDate11 = sdff.format(date1);
//		System.out.println(formattedDate11);
//		
//		
//		
//		
//		SimpleDateFormat sdf2 = new SimpleDateFormat("yyMMddHHmm");
//		SimpleDateFormat printsdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
//		sdf2.setLenient(false);
//		TimeZone timeZone1=TimeZone.getTimeZone("IST");
//		sdf2.setTimeZone(timeZone1);
//		//Timestamp ts =new Timestamp(1707041538);
//		Date date2 = new Date();
//		date2.setTime(170704153800L);
//		String formattedDate2 = sdf2.format(date2);
//		System.out.println(formattedDate2);
//		
//		Date formattedDate3=null;
//		try {
//			formattedDate3 = sdf2.parse("1707041538");
//		} catch (ParseException e) {
//			e.printStackTrace();
//		}
//		//System.out.println(printsdf.format(formattedDate3));
//		
//		String trDate="1707041538";
//		/*if(trDate.length()==10)
//		{
//			Date tradeDate = new SimpleDateFormat("yyMMddHHmm").parse(trDate);
//			String krwtrDate = new SimpleDateFormat("yyyy-MM-dd ").format(tradeDate);
//			System.out.println(krwtrDate);
//			
//		}
//		else if(trDate.length()==12)
//		{
//			Date tradeDate = new SimpleDateFormat("yyMMddHHmmss").parse(trDate);
//			String krwtrDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(tradeDate);
//			System.out.println(krwtrDate);
//		}
//		else
//		{
//			Date dtt = new Date();
//			String krwtrDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dtt);
//			System.out.println(krwtrDate);
//		}*/
//		if(trDate.length()==12)
//		{
//			Date tradeDate = new SimpleDateFormat("yyMMddHHmmss").parse(trDate);
//			String krwtrDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(tradeDate);
//			System.out.println(krwtrDate);
//		}
//		else
//		{
//			String teststr =trDate.concat("00");
//			System.out.println(teststr);
//			Date tradeDate = new SimpleDateFormat("yyMMddHHmmss").parse(teststr);
//			String krwtrDate = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(tradeDate);
//			System.out.println(krwtrDate);
//		}
//		
}

	private static LinkedHashMap<String, Object> createHashMap() {
		// TODO Auto-generated method stub
		LinkedHashMap<String, Object> map = new LinkedHashMap<String, Object>();
		map.put("apple", "");
		map.put("orange", 2);
		map.put("banana", 3);
		map.put("watermelon","" );
		
		if(map.get("apple").equals(""))
		{
			map.put("apple", 0);
		}
        System.out.println(map);
		return map;
	}

}
