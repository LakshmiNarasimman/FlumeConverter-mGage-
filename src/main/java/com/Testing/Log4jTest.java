package com.Testing;
import java.io.IOException;
import java.sql.SQLException;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;

public class Log4jTest  {
	static Logger log = Logger.getRootLogger();
	
	
    @SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, SQLException {
    	Logger logger = Logger.getLogger(Log4jTest.class);
    	int i=0;
         while (i <=10)
         {
             log.info("Hello this is an info message" + i);
             System.out.println("message sent no:"+i);
             i++;
         }
        
        Logger.getRootLogger().setLevel(Level.OFF);
//         try
//         {
//       LogManager.shutdown();
//         }catch(Exception e)
//         {
//        	 e.printStackTrace();
//         }
       
     }
    //log.getRootLogger().setLevel(Level.OFF);
    
   
   
    
}