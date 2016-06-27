package org.linear.activemq.input;

/**
 * This class first loads the historical data. Once the historical data is loaded we
 * start injecting tuples.
 */

import com.espertech.esper.example.servershell.jms.JMSContext;
import com.espertech.esper.example.servershell.jms.JMSContextFactory;
import com.espertech.esper.example.servershell.jmx.EPServiceProviderJMXMBean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.linear.activemq.util.Constants;
import org.linear.activemq.util.Utilities;
import org.linear.db.LinearRoadDBComm;
import org.linear.activemq.layer.dailyexp.HistoryLoadingNotifierClient;

import javax.jms.BytesMessage;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.rmi.registry.LocateRegistry;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class InputEventInjectorClient
{
    private static Log log = LogFactory.getLog(InputEventInjectorClient.class);
    private JMSContext jmsCtx_account_balance;
    private JMSContext jmsCtx_daily_exp;
    private JMSContext jmsCtx_segstat;
    private JMSContext jmsCtx_toll;
    private JMSContext jmsCtx_accident;
    private JMSContext jmsCtx_traveltime;
    
    private MessageProducer producer_account_balance;
    private MessageProducer producer_daily_exp;
    private MessageProducer producer_segstat;
    private MessageProducer producer_toll;
    private MessageProducer producer_accident;
    private MessageProducer producer_traveltime;
       
    public static void main(String[] args) throws Exception
    {   	
        try
        {
            new InputEventInjectorClient();
        }
        catch (Throwable t)
        {
            log.error("Error starting server shell client : " + t.getMessage(), t);
            System.exit(-1);
        }
    }

    public InputEventInjectorClient() throws Exception
    {
    	log.info(Utilities.getTimeStamp() + ": Started running LR");
        log.info("Loading properties");
        Properties properties = new Properties();
        InputStream propertiesIS = InputEventInjectorClient.class.getClassLoader().getResourceAsStream(org.linear.activemq.util.Constants.CONFIG_FILENAME);
        if (propertiesIS == null)
        {
            throw new RuntimeException("Properties file '" + org.linear.activemq.util.Constants.CONFIG_FILENAME + "' not found in classpath");
        }
        properties.load(propertiesIS);

        // Start RMI registry
        log.info("Starting RMI registry");
        int port = Integer.parseInt(properties.getProperty(Constants.MGMT_RMI_PORT));
        LocateRegistry.createRegistry(port);
        
        // Attached via JMX to running server
        log.info("Attach to server via JMX");
        JMXServiceURL url = new JMXServiceURL(properties.getProperty(org.linear.activemq.util.Constants.MGMT_SERVICE_URL_ACCBALANCE_LAYER));
        JMXConnector jmxc = JMXConnectorFactory.connect(url, null);
        MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();
        ObjectName mBeanName = new ObjectName(org.linear.activemq.util.Constants.MGMT_MBEAN_NAME);
        EPServiceProviderJMXMBean proxy = (EPServiceProviderJMXMBean) MBeanServerInvocationHandler.newProxyInstance(
                     mbsc, mBeanName, EPServiceProviderJMXMBean.class, true);

        // Connect to JMS
        log.info("Connecting to JMS server");
        String factory = properties.getProperty(org.linear.activemq.util.Constants.JMS_CONTEXT_FACTORY);
        
        String jmsurl_accident = properties.getProperty(org.linear.activemq.util.Constants.JMS_PROVIDER_URL_ACCIDENTS_LAYER);
        String jmsurl_account_balance = properties.getProperty(org.linear.activemq.util.Constants.JMS_PROVIDER_URL_ACCBALANCE_LAYER);
        String jmsurl_daily_exp = properties.getProperty(org.linear.activemq.util.Constants.JMS_PROVIDER_URL_DAILYEXP_LAYER);
        String jmsurl_segstat = properties.getProperty(org.linear.activemq.util.Constants.JMS_PROVIDER_URL_SEGSTATS_LAYER);
        String jmsurl_toll = properties.getProperty(org.linear.activemq.util.Constants.JMS_PROVIDER_URL_TOLL_LAYER);
        String jmsurl_traveltime = properties.getProperty(org.linear.activemq.util.Constants.JMS_PROVIDER_URL_TOLL_LAYER);
        
        String connFactoryName = properties.getProperty(org.linear.activemq.util.Constants.JMS_CONNECTION_FACTORY_NAME);
        String user = properties.getProperty(org.linear.activemq.util.Constants.JMS_USERNAME);
        String password = properties.getProperty(org.linear.activemq.util.Constants.JMS_PASSWORD);
        String destination_accident = properties.getProperty(org.linear.activemq.util.Constants.JMS_INCOMING_DESTINATION_ACCIDENTS_LAYER);
        String destination_account_balance = properties.getProperty(org.linear.activemq.util.Constants.JMS_INCOMING_DESTINATION_ACCBALANCE_LAYER);
        String destination_daily_exp = properties.getProperty(org.linear.activemq.util.Constants.JMS_INCOMING_DESTINATION_DAILYEXP_LAYER);
        String destination_segstat = properties.getProperty(org.linear.activemq.util.Constants.JMS_INCOMING_DESTINATION_SEGSTATS_LAYER);
        String destination_toll = properties.getProperty(org.linear.activemq.util.Constants.JMS_INCOMING_DESTINATION_TOLL_LAYER);
        String destination_traveltime = properties.getProperty(org.linear.activemq.util.Constants.JMS_INCOMING_DESTINATION_TRAVEL_TIME_LAYER);
        String historyFile = properties.getProperty(org.linear.activemq.util.Constants.LINEAR_HISTORY);
        String carDataFile = properties.getProperty(org.linear.activemq.util.Constants.LINEAR_CAR_DATA_POINTS);
        boolean isTopic = Boolean.parseBoolean(properties.getProperty(org.linear.activemq.util.Constants.JMS_IS_TOPIC));
        
        jmsCtx_accident = JMSContextFactory.createContext(factory, jmsurl_accident, connFactoryName, user, password, destination_accident, isTopic);
        jmsCtx_account_balance = JMSContextFactory.createContext(factory, jmsurl_account_balance, connFactoryName, user, password, destination_account_balance, isTopic);
        jmsCtx_daily_exp = JMSContextFactory.createContext(factory, jmsurl_daily_exp, connFactoryName, user, password, destination_daily_exp, isTopic);
        jmsCtx_segstat = JMSContextFactory.createContext(factory, jmsurl_segstat, connFactoryName, user, password, destination_segstat, isTopic);
        jmsCtx_toll = JMSContextFactory.createContext(factory, jmsurl_toll, connFactoryName, user, password, destination_toll, isTopic);
        jmsCtx_traveltime = JMSContextFactory.createContext(factory, jmsurl_traveltime, connFactoryName, user, password, destination_traveltime, isTopic);
           
        // Get producer
        jmsCtx_accident.getConnection().start();
        producer_accident = jmsCtx_accident.getSession().createProducer(jmsCtx_accident.getDestination());
        
        jmsCtx_daily_exp.getConnection().start();
        producer_daily_exp = jmsCtx_daily_exp.getSession().createProducer(jmsCtx_daily_exp.getDestination());
        
        jmsCtx_segstat.getConnection().start();
        producer_segstat = jmsCtx_segstat.getSession().createProducer(jmsCtx_segstat.getDestination());
        
        jmsCtx_toll.getConnection().start();
        producer_toll = jmsCtx_toll.getSession().createProducer(jmsCtx_toll.getDestination());
        
        jmsCtx_traveltime.getConnection().start();
        producer_traveltime = jmsCtx_traveltime.getSession().createProducer(jmsCtx_traveltime.getDestination());
        
        jmsCtx_account_balance.getConnection().start();
        producer_account_balance = jmsCtx_account_balance.getSession().createProducer(jmsCtx_account_balance.getDestination());
       
        //Next we wait until the history information loading gets completed
        int c = 0;
        while(!HistoryLoadingNotifierClient.isHistoryLoaded()){
        	Thread.sleep(1000);//just wait one second and check again
        	System.out.println(c + " : isHistoryLoading...");
        	c++;
        }
        
        System.out.println("Done loading the history....");
                
		try{
			BufferedReader in = new BufferedReader(new FileReader(carDataFile));
			
			String line;
			
			/*
			 
			 The input file has the following format
			 
			    Cardata points input tuple format
				0 - type of the packet (0=Position Report, 1=Account Balance, 2=Expenditure, 3=Travel Time [According to Richard's thesis. See Below...])
				1 - Seconds since start of simulation (i.e., time is measured in seconds)
				2 - Car ID (0..999,999)
				3 - An integer number of miles per hour (i.e., speed) (0..100)
				4 - Expressway number (0..9)
				5 - The lane number (There are 8 lanes altogether)(0=Ramp, 1=Left, 2=Middle, 3=Right)(4=Left, 5=Middle, 6=Right, 7=Ramp)
				6 - Direction (west = 0; East = 1)
				7 - Mile (This corresponds to the seg field in the original table) (0..99)
				8 - Distance from the last mile post (0..1759) (Arasu et al. Pos(0...527999) identifies the horizontal position of the vehicle as a meaure of number of feet
				    from the western most position on the expressway)
				9 - Query ID (0..999,999)
				10 - Starting milepost (m_init) (0..99)
				11 - Ending milepost (m_end) (0..99)
				12 - day of the week (dow) (0=Sunday, 1=Monday,...,6=Saturday) (in Arasu et al. 1...7. Probably this is wrong because 0 is available as DOW)
				13 - time of the day (tod) (0:00..23:59) (in Arasu et al. 0...1440)
				14 - day
				
				Notes
				* While Richard thesis explain the input tuple formats, it is probably not correct.
				* The correct type number would be 
				* 0=Position Report (Type=0, Time, VID, Spd, Xway, Lane, Dir, Seg, Pos)
				* 2=Account Balance Queries (because all and only all the 4 fields available on tuples with type 2) (Type=2, Time, VID, QID)
				* 3=Daily expenditure (Type=3, Time, VID, XWay, QID, Day). Here if day=1 it is yesterday. d=69 is 10 weeks ago. 
				* 4=Travel Time requests (Types=4, Time, VID, XWay, QID, Sinit, Send, DOW, TOD)
				
				history data input tuple format
				0 - Car ID
				1 - day
				2 - x - Expressway number
				3 - daily expenditure
				
				E.g.
				#(1 3 0 31)
				#(1 4 0 61)
				#(1 5 0 34)
				#(1 6 0 30)
				#(1 7 0 63)
				#(1 8 0 55)
				
				//Note that since the historical data table's key is made out of several fields it was decided to use a relation table 
				//instead of using a hashtable
			 
			 */
			
			
			while((line = in.readLine()) != null){			
				emit(line.substring(2, line.length() - 1));
			}
		}catch(IOException ec){
			ec.printStackTrace();
		}
		
		System.out.println(Utilities.getTimeStamp() + ": Done running LR");       

        log.info("Exiting");
        System.exit(-1);
    }
    
	public void emit(String tuple){
		String[] fields = tuple.split(" ");
		byte typeField = Byte.parseByte(fields[0]); 
		BytesMessage bytesMessage = null;
		
		switch(typeField){
		case 0:
			//This is a position report (Type=0, Time, VID, Spd, Xway, Lane, Dir, Seg, Pos)		
	          try {
				bytesMessage = jmsCtx_accident.getSession().createBytesMessage();
		        bytesMessage.writeBytes(tuple.getBytes());
		        bytesMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
		        producer_accident.send(bytesMessage);
		          
		        bytesMessage = jmsCtx_segstat.getSession().createBytesMessage();
		        bytesMessage.writeBytes(tuple.getBytes());
		        bytesMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
		        producer_segstat.send(bytesMessage);
				
		        //For toll event listener we have to wrap the message in the internal protocol (See Constants class)
		        bytesMessage = jmsCtx_toll.getSession().createBytesMessage();
		        bytesMessage.writeBytes((Constants.POS_EVENT_TYPE + " " + tuple).getBytes());
		        bytesMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
		        producer_toll.send(bytesMessage);
			} catch (JMSException e) {
				e.printStackTrace();
			}
	          
			break;
		case 2:
			//This is an Account Balance report (Type=2, Time, VID, QID)

			try{
	          bytesMessage = jmsCtx_account_balance.getSession().createBytesMessage();
	          bytesMessage.writeBytes(tuple.getBytes());
	          bytesMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
	          producer_account_balance.send(bytesMessage);
			} catch (JMSException e) {
				e.printStackTrace();
			}
			break;
		case 3 : 
			//This is an Expenditure report (Type=3, Time, VID, XWay, QID, Day)

			try{	
		        bytesMessage = jmsCtx_daily_exp.getSession().createBytesMessage();
		        bytesMessage.writeBytes(tuple.getBytes());
		        bytesMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
		        producer_daily_exp.send(bytesMessage);
			} catch (JMSException e) {
				e.printStackTrace();
			}
			break;
		case 4:
			//This is a travel time report (Types=4, Time, VID, XWay, QID, Sinit, Send, DOW, TOD)
			try{	
		          bytesMessage = jmsCtx_traveltime.getSession().createBytesMessage();
		          bytesMessage.writeBytes(tuple.getBytes());
		          bytesMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
		          producer_traveltime.send(bytesMessage);
			} catch (JMSException e) {
				e.printStackTrace();
			}
			break;
		case 5:
			System.out.println("Travel time query was issued : " + tuple);
			break;
		}		
	}
}