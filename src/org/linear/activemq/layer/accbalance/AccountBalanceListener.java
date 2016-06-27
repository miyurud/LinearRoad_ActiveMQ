/**************************************************************************************
 * Copyright (C) 2008 EsperTech, Inc. All rights reserved.                            *
 * http://esper.codehaus.org                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package org.linear.activemq.layer.accbalance;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Properties;

import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.example.servershell.jms.JMSContext;
import com.espertech.esper.example.servershell.jms.JMSContextFactory;

import javax.jms.DeliveryMode;
import javax.jms.MessageListener;
import javax.jms.Message;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.linear.activemq.events.AccidentEvent;
import org.linear.activemq.events.AccountBalanceEvent;
import org.linear.activemq.events.LAVEvent;
import org.linear.activemq.events.NOVEvent;
import org.linear.activemq.events.PositionReportEvent;
import org.linear.activemq.input.InputEventInjectorClient;
import org.linear.activemq.util.Constants;
import org.linear.db.LinearRoadDBComm;


public class AccountBalanceListener implements MessageListener
{
    private static Log log = LogFactory.getLog(AccountBalanceListener.class);
    private EPRuntime engine;
    private int count;
    private LinkedList<AccountBalanceEvent> accEvtList;
    private HashMap<Integer, Integer> tollList;
    
	private JMSContext jmsCtx_output;
	private MessageProducer producer_output;
	
	private String host;
	private int port;

    public AccountBalanceListener(EPRuntime engine)
    {
        this.engine = engine;
        this.accEvtList = new LinkedList<AccountBalanceEvent>();
        this.tollList = new HashMap<Integer, Integer>();
        
        Properties properties = new Properties();
        InputStream propertiesIS = InputEventInjectorClient.class.getClassLoader().getResourceAsStream(org.linear.activemq.util.Constants.CONFIG_FILENAME);
        if (propertiesIS == null)
        {
            throw new RuntimeException("Properties file '" + org.linear.activemq.util.Constants.CONFIG_FILENAME + "' not found in classpath");
        }
        
        try {
			properties.load(propertiesIS);
	
			String destination_output = properties.getProperty(org.linear.activemq.util.Constants.JMS_INCOMING_DESTINATION_OUTPUT_LAYER);
			String jmsurl_output = properties.getProperty(org.linear.activemq.util.Constants.JMS_PROVIDER_URL_OUTPUT_LAYER);
			String connFactoryName = properties.getProperty(org.linear.activemq.util.Constants.JMS_CONNECTION_FACTORY_NAME);
	        String user = properties.getProperty(org.linear.activemq.util.Constants.JMS_USERNAME);
	        String password = properties.getProperty(org.linear.activemq.util.Constants.JMS_PASSWORD);
	        boolean isTopic = Boolean.parseBoolean(properties.getProperty(org.linear.activemq.util.Constants.JMS_IS_TOPIC));
	        String factory = properties.getProperty(org.linear.activemq.util.Constants.JMS_CONTEXT_FACTORY);
			jmsCtx_output = JMSContextFactory.createContext(factory, jmsurl_output, connFactoryName, user, password, destination_output, isTopic);
			jmsCtx_output.getConnection().start();
			producer_output = jmsCtx_output.getSession().createProducer(jmsCtx_output.getDestination());
			
			host = properties.getProperty(org.linear.activemq.util.Constants.LINEAR_DB_HOST);
			port = Integer.parseInt(properties.getProperty(org.linear.activemq.util.Constants.LINEAR_DB_PORT));
		} catch (IOException e) {
			e.printStackTrace();
		} catch (JMSException ec) {
			ec.printStackTrace();
		} catch (NamingException ex) {
			ex.printStackTrace();
		}
    }

    public void onMessage(Message message)
    {
        BytesMessage bytesMsg = (BytesMessage) message;
        String body = getBody(bytesMsg);
        String[] fields = body.split(" ");

       byte typeField = Byte.parseByte(fields[0]);
       
       switch(typeField){
       	   case Constants.ACC_BAL_EVENT_TYPE:
       		   AccountBalanceEvent et = new AccountBalanceEvent(Long.parseLong(fields[1]), Integer.parseInt(fields[2]), Integer.parseInt(fields[9]));
       		   process(et);
       		   //System.out.println("ACC BAL Evt : " + et.toString());
       		   break;
	       case Constants.TOLL_EVENT_TYPE:
	    	   int key = Integer.parseInt(fields[1]);
	    	   int value = Integer.parseInt(fields[2]);
	    	   
	    	   Integer kkey = (Integer)tollList.get(key);
	    	   
	    	   //System.out.println("key : " + key + " value : " + value);
	    	   
	    	   if(kkey != null){
	    		   tollList.put(key, (kkey + value)); //If the car id is already in the hashmap we need to add the tool to the existing toll.
	    	   }else{
	    		   tollList.put(key, value);
	    	   }
	    	   
	    	   break;
       }
        
        
    }

    public int getCount()
    {
        return count;
    }
    
    public void process(AccountBalanceEvent evt){
		int len = 0;
		Statement stmt;
		BytesMessage bytesMessage = null;
			    		
		if(evt != null){
				try{
				    bytesMessage = jmsCtx_output.getSession().createBytesMessage();
				    bytesMessage.writeBytes((Constants.ACC_BAL_EVENT_TYPE + " " + evt.vid + " " + tollList.get(evt.vid)).getBytes());
				    bytesMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
				    producer_output.send(bytesMessage);
				}catch(JMSException e){
					e.printStackTrace();
				}
		}
    }

    private String getBody(BytesMessage bytesMsg)
    {
        try
        {
            long length = bytesMsg.getBodyLength();
            byte[] buf = new byte[(int)length];
            bytesMsg.readBytes(buf);
            return new String(buf);
        }
        catch (JMSException e)
        {
            String text = "Error getting message body";
            log.error(text, e);
            throw new RuntimeException(text, e);
        }
    }
}
