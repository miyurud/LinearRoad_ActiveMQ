package org.linear.activemq.output;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Set;

import javax.jms.BytesMessage;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.linear.activemq.events.PositionReportEvent;
import org.linear.activemq.input.InputEventInjectorClient;
import org.linear.activemq.layer.segstat.SegmentStatisticsListener;
import org.linear.activemq.util.Constants;
import org.linear.db.LinearRoadDBComm;

import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.example.servershell.jms.JMSContext;
import com.espertech.esper.example.servershell.jms.JMSContextFactory;

public class OutputEventListener implements MessageListener
{
    private static Log log = LogFactory.getLog(SegmentStatisticsListener.class);
    private EPRuntime engine;
    private int count;
    
	private JMSContext jmsCtx_output;
	private MessageProducer producer_output;

    public OutputEventListener(EPRuntime engine)
    {
        this.engine = engine;
       
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
    	//For the moment we just do nothing.
    
        BytesMessage bytesMsg = (BytesMessage) message;
        String body = getBody(bytesMsg);
        
        //System.out.println(body);
   
    }

    public int getCount()
    {
        return count;
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
