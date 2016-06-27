/**************************************************************************************
 * Copyright (C) 2008 EsperTech, Inc. All rights reserved.                            *
 * http://esper.codehaus.org                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package org.linear.activemq.layer.segstat;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;

import com.espertech.esper.client.EPRuntime;

import javax.jms.DeliveryMode;
import javax.jms.MessageListener;
import javax.jms.Message;
import javax.jms.BytesMessage;
import javax.jms.JMSException;

import com.espertech.esper.example.servershell.jms.JMSContext;
import com.espertech.esper.example.servershell.jms.JMSContextFactory;

import javax.jms.MessageProducer;
import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.linear.activemq.events.PositionReportEvent;
import org.linear.activemq.input.InputEventInjectorClient;
import org.linear.activemq.util.Constants;
import org.linear.db.LinearRoadDBComm;

/**
 * @author miyuru
 * 
 * --Richard Thesis
 *  Systems implementing Linear Road must maintain 10 weeks worth of statistics data for each segment on the Linear Road express way.
 *  This data is used for calculating tolls, computing travel time, and toll estimates. The data that must be maintained for each of the L X 200 segments
 *  includes the following
 *  
 *  NOV, LAV
 *  
 * --Uppsala
 * 
 * The segment statistics is responsible for maintianing the statistics for every segment 
 * on every expressway with different time span.
 * 
 * This class is used to calcluate two things
 * 
 * NOV - Number Of Vehicles present on a segment during the minute prior to current minute
 * LAV - Latest Average Velocity (LAV). This is calculated for a given segment with a time span of
 *       the five minutes prior to the current minute. 
 *
 */

public class SegmentStatisticsListener implements MessageListener
{
    private static Log log = LogFactory.getLog(SegmentStatisticsListener.class);
    private EPRuntime engine;
    private int count;
    
	private long currentSecond;
	private LinkedList<PositionReportEvent> posEvtList;
	private ArrayList<PositionReportEvent> evtListNOV;
	private ArrayList<PositionReportEvent> evtListLAV;
	private byte minuteCounter;
	private int lavWindow = 5; //This is LAV window in minutes
	
	private JMSContext jmsCtx_toll;
	private MessageProducer producer_toll;
	
	private String host;
	private int port;

    public SegmentStatisticsListener(EPRuntime engine)
    {
    	this.currentSecond = -1;
        this.engine = engine;
        posEvtList = new LinkedList<PositionReportEvent>();
        evtListNOV = new ArrayList<PositionReportEvent>();
        evtListLAV = new ArrayList<PositionReportEvent>();
        
        Properties properties = new Properties();
        InputStream propertiesIS = InputEventInjectorClient.class.getClassLoader().getResourceAsStream(org.linear.activemq.util.Constants.CONFIG_FILENAME);
        if (propertiesIS == null)
        {
            throw new RuntimeException("Properties file '" + org.linear.activemq.util.Constants.CONFIG_FILENAME + "' not found in classpath");
        }

        try {
			properties.load(propertiesIS);
	
			String destination_toll = properties.getProperty(org.linear.activemq.util.Constants.JMS_INCOMING_DESTINATION_TOLL_LAYER);
			String jmsurl_toll = properties.getProperty(org.linear.activemq.util.Constants.JMS_PROVIDER_URL_TOLL_LAYER);
			String connFactoryName = properties.getProperty(org.linear.activemq.util.Constants.JMS_CONNECTION_FACTORY_NAME);
	        String user = properties.getProperty(org.linear.activemq.util.Constants.JMS_USERNAME);
	        String password = properties.getProperty(org.linear.activemq.util.Constants.JMS_PASSWORD);
	        boolean isTopic = Boolean.parseBoolean(properties.getProperty(org.linear.activemq.util.Constants.JMS_IS_TOPIC));
	        String factory = properties.getProperty(org.linear.activemq.util.Constants.JMS_CONTEXT_FACTORY);
			jmsCtx_toll = JMSContextFactory.createContext(factory, jmsurl_toll, connFactoryName, user, password, destination_toll, isTopic);
			jmsCtx_toll.getConnection().start();
			producer_toll = jmsCtx_toll.getSession().createProducer(jmsCtx_toll.getDestination());
			
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
        	case 0:
        		//posEvtList.add(new PositionReportEvent(fields));
        		process(new PositionReportEvent(fields));
        		break;
        	case 2:
        		log.info("Account balance report");
        		break;
        	case 3:
        		log.info("Expenditure report");
        		break;
        	case 4:
        		log.info("Travel time report");
        		break;
        }
    }

    public int getCount()
    {
        return count;
    }
    
    public void process(PositionReportEvent evt){       	
		if(currentSecond == -1){
			currentSecond = evt.time;
		}else{
			if((evt.time - currentSecond) > 60){
				calculateNOV();
				
				evtListNOV.clear();
				
				currentSecond = evt.time;
				minuteCounter++;
				
				if(minuteCounter >= lavWindow){
					calculateLAV(currentSecond);
						
					//LAV list cannot be cleared because it need to keep the data for 5 minutes prior to any time t
					//evtListLAV.clear();
					
					minuteCounter = 0;
				}
			}
		}
		evtListNOV.add(evt);
		evtListLAV.add(evt);
    }

	private void calculateLAV(long currentTime) {
		float result = -1;
		float avgVelocity = -1;
		
		ArrayList<Byte> segList = new ArrayList<Byte>();
		Hashtable<Byte, ArrayList<Integer> > htResult = new Hashtable<Byte, ArrayList<Integer> >();  

		//First identify the number of segments
		Iterator<PositionReportEvent> itr = evtListLAV.iterator();
		byte curID = -1;
		while(itr.hasNext()){
			curID = itr.next().mile;
			
			if(!segList.contains(curID)){
				segList.add(curID);
			}
		}
		
		ArrayList<PositionReportEvent> tmpEvtListLAV = new ArrayList<PositionReportEvent>(); 
		float lav = -1;
		
		for(byte i =0; i < 2; i++){ //We need to do this calculation for both directions (west = 0; East = 1)
			Iterator<Byte> segItr = segList.iterator();
			int vid = -1;
			byte mile = -1;
			ArrayList<Integer> tempList = null;
			PositionReportEvent evt = null;
			long totalSegmentVelocity = 0;
			long totalSegmentVehicles = 0;
			BytesMessage bytesMessage = null;
			
			//We calculate LAV per segment
			while(segItr.hasNext()){
				mile = segItr.next();
				itr = evtListLAV.iterator();
				
				while(itr.hasNext()){
					evt = itr.next();
					
					if((Math.abs((evt.time - currentTime)) < 300)){
						if((evt.mile == mile) && (i == evt.dir)){ //Need only last 5 minutes data only
							vid = evt.vid;
							totalSegmentVelocity += evt.speed;
							totalSegmentVehicles++;
						}
						
						if(i == 1){//We need to add the events to the temp list only once. Because we iterate twice through the list
							tmpEvtListLAV.add(evt);//Because of the filtering in the previous if statement we do not accumulate events that are older than 5 minutes
						}
					}
				}
				
				lav = ((float)totalSegmentVelocity/totalSegmentVehicles);
				if(!Float.isNaN(lav)){				
					try{
					    bytesMessage = jmsCtx_toll.getSession().createBytesMessage();

					    bytesMessage.writeBytes((Constants.LAV_EVENT_TYPE + " " + mile + " " + lav + " " + i).getBytes());
					    bytesMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
					    producer_toll.send(bytesMessage);
					}catch(JMSException e){
						e.printStackTrace();
					}

					totalSegmentVelocity = 0;
					totalSegmentVehicles = 0;
				}
			}
		}
			
		//We assign the updated list here. We have discarded the events that are more than 5 minutes duration
		evtListLAV = tmpEvtListLAV;	
	}
	
	private void calculateNOV() {
		BytesMessage bytesMessage = null;
		ArrayList<Byte> segList = new ArrayList<Byte>();
		Hashtable<Byte, ArrayList<Integer> > htResult = new Hashtable<Byte, ArrayList<Integer> >();  
		
		//Get the list of segments first
		Iterator<PositionReportEvent> itr = evtListNOV.iterator();
		byte curID = -1;
		while(itr.hasNext()){
			curID = itr.next().mile;
			
			if(!segList.contains(curID)){
				segList.add(curID);
			}
		}
		
		Iterator<Byte> segItr = segList.iterator();
		int vid = -1;
		byte mile = -1;
		ArrayList<Integer> tempList = null;
		PositionReportEvent evt = null;

		//For each segment		
		while(segItr.hasNext()){
			mile = segItr.next();
			itr = evtListNOV.iterator();
			while(itr.hasNext()){
				evt = itr.next();
				
				if(evt.mile == mile){
					vid = evt.vid;
					
					if(!htResult.containsKey(mile)){
						tempList = new ArrayList<Integer>();
						tempList.add(vid);
						htResult.put(mile, tempList);
					}else{
						tempList = htResult.get(mile);
						tempList.add(vid);
						
						htResult.put(mile, tempList);
					}
				}
			}
		}
		
		Set<Byte> keys = htResult.keySet();
		
		Iterator<Byte> itrKeys = keys.iterator();
		int numVehicles = -1;
		mile = -1;
		
		while(itrKeys.hasNext()){
			mile = itrKeys.next();
			numVehicles = htResult.get(mile).size();
			
			try{
			    bytesMessage = jmsCtx_toll.getSession().createBytesMessage();

			    bytesMessage.writeBytes((Constants.NOV_EVENT_TYPE + " " + ((int)Math.floor(currentSecond/60)) + " " + mile + " " + numVehicles).getBytes());
			    bytesMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
			    producer_toll.send(bytesMessage);
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
