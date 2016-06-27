/**************************************************************************************
 * Copyright (C) 2008 EsperTech, Inc. All rights reserved.                            *
 * http://esper.codehaus.org                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package org.linear.activemq.layer.accident;

import java.io.IOException;
import java.io.InputStream;
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
import org.linear.activemq.events.PositionReportEvent;
import org.linear.activemq.input.InputEventInjectorClient;
import org.linear.activemq.util.Constants;

public class AccidentListener implements MessageListener
{
    private static Log log = LogFactory.getLog(AccidentListener.class);
    private EPRuntime engine;
    private int count;
    
	private JMSContext jmsCtx_toll;
	private MessageProducer producer_toll;
    
    private LinkedList<Car> posRptQueue = new LinkedList<Car>();

    public AccidentListener(EPRuntime engine)
    {
        this.engine = engine;
        
        
        Properties properties = new Properties();
        InputStream propertiesIS = AccidentListener.class.getClassLoader().getResourceAsStream(org.linear.activemq.util.Constants.CONFIG_FILENAME);
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
        String fields[] = body.split(" ");
        
        PositionReportEvent posEvt = new PositionReportEvent(fields);
		Car c = new Car(posEvt.time, posEvt.vid, posEvt.speed,
				posEvt.xWay, posEvt.lane, posEvt.dir, posEvt.mile);

		detect(c);
        
    }

    public void detect(Car c) {
		BytesMessage bytesMessage = null;
		
		if (c.speed > 0) {
			remove_from_smashed_cars(c);
			remove_from_stopped_cars(c);
		} else if (c.speed == 0) {
			if (is_smashed_car(c) == false) {
				if (is_stopped_car(c) == true) {
					renew_stopped_car(c);
				} else {
					stopped_cars.add(c);
				}
				
				int flag = 0;
				for (int i = 0; i < stopped_cars.size() -1; i++) {
					Car t_car = (Car)stopped_cars.get(i);
					if ((t_car.carid != c.carid)&&(!t_car.notified)&&((c.time - t_car.time) <= 120) && (t_car.posReportID >= 3) &&
							((c.xway0 == t_car.xway0 && c.mile0 == t_car.mile0 && c.lane0 == t_car.lane0 && c.offset0 == t_car.offset0 && c.dir0 == t_car.dir0) &&
							(c.xway1 == t_car.xway1 && c.mile1 == t_car.mile1 && c.lane1 == t_car.lane1 && c.offset1 == t_car.offset1 && c.dir1 == t_car.dir1) &&
							(c.xway2 == t_car.xway2 && c.mile2 == t_car.mile2 && c.lane2 == t_car.lane2 && c.offset2 == t_car.offset2 && c.dir2 == t_car.dir2) &&
							(c.xway3 == t_car.xway3 && c.mile3 == t_car.mile3 && c.lane3 == t_car.lane3 && c.offset3 == t_car.offset3 && c.dir3 == t_car.dir3))) {
						
						if (flag == 0) {
							AccidentEvent a_event = new AccidentEvent();
							a_event.vid1 = c.carid;
							a_event.vid2 = t_car.carid;
							a_event.xway = c.xway0;
							a_event.mile = c.mile0;
							a_event.dir = c.dir0;
							a_event.time = t_car.time;
							
							try{
							    bytesMessage = jmsCtx_toll.getSession().createBytesMessage();

							    bytesMessage.writeBytes((Constants.NOV_EVENT_TYPE + " " + a_event.vid1 + " " + a_event.vid2 + " " + a_event.xway + " " + a_event.mile + " " + a_event.dir ).getBytes());
							    bytesMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
							    producer_toll.send(bytesMessage);
							}catch(JMSException e){
								e.printStackTrace();
							}
							
							t_car.notified = true;
							c.notified = true;
							flag = 1;
						}
						//The cars c and t_car have smashed with each other
						add_smashed_cars(c);
						add_smashed_cars(t_car);
						
						break;
					}
				}
			}
		}
	}
    
	public static LinkedList smashed_cars = new LinkedList();
	public static LinkedList stopped_cars = new LinkedList();
	public static LinkedList accidents = new LinkedList();
	
	public boolean is_smashed_car(Car car) {
		for (int i = 0; i < smashed_cars.size(); i++) {
			Car t_car = (Car)smashed_cars.get(i);

			if (((Car)smashed_cars.get(i)).carid == car.carid){
				return true;
			}
		}
		return false;
	}
	
	public void add_smashed_cars(Car c) {
		for (int i = 0; i < smashed_cars.size(); i++) {
			Car t_car = (Car)smashed_cars.get(i);
			if (c.carid == t_car.carid) {
				smashed_cars.remove(i);
			}
		}
		smashed_cars.add(c);
	}
	
	public boolean is_stopped_car(Car c) {
		for (int i = 0; i < stopped_cars.size(); i++) {
			Car t_car = (Car)stopped_cars.get(i);
			if (c.carid == t_car.carid) {
				return true;
			}
		}
		return false;
	}
	
	public void remove_from_smashed_cars(Car c) {
		for (int i = 0; i < smashed_cars.size(); i++) {
			Car t_car = (Car)smashed_cars.get(i);
			if (c.carid == t_car.carid) {
				smashed_cars.remove();
			}
		}
	}
	
	public void remove_from_stopped_cars(Car c) {
		for (int i = 0; i < stopped_cars.size(); i++) {
			Car t_car = (Car)stopped_cars.get(i);
			if (c.carid == t_car.carid) {
				stopped_cars.remove();
			}
		}
	}
	
	public void renew_stopped_car(Car c) {
		for (int i = 0; i < stopped_cars.size(); i++) {
			Car t_car = (Car)stopped_cars.get(i);
			if (c.carid == t_car.carid) {
				c.xway3 = t_car.xway2;
				c.xway2 = t_car.xway1;
				c.xway1 = t_car.xway0;
				c.mile3 = t_car.mile2;
				c.mile2 = t_car.mile1;
				c.mile1 = t_car.mile0;				
				c.lane3 = t_car.lane2;
				c.lane2 = t_car.lane1;
				c.lane1 = t_car.lane0;
				c.offset3 = t_car.offset2;
				c.offset2 = t_car.offset1;
				c.offset1 = t_car.offset0;				
				c.dir3 = t_car.dir2;
				c.dir2 = t_car.dir1;
				c.dir1 = t_car.dir0;
				c.notified = t_car.notified;
				c.posReportID = (byte)(t_car.posReportID + 1);
				
				stopped_cars.remove(i);
				stopped_cars.add(c);
				
				//Since we already found the car from the list we break at here
				break;
			}
		}
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
