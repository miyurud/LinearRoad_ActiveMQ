/**************************************************************************************
 * Copyright (C) 2008 EsperTech, Inc. All rights reserved.                            *
 * http://esper.codehaus.org                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package org.linear.activemq.layer.accbalance;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.example.servershell.jms.JMSContext;
import com.espertech.esper.example.servershell.jms.JMSContextFactory;
import com.espertech.esper.example.servershell.jmx.EPServiceProviderJMX;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.linear.activemq.util.Constants;

import javax.jms.MessageConsumer;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;

import java.io.InputStream;
import java.rmi.registry.LocateRegistry;
import java.util.Properties;

public class AccountBalance
{
    private static Log log = LogFactory.getLog(AccountBalance.class);

    private boolean isShutdown;

    public static void main(String[] args) throws Exception
    {
        try
        {
           new AccountBalance();
        }
        catch (Throwable t)
        {
            log.error("Error starting server shell : " + t.getMessage(), t);
            System.exit(-1);
        }
    }

    public AccountBalance() throws Exception
    {
        log.info("Loading properties");
        Properties properties = new Properties();
        InputStream propertiesIS = AccountBalance.class.getClassLoader().getResourceAsStream(Constants.CONFIG_FILENAME);
        if (propertiesIS == null)
        {
            throw new RuntimeException("Properties file '" + Constants.CONFIG_FILENAME + "' not found in classpath");
        }
        properties.load(propertiesIS);

        // Start RMI registry
        log.info("Starting RMI registry");
        int port = Integer.parseInt(properties.getProperty(Constants.MGMT_RMI_PORT_ACCBALANCE_LAYER));
        LocateRegistry.createRegistry(port);

        // Obtain MBean servera
        log.info("Obtaining JMX server and connector");
        MBeanServer mbs = MBeanServerFactory.createMBeanServer();
        String jmxServiceURL = properties.getProperty(Constants.MGMT_SERVICE_URL_ACCBALANCE_LAYER);
        JMXServiceURL jmxURL = new JMXServiceURL(jmxServiceURL);
        JMXConnectorServer cs = JMXConnectorServerFactory.newJMXConnectorServer(jmxURL, null, mbs);
        cs.start();

        // Initialize engine
        log.info("Getting Esper engine instance");
        Configuration configuration = new Configuration();
        //configuration.addEventType("NumEvent", NumEvent.class.getName());
        EPServiceProvider engine = EPServiceProviderManager.getDefaultProvider(configuration);
        
        // Initialize engine
        log.info("Creating sample statement");
        //SampleStatement stmt = new SampleStatement();
        //stmt.createStatement(engine.getEPAdministrator());

        // Register MBean
        log.info("Registering MBean");
        ObjectName name = new ObjectName(Constants.MGMT_MBEAN_NAME);
        EPServiceProviderJMX mbean = new EPServiceProviderJMX(engine);
        mbs.registerMBean(mbean, name);

        // Connect to JMS
        log.info("Connecting to JMS server");
        String factory = properties.getProperty(Constants.JMS_CONTEXT_FACTORY);
        String jmsurl = properties.getProperty(Constants.JMS_PROVIDER_URL_ACCBALANCE_LAYER);
        String connFactoryName = properties.getProperty(Constants.JMS_CONNECTION_FACTORY_NAME);
        String user = properties.getProperty(Constants.JMS_USERNAME);
        String password = properties.getProperty(Constants.JMS_PASSWORD);
        String destination = properties.getProperty(Constants.JMS_INCOMING_DESTINATION_ACCBALANCE_LAYER);
        boolean isTopic = Boolean.parseBoolean(properties.getProperty(Constants.JMS_IS_TOPIC));
        JMSContext jmsCtx = JMSContextFactory.createContext(factory, jmsurl, connFactoryName, user, password, destination, isTopic);

        int numListeners = Integer.parseInt(properties.getProperty(Constants.JMS_NUM_LISTENERS));
        log.info("Creating " + numListeners + " listeners to destination '" + destination + "'");

        AccountBalanceListener listeners[] = new AccountBalanceListener[numListeners];
        for (int i = 0; i < numListeners; i++)
        {
            listeners[i] = new AccountBalanceListener(engine.getEPRuntime());
            MessageConsumer consumer = jmsCtx.getSession().createConsumer(jmsCtx.getDestination());
            consumer.setMessageListener(listeners[i]);
        }

        // Start processing
        log.info("Starting JMS connection");
        jmsCtx.getConnection().start();

        // Register shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread()
        {
            public void run()
            {
                isShutdown = true;
            }
        });

        do
        {
            Thread.sleep(1000);
        }
        while (!isShutdown);

        log.info("Shutting down server");
        jmsCtx.destroy();

        log.info("Exiting");
        System.exit(-1);
    }
}
