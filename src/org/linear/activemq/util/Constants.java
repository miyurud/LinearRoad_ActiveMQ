/**************************************************************************************
 * Copyright (C) 2008 EsperTech, Inc. All rights reserved.                            *
 * http://esper.codehaus.org                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package org.linear.activemq.util;


public class Constants
{
    public static final String CONFIG_FILENAME = "servershell_config.properties";

    public static final String JMS_CONTEXT_FACTORY = "jms-context-factory";
    public static final String JMS_PROVIDER_URL_ACCIDENTS_LAYER = "jms-provider-url-acc";
    public static final String JMS_PROVIDER_URL_SEGSTATS_LAYER = "jms-provider-url-segstat";
    public static final String JMS_PROVIDER_URL_TOLL_LAYER = "jms-provider-url-toll";
    public static final String JMS_PROVIDER_URL_TRAVEL_TIME_LAYER = "jms-provider-url-traveltime";
    public static final String JMS_PROVIDER_URL_ACCBALANCE_LAYER = "jms-provider-url-accbalance";
    public static final String JMS_PROVIDER_URL_DAILYEXP_LAYER = "jms-provider-url-dailyexp";
    public static final String JMS_PROVIDER_URL_OUTPUT_LAYER = "jms-provider-url-output";
    
    public static final String JMS_CONNECTION_FACTORY_NAME = "jms-connection-factory-name";
    public static final String JMS_USERNAME = "jms-user";
    public static final String JMS_PASSWORD = "jms-password";
    
    public static final String JMS_INCOMING_DESTINATION_ACCIDENTS_LAYER = "jms-incoming-destination-acc";
    public static final String JMS_INCOMING_DESTINATION_SEGSTATS_LAYER = "jms-incoming-destination-segstat";
    public static final String JMS_INCOMING_DESTINATION_TOLL_LAYER = "jms-incoming-destination-toll";
    public static final String JMS_INCOMING_DESTINATION_TRAVEL_TIME_LAYER = "jms-incoming-destination-traveltime";
    public static final String JMS_INCOMING_DESTINATION_ACCBALANCE_LAYER = "jms-incoming-destination-accbalance";
    public static final String JMS_INCOMING_DESTINATION_DAILYEXP_LAYER = "jms-incoming-destination-dailyexp";
    public static final String JMS_INCOMING_DESTINATION_OUTPUT_LAYER = "jms-incoming-destination-output";
    
    public static final String JMS_IS_TOPIC = "jms-is-topic";
    public static final String JMS_NUM_LISTENERS = "jms-num-listeners";

    public static final String MGMT_RMI_PORT = "rmi-port";
    public static final String MGMT_RMI_PORT_ACCIDENTS_LAYER = "rmi-port-acc";
    public static final String MGMT_RMI_PORT_SEGSTATS_LAYER = "rmi-port-segstat";
    public static final String MGMT_RMI_PORT_TOLL_LAYER = "rmi-port-toll";
    public static final String MGMT_RMI_PORT_TRAVEL_TIME_LAYER = "rmi-port-traveltime";
    public static final String MGMT_RMI_PORT_ACCBALANCE_LAYER = "rmi-port-accbalance";
    public static final String MGMT_RMI_PORT_DAILYEXP_LAYER = "rmi-port-dailyexp";
    public static final String MGMT_RMI_PORT_OUTPUT_LAYER = "rmi-port-output";
    
    public static final String MGMT_SERVICE_URL = "jmx-service-url";
    public static final String MGMT_SERVICE_URL_ACCIDENTS_LAYER = "jmx-service-url-acc";
    public static final String MGMT_SERVICE_URL_SEGSTATS_LAYER = "jmx-service-url-segstat";
    public static final String MGMT_SERVICE_URL_TOLL_LAYER = "jmx-service-url-toll";
    public static final String MGMT_SERVICE_URL_TRAVEL_TIME_LAYER = "jmx-service-url-traveltime";
    public static final String MGMT_SERVICE_URL_ACCBALANCE_LAYER = "jmx-service-url-accbalance";
    public static final String MGMT_SERVICE_URL_DAILYEXP_LAYER = "jmx-service-url-dailyexp";
    public static final String MGMT_SERVICE_URL_OUTPUT_LAYER = "jmx-service-url-output";
    public static final String MGMT_MBEAN_NAME = "com.espertech.esper.mbean:type=EPServiceProviderJMXMBean";
    
    public static final String LINEAR_HISTORY = "linear-history-file";
    public static final String LINEAR_CAR_DATA_POINTS = "linear-cardatapoints-file";
    
    public static final int POS_EVENT_TYPE = 0;
    public static final int ACC_BAL_EVENT_TYPE = 2;
    public static final int DAILY_EXP_EVENT_TYPE = 3;
    public static final int TRAVELTIME_EVENT_TYPE = 4;
    public static final int NOV_EVENT_TYPE = -5;
    public static final int LAV_EVENT_TYPE = 6;
    public static final int TOLL_EVENT_TYPE = 7;
    public static final int ACCIDENT_EVENT_TYPE = 8;
    
    public static final String LINEAR_DB_HOST = "linear-db-host";
    public static final String LINEAR_DB_PORT = "linear-db-port";
    
    public static final int HISTORY_LOADING_NOTIFIER_PORT = 2233;
    
    public static final String CLEAN_START = "clean-start";
}
