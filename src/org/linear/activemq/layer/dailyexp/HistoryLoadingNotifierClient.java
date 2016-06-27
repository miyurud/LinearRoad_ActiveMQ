/**
 * 
 */
package org.linear.activemq.layer.dailyexp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.linear.activemq.util.Constants;
import org.linear.activemq.layer.dailyexp.DailyExpenses;

/**
 * @author miyuru
 *
 */
public class HistoryLoadingNotifierClient {
	private static Log log = LogFactory.getLog(DailyExpenses.class);
	
	public static boolean isHistoryLoaded(){
		boolean result = false;
		
		String host = null;
		
        log.info("Loading properties");
        Properties properties = new Properties();
        InputStream propertiesIS = DailyExpenses.class.getClassLoader().getResourceAsStream(Constants.CONFIG_FILENAME);
        if (propertiesIS == null)
        {
            throw new RuntimeException("Properties file '" + Constants.CONFIG_FILENAME + "' not found in classpath");
        }
        try {
			properties.load(propertiesIS);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
        String jmxServiceURL = properties.getProperty(Constants.MGMT_SERVICE_URL_DAILYEXP_LAYER);
        
        String[] res = jmxServiceURL.split("service:jmx:rmi:///jndi/rmi://");
        host = res[1];
        res = host.split(":");
        host = res[0];
		
		try {
			Socket skt = new Socket(host, Constants.HISTORY_LOADING_NOTIFIER_PORT);
			PrintWriter out = new PrintWriter(skt.getOutputStream());
			BufferedReader buff = new BufferedReader(new InputStreamReader(skt.getInputStream()));
			
			out.println("done?");
			out.flush();
			
			String response = buff.readLine();
			if(response != null){
					if(response.equals("yes")){
						result = true;
					}				
			}
			out.close();
			buff.close();
			skt.close();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return result;
	}
}
