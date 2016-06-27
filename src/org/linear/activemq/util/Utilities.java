package org.linear.activemq.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * This is a utility class
 * @author miyuru
 *
 */
public class Utilities {
	
	/**
	 *  This method is for obtaining the time stamp of the system. The time stamp is of the following format
	 *  <yy/MM/dd HH:mm:ss>-<time stamp in milisecond>
	 * @return
	 */
	public static String getTimeStamp(){	
		DateFormat dateFormat = new SimpleDateFormat("yy/MM/dd HH:mm:ss");
		Calendar cal = Calendar.getInstance();
		
		return dateFormat.format(cal.getTime())+"-"+cal.getTimeInMillis();
	}
}
