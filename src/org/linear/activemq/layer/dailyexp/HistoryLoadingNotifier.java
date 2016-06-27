/**
 * 
 */
package org.linear.activemq.layer.dailyexp;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;

import org.linear.activemq.util.Constants;

/**
 * @author miyuru
 *
 */
public class HistoryLoadingNotifier extends Thread {
	private ServerSocket svr;
	private ArrayList<HistoryLoadingNotifierSession> sessionList;
	private boolean statusFlag;
	
	public HistoryLoadingNotifier(boolean status){
		statusFlag = status;
		sessionList = new ArrayList<HistoryLoadingNotifierSession>();
	}
	
	public void setStatus(boolean flg){
		statusFlag = flg;
		
		Iterator<HistoryLoadingNotifierSession> itr = sessionList.iterator();
		while(itr.hasNext()){
			HistoryLoadingNotifierSession obj = itr.next();
			obj.setStatus(statusFlag);
		}
	}
	
	public void run(){
		try {
			svr = new ServerSocket(Constants.HISTORY_LOADING_NOTIFIER_PORT);
			
			while (true){
				Socket skt = svr.accept();
				HistoryLoadingNotifierSession session = new HistoryLoadingNotifierSession(skt, statusFlag);
				sessionList.add(session);
				session.start();//start running the thread
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
