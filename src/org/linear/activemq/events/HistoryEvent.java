/**
 * 
 */
package org.linear.activemq.events;

/**
 * @author miyuru
 *
 */
public class HistoryEvent {
	public HistoryEvent(int carid, int d, int x, int daily_exp) {
		super();
		this.carid = carid;
		this.d = d;
		this.x = x;
		this.daily_exp = daily_exp;
	}
	//carid, d, x, daily_exp
	public int carid;
	public int d;
	public int x;
	public int daily_exp;
	
	
}
