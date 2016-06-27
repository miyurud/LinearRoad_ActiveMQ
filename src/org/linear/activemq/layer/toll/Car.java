package org.linear.activemq.layer.toll;

public class Car {
		long time;
		int carid;
		byte speed, mile, xway, lane, dir, offset;
		boolean notified;
		
		public Car() {
			this.time = -1;
			this.carid = 0;
			this.speed = 0;
			this.xway = -1; 
			this.lane = -1;
			this.dir = -1;
		}
		
		public Car(long time, int carid, byte speed, byte xway0, byte lane0, byte dir0, byte mile) {
			this.time = time; this.carid = carid; this.speed = speed;
			this.xway = xway0; this.lane = lane0; this.dir = dir0;
			this.mile = mile;
		}
}
