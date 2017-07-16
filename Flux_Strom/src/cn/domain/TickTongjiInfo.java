package cn.domain;

import java.sql.Timestamp;

public class TickTongjiInfo {
	private Timestamp tickTime;
	private double br;
	private double avgtime;
	private double avgdeep;
	public Timestamp getTickTime() {
		return tickTime;
	}
	public void setTickTime(Timestamp tickTime) {
		this.tickTime = tickTime;
	}
	public double getBr() {
		return br;
	}
	public void setBr(double br) {
		this.br = br;
	}
	public double getAvgtime() {
		return avgtime;
	}
	public void setAvgtime(double avgtime) {
		this.avgtime = avgtime;
	}
	public double getAvgdeep() {
		return avgdeep;
	}
	public void setAvgdeep(double avgdeep) {
		this.avgdeep = avgdeep;
	}
	
	
}
