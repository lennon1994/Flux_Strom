package cn.domain;

import java.sql.Date;
import java.sql.Timestamp;

public class TongjiInfo {
	private Timestamp sstime; 
	private int pv;
	private int uv;
	private int vv;
	private int newip;
	private int newcust;
	
	public Timestamp getSstime() {
		return sstime;
	}
	public void setSstime(Timestamp sstime) {
		this.sstime = sstime;
	}
	public int getPv() {
		return pv;
	}
	public void setPv(int pv) {
		this.pv = pv;
	}
	public int getUv() {
		return uv;
	}
	public void setUv(int uv) {
		this.uv = uv;
	}
	public int getVv() {
		return vv;
	}
	public void setVv(int vv) {
		this.vv = vv;
	}
	public int getNewip() {
		return newip;
	}
	public void setNewip(int newip) {
		this.newip = newip;
	}
	public int getNewcust() {
		return newcust;
	}
	public void setNewcust(int newcust) {
		this.newcust = newcust;
	}
}
