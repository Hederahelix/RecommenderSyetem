package Dat2Db;

public class Record {
	private int iid;
	private int uid;
	private double timestamp;
	
	public Record(int uid,int iid,double timestamp) 
	{ 
		this.iid = iid; 
		this.uid = uid; 
		this.timestamp = timestamp; 
	}

	public int getIid() {
		return iid;
	}

	public void setIid(int iid) {
		this.iid = iid;
	}

	public int getUid() {
		return uid;
	}

	public void setUid(int uid) {
		this.uid = uid;
	}

	public double getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(double timestamp) {
		this.timestamp = timestamp;
	} 
}
