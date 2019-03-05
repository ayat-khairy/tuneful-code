package cl.cam.ac.uk.tuneful.wl.characterization;


public class ExectuionProvenance {
	private int appId;
	private double avgExecutorRunTime;
	private double avgexecutorCpuTimeToMemoryRatio;
	private double AvgInOutRatio;
	private double avgShuffleReadBytes;
	private double avgShuffleWriteBytes;
	private double avgMemoryBytesSpilled;
	private double avgDiskBytesSpilled;
	private double avgExecutorDeserializeCpuTimeRatio;
	private double avgResultSize;
	private int numberOfStages;
	private double avgCPUToGCTime;
	private double ConfMemoryPerCore;
	private long duration;
	private String name;
	

	public int getAppId() {
		return appId;
	}

	public void setAppId(int i) {
		this.appId = i;
	}

	public double getAvgExecutorRunTime() {
		return avgExecutorRunTime;
	}

	public void setAvgExecutorRunTime(double avgExecutorRunTime2) {
		this.avgExecutorRunTime = avgExecutorRunTime2;
	}

	public double getAvgexecutorCpuTimeToMemoryRatio() {
		return avgexecutorCpuTimeToMemoryRatio;
	}

	public void setAvgexecutorCpuTimeToMemoryRatio(double avgexecutorCpuTimeToMemoryRatio2) {
		this.avgexecutorCpuTimeToMemoryRatio = avgexecutorCpuTimeToMemoryRatio2;
	}

	public double getAvgInOutRatio() {
		return AvgInOutRatio;
	}

	public void setAvgInOutRatio(double avgInOutRatio2) {
		AvgInOutRatio = avgInOutRatio2;
	}

	public double getAvgShuffleReadBytes() {
		return avgShuffleReadBytes;
	}

	public void setAvgShuffleReadBytes(double avgShuffleReadBytes2) {
		this.avgShuffleReadBytes = avgShuffleReadBytes2;
	}

	public double getAvgShuffleWriteBytes() {
		return avgShuffleWriteBytes;
	}

	public void setAvgShuffleWriteBytes(double avgShuffleWriteBytes2) {
		this.avgShuffleWriteBytes = avgShuffleWriteBytes2;
	}

	public double getAvgMemoryBytesSpilled() {
		return avgMemoryBytesSpilled;
	}

	public void setAvgMemoryBytesSpilled(double avgMemoryBytesSpilled2) {
		this.avgMemoryBytesSpilled = avgMemoryBytesSpilled2;
	}

	public double getAvgDiskBytesSpilled() {
		return avgDiskBytesSpilled;
	}

	public void setAvgDiskBytesSpilled(double avgDiskBytesSpilled2) {
		this.avgDiskBytesSpilled = avgDiskBytesSpilled2;
	}

	public double getAvgExecutorDeserializeCpuTimeRatio() {
		return avgExecutorDeserializeCpuTimeRatio;
	}

	public void setAvgExecutorDeserializeCpuTimeRatio(double avgExecutorDeserializeCpuTimeRatio2) {
		this.avgExecutorDeserializeCpuTimeRatio = avgExecutorDeserializeCpuTimeRatio2;
	}

	public double getAvgResultSize() {
		return avgResultSize;
	}

	public void setAvgResultSize(double avgResultSize2) {
		this.avgResultSize = avgResultSize2;
	}

	public int getNumberOfStages() {
		return numberOfStages;
	}

	public void setNumberOfStages(int numberOfStages) {
		this.numberOfStages = numberOfStages;
	}

	public double getAvgGCTime() {
		return avgCPUToGCTime;
	}

	public void setAvgGCTime(double avgGCTime2) {
		this.avgCPUToGCTime = avgGCTime2;
	}

	public double getConfMemoryPerCore() {
		return ConfMemoryPerCore;
	}

	public void setConfMemoryPerCore(double confMemoryPerCore2) {
		ConfMemoryPerCore = confMemoryPerCore2;
	}

	public String asString() {
	 String asStr = 
			 //avgExecutorRunTime + ","  +
	avgexecutorCpuTimeToMemoryRatio +  ","  + AvgInOutRatio +  
			 ","  +avgShuffleReadBytes + ","  + avgShuffleWriteBytes + 
//			 ","  +avgMemoryBytesSpilled + ","  + avgDiskBytesSpilled + 
			 ","  +avgExecutorDeserializeCpuTimeRatio+ ","  + avgResultSize+  ","  +numberOfStages+  ","  +avgCPUToGCTime;
//			 + "," + ConfMemoryPerCore;
		return asStr;
	}

	public long getDuration() {
		return duration;
	}

	public void setDuration(long duration) {
		this.duration = duration;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}


}
