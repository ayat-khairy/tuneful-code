package cl.cam.ac.uk.tuneful.wl.characterization;

import java.util.Date;

public class StageExectuionProvenance {
	private int appId;
	private long executorRunTime;
	private long executorCpuTime;
	private Date submissionTime;
	private Date firstTaskLaunchedTime;
	private Date completionTime;
	private long inputBytes;
	private int inputRecords;
	private long outputBytes;
	private int outputRecords;
	private long shuffleReadBytes;
	private int shuffleReadRecords;
	private long shuffleWriteBytes;
	private int shuffleWriteRecords;
	private long memoryBytesSpilled;
	private long diskBytesSpilled;
	private long executorDeserializeCpuTime;
	private long executorDeserializeTime;
	private long resultSize;
	private long gcTime;

	public int getAppId() {
		return appId;
	}

	public void setAppId(int i) {
		this.appId = i;
	}

	public long getExecutorRunTime() {
		return executorRunTime;
	}

	public void setExecutorRunTime(long executorRunTime) {
		this.executorRunTime = executorRunTime;
	}

	public long getExecutorCpuTime() {
		return executorCpuTime;
	}

	public void setExecutorCpuTime(long executorCpuTime) {
		this.executorCpuTime = executorCpuTime;
	}

	public Date getFirstTaskLaunchedTime() {
		return firstTaskLaunchedTime;
	}

	public void setFirstTaskLaunchedTime(Date firstTaskLaunchedTime) {
		this.firstTaskLaunchedTime = firstTaskLaunchedTime;
	}

	public Date getSubmissionTime() {
		return submissionTime;
	}

	public void setSubmissionTime(Date submissionTime) {
		this.submissionTime = submissionTime;
	}

	public Date getCompletionTime() {
		return completionTime;
	}

	public void setCompletionTime(Date completionTime) {
		this.completionTime = completionTime;
	}

	public int getInputRecords() {
		return inputRecords;
	}

	public void setInputRecords(int inputRecords) {
		this.inputRecords = inputRecords;
	}

	public long getOutputBytes() {
		return outputBytes;
	}

	public void setOutputBytes(long outputBytes) {
		this.outputBytes = outputBytes;
	}

	public int getOutputRecords() {
		return outputRecords;
	}

	public void setOutputRecords(int outputRecords) {
		this.outputRecords = outputRecords;
	}

	public long getShuffleReadBytes() {
		return shuffleReadBytes;
	}

	public void setShuffleReadBytes(long shuffleReadBytes) {
		this.shuffleReadBytes = shuffleReadBytes;
	}

	public int getShuffleReadRecords() {
		return shuffleReadRecords;
	}

	public void setShuffleReadRecords(int shuffleReadRecords) {
		this.shuffleReadRecords = shuffleReadRecords;
	}

	public long getShuffleWriteBytes() {
		return shuffleWriteBytes;
	}

	public void setShuffleWriteBytes(long shuffleWriteBytes) {
		this.shuffleWriteBytes = shuffleWriteBytes;
	}

	public int getShuffleWriteRecords() {
		return shuffleWriteRecords;
	}

	public void setShuffleWriteRecords(int shuffleWriteRecords) {
		this.shuffleWriteRecords = shuffleWriteRecords;
	}

	public long getMemoryBytesSpilled() {
		return memoryBytesSpilled;
	}

	public void setMemoryBytesSpilled(long memoryBytesSpilled) {
		this.memoryBytesSpilled = memoryBytesSpilled;
	}

	public long getDiskBytesSpilled() {
		return diskBytesSpilled;
	}

	public void setDiskBytesSpilled(long l) {
		this.diskBytesSpilled = l;
	}

	public long getInputBytes() {
		return inputBytes;
	}

	public void setInputBytes(long inputBytes) {
		this.inputBytes = inputBytes;
	}

	public long getExecutorDeserializeCpuTime() {
		return executorDeserializeCpuTime;
	}

	public void setExecutorDeserializeCpuTime(long executorDeserializeCpuTime) {
		this.executorDeserializeCpuTime = executorDeserializeCpuTime;
	}

	public long getExecutorDeserializeTime() {
		return executorDeserializeTime;
	}

	public void setExecutorDeserializeTime(long executorDeserializeTime) {
		this.executorDeserializeTime = executorDeserializeTime;
	}

	public long getResultSize() {
		return resultSize;
	}

	public void setResultSize(long resultSize) {
		this.resultSize = resultSize;
	}
	public void setGCTime(long gcTime) {
		this.gcTime = gcTime;
	}
	public long getGCTime() {
		return gcTime;
	}

}
