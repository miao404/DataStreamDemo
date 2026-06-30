package org.example.stream.metrics;

public class SampleLogEvent {
    public long eventTimeStamp;
    public String partitionLogChannel;
    public String partitionApp;
    public String sessionKey;
    public String requestKey;
    public Long sequenceKey;
    public String Log;
    public String logType;
    public SampleFlowInfo sampleFlowInfo;

    public static Long sequenceId = new Long(0);

    public SampleLogEvent() {
    }

    public SampleLogEvent(long eventTimeStamp) {
        this.eventTimeStamp = eventTimeStamp;
        long id = sequenceId % 1000;
        sequenceId += 1;
        this.sequenceKey = Thread.currentThread().getId() * 1000 + id;
        this.partitionLogChannel = "channel " + String.valueOf(id);
        this.partitionApp = "app " + String.valueOf(id);
        this.requestKey = "request " + String.valueOf(id);
    }


    public SampleFlowInfo getSampleFlowInfo() {
        return sampleFlowInfo;
    }

    public void setSampleFlowInfo(SampleFlowInfo sampleFlowInfo) {
        this.sampleFlowInfo = sampleFlowInfo;
    }

    public long getEventTimeStamp() {
        return eventTimeStamp;
    }

    public void setEventTimeStamp(long eventTimeStamp) {
        this.eventTimeStamp = eventTimeStamp;
    }

    public String getPartitionLogChannel() {
        return partitionLogChannel;
    }

    public void setPartitionLogChannel(String partitionLogChannel) {
        this.partitionLogChannel = partitionLogChannel;
    }

    public String getPartitionApp() {
        return partitionApp;
    }

    public void setPartitionApp(String partitionApp) {
        this.partitionApp = partitionApp;
    }

    public String getSessionKey() {
        return sessionKey;
    }

    public void setSessionKey(String sessionKey) {
        this.sessionKey = sessionKey;
    }

    public String getRequestKey() {
        return requestKey;
    }

    public void setRequestKey(String requestKey) {
        this.requestKey = requestKey;
    }

    public Long getSequenceKey() {
        return sequenceKey;
    }

    public void setSequenceKey(Long sequenceKey) {
        this.sequenceKey = sequenceKey;
    }

    public String getLog() {
        return Log;
    }

    public void setLog(String log) {
        Log = log;
    }

    public String getLogType() {
        return logType;
    }

    public void setLogType(String logType) {
        this.logType = logType;
    }

    public static Long getSequenceId() {
        return sequenceId;
    }

    public static void setSequenceId(Long sequenceId) {
        SampleLogEvent.sequenceId = sequenceId;
    }

    public String duplicateKey() {
        return this.getPartitionLogChannel() + ","
                + this.getPartitionApp() + ","
                + this.getSessionKey() + ","
                + this.getRequestKey() + ","
                + this.getSequenceKey();
    }

    public String joinKey() {
        return this.getPartitionLogChannel() + ","
                + this.getPartitionApp() + ","
                + this.getSessionKey() + ","
                + this.getRequestKey() + ",";
    }
}
