package org.apache.dolphinscheduler.service.quartz.cron;

import java.util.Date;

public class SchedulingBatch {

    private Date schedulerTime;

    private int schedulerInterval;

    private int nextBatchNo;

    public SchedulingBatch(Date schedulerTime, int schedulerInterval, int nextBatchNo) {
        this.schedulerTime = schedulerTime;
        this.schedulerInterval = schedulerInterval;
        this.nextBatchNo = nextBatchNo;
    }

    public Date getSchedulerTime() {
        return schedulerTime;
    }

    public void setSchedulerTime(Date schedulerTime) {
        this.schedulerTime = schedulerTime;
    }

    public int getSchedulerInterval() {
        return schedulerInterval;
    }

    public void setSchedulerInterval(int schedulerInterval) {
        this.schedulerInterval = schedulerInterval;
    }

    public int getNextBatchNo() {
        return nextBatchNo;
    }

    public void setNextBatchNo(int nextBatchNo) {
        this.nextBatchNo = nextBatchNo;
    }
}
