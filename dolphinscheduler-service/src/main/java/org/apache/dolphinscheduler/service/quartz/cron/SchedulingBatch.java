package org.apache.dolphinscheduler.service.quartz.cron;

import org.apache.dolphinscheduler.common.model.DateInterval;
import org.apache.dolphinscheduler.common.utils.DependentUtils;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;

import java.util.Date;
import java.util.List;

public class SchedulingBatch {

    private Date schedulerTime;

    private int schedulerInterval;

    private int batchNo;

    private DateInterval dateInterval;

    public SchedulingBatch(Date schedulerTime, int schedulerInterval, int batchNo) {
        this.schedulerTime = schedulerTime;
        this.schedulerInterval = schedulerInterval;
        this.batchNo = batchNo;
    }

    public SchedulingBatch(ProcessInstance processInstance) {
        this.schedulerTime = processInstance.getScheduleTime();
        this.schedulerInterval = processInstance.getSchedulerInterval();
        this.batchNo = processInstance.getSchedulerBatchNo();
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

    public int getBatchNo() {
        return batchNo;
    }

    public void setBatchNo(int batchNo) {
        this.batchNo = batchNo;
    }

    public int getNextBatchNo() {
        return batchNo + 1;
    }

    public DateInterval getDateInterval() {
        if (dateInterval != null) {
            return dateInterval;
        }
        List<DateInterval> dateIntervals = DependentUtils
                .getDateIntervalListForDependent(this.schedulerTime, this.schedulerInterval);
        this.dateInterval = dateIntervals.get(0);
        return dateInterval;
    }

    public Date getStartTime() {
        return getDateInterval().getStartTime();
    }

    public Date getEndTime() {
        return getDateInterval().getEndTime();
    }
}
