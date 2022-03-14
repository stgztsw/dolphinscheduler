package org.apache.dolphinscheduler.service.depend.pojo;

import org.apache.dolphinscheduler.service.depend.enums.IntervalType;

/**
 * @program: dolphinscheduler
 * @description:
 * @author: Mr.Yang
 * @create: 2022-03-11 09:49
 **/

public class DependSendMailBase {

    public IntervalType intervalType;

    public Integer totalCount = 0;

    public Integer successCount = 0;

    public Integer faildCount = 0;

    public Integer execCount = 0;

    public Integer unExecCount = 0;

    public DependSendMailBase(){

    }

    public DependSendMailBase(IntervalType intervalType) {
        this.intervalType = intervalType;
    }

    public void clear() {
        this.totalCount = 0;
        this.successCount = 0;
        this.faildCount = 0;
        this.execCount = 0;
        this.unExecCount = 0;
    }

    public IntervalType getIntervalType() {
        return intervalType;
    }

    public void setIntervalType(IntervalType intervalType) {
        this.intervalType = intervalType;
    }

    public Integer getTotalCount() {
        return totalCount;
    }

    public void addTotalCount(Integer totalCount) {
        this.totalCount+=totalCount;
    }

    public Integer getSuccessCount() {
        return successCount;
    }

    public void addSuccessCount(Integer successCount) {
        this.successCount+=successCount;
    }

    public Integer getFaildCount() {
        return faildCount;
    }

    public void addFaildCount(Integer faildCount) {
        this.faildCount+=faildCount;
    }

    public Integer getExecCount() {
        return execCount;
    }

    public void addExecCount(Integer execCount) {
        this.execCount+=execCount;
    }

    public Integer getUnExecCount() {
        return unExecCount;
    }

    public void addUnExecCount(Integer unExecCount) {
        this.unExecCount+=unExecCount;
    }
}
