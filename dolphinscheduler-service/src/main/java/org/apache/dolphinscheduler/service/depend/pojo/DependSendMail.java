package org.apache.dolphinscheduler.service.depend.pojo;

import org.apache.dolphinscheduler.service.depend.enums.IntervalType;

import java.util.HashSet;
import java.util.LinkedHashMap;

public class DependSendMail {

    private IntervalType intervalType;

    private  Integer totalCount = 0;

    private  Integer successCount = 0;

    private   Integer faildCount = 0;

    private   Integer execCount = 0;

    private   Integer unExecCount = 0;

    private   LinkedHashMap<Integer, Object> faildObjs = new LinkedHashMap<>();

    private   LinkedHashMap<Integer, Object> execObjs = new LinkedHashMap<>();

    private   LinkedHashMap<Integer, Object> unExecObjs = new LinkedHashMap<>();

    public DependSendMail(IntervalType intervalType) {
        this.intervalType = intervalType;
    }

    public DependSendMail() {
    }

    public void clear() {
        this.totalCount = 0;
        this.successCount = 0;
        this.faildCount = 0;
        this.execCount = 0;
        this.unExecCount = 0;
        this.faildObjs.clear();
        this.execObjs.clear();
        this.unExecObjs.clear();
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

    public LinkedHashMap<Integer, Object> getFaildObjs() {
        return faildObjs;
    }

    public void addFaildObjs(Integer id, Object obj) {
        this.faildObjs.put(id,obj);
    }

    public LinkedHashMap<Integer, Object> getExecObjs() {
        return execObjs;
    }

    public void addExecObjs(Integer id, Object obj) {
        this.execObjs.put(id,obj);
    }

    public LinkedHashMap<Integer, Object> getUnExecObjs() {
        return unExecObjs;
    }

    public void addUnExecObjs(Integer id, Object obj) {
        this.unExecObjs.put(id,obj);
    }
}
