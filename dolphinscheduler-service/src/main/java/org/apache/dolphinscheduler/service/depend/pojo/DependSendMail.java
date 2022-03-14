package org.apache.dolphinscheduler.service.depend.pojo;

import org.apache.dolphinscheduler.service.depend.enums.IntervalType;

import java.util.HashSet;
import java.util.LinkedHashMap;

public class DependSendMail extends DependSendMailBase{

    private   LinkedHashMap<Integer, Object> faildObjs = new LinkedHashMap<>();

    private   LinkedHashMap<Integer, Object> execObjs = new LinkedHashMap<>();

    private   LinkedHashMap<Integer, Object> unExecObjs = new LinkedHashMap<>();

    public DependSendMail(IntervalType intervalType) {
        this.intervalType = intervalType;
    }

    public DependSendMail() {
    }


    @Override
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
