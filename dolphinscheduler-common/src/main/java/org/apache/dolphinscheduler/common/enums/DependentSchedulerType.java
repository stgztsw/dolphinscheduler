package org.apache.dolphinscheduler.common.enums;

import com.baomidou.mybatisplus.annotation.EnumValue;

public enum DependentSchedulerType {

    NON_SCHEDULER(0, "not scheduler"),
    SCHEDULER(1, "scheduler"),
    MANUAL_SCHEDULER(2,"manual scheduler"),
    RECOVER(3, "recover task"),
    REPEAT(4, "repeat");

    DependentSchedulerType(int code, String descp){
        this.code = code;
        this.descp = descp;
    }

    @EnumValue
    private final int code;
    private final String descp;

    public int getCode() {
        return code;
    }

    public String getDescp() {
        return descp;
    }

    public boolean isScheduler() {
        return this == SCHEDULER || this == MANUAL_SCHEDULER;
    }
}
