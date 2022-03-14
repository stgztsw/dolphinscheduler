package org.apache.dolphinscheduler.common.enums;

import com.baomidou.mybatisplus.annotation.EnumValue;

public enum DependentSchedulerType {

    // 依赖
    NON_SCHEDULER(0, "not scheduler"),
    // 调度
    SCHEDULER(1, "scheduler"),
    // 手动调度
    MANUAL_SCHEDULER(2,"manual scheduler"),
    // 恢复
    RECOVER(3, "recover task"),
    // 重复
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
