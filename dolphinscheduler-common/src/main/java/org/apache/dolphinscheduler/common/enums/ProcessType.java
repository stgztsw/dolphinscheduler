package org.apache.dolphinscheduler.common.enums;

import com.baomidou.mybatisplus.annotation.EnumValue;

public enum ProcessType {

    NORMAL(0, "其他process"),
    SCHEDULER(1, "调度process");

    ProcessType(int code, String descp) {
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

    public static ProcessType of(Integer type){
        for(ProcessType processType : values()){
            if(processType.getCode() == type){
                return processType;
            }
        }
        throw new IllegalArgumentException("invalid type : " + type);
    }
}
