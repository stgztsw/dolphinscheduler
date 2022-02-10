package org.apache.dolphinscheduler.common.enums;

import com.baomidou.mybatisplus.annotation.EnumValue;

public enum DependentViewRelation {

    ONE_ASCEND(0,"one_ascend"),
    ONE_DESCEND(1,"one_descend"),
    ONE_ALL(2,"one_all");

    DependentViewRelation(int code, String descp) {
        this.code = code;
        this.descp = descp;
    }

//    @EnumValue
    private final int code;
    private final String descp;

    public int getCode() {
        return code;
    }

    public String getDescp() {
        return descp;
    }


    public static DependentViewRelation of(int type){
        for(DependentViewRelation dvr : values()){
            if(dvr.getCode() == type){
                return dvr;
            }
        }
        return null;
    }
}
