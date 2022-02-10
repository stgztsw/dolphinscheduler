package org.apache.dolphinscheduler.common.enums;

/**
 * @program: dolphinscheduler
 * @description:
 * @author: Mr.Yang
 * @create: 2021-12-31 11:08
 **/

public enum DependViewType {
    PROCESS_INSTANCE(0,"processInstance"),
    PROCESS_DEFINITION(1,"processDefinition");

    DependViewType(int code, String descp) {
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
}
