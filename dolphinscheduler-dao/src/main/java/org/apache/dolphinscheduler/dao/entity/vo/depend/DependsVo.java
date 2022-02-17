package org.apache.dolphinscheduler.dao.entity.vo.depend;

import org.apache.dolphinscheduler.common.enums.ExecutionStatus;

/**
 * @program: dolphinscheduler
 * @description:
 * @author: Mr.Yang
 * @create: 2022-01-04 15:48
 **/

public class DependsVo {
    private Integer processId;
    private Integer definitionId;
    private String name;
    private ExecutionStatus state;
    private String dependType;

    public DependsVo(Integer processId, Integer definitionId,String name,ExecutionStatus state,String dependType) {
        this.name = name;
        this.processId = processId;
        this.definitionId = definitionId;
        this.state = state;
        this.dependType = dependType;
    }

    public String getDependType() {
        return dependType;
    }

    public void setDependType(String dependType) {
        this.dependType = dependType;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getProcessId() {
        return processId;
    }

    public void setProcessId(Integer processId) {
        this.processId = processId;
    }

    public Integer getDefinitionId() {
        return definitionId;
    }

    public void setDefinitionId(Integer definitionId) {
        this.definitionId = definitionId;
    }

    public ExecutionStatus getState() {
        return state;
    }

    public void setState(ExecutionStatus state) {
        this.state = state;
    }

    @Override
    public String toString() {
        return "DependsVo{" +
                "processId=" + processId +
                ", definitionId=" + definitionId +
                ", name='" + name + '\'' +
                ", state=" + state +
                ", dependType='" + dependType + '\'' +
                '}';
    }
}
