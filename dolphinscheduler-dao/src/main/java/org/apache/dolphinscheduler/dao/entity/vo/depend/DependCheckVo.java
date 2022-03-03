package org.apache.dolphinscheduler.dao.entity.vo.depend;

import org.apache.dolphinscheduler.common.enums.ExecutionStatus;

/**
 * @program: dolphinscheduler
 * @description:
 * @author: Mr.Yang
 * @create: 2022-03-02 17:57
 **/

public class DependCheckVo {
    private String projectName;
    private Integer processId;
    private Integer definitionId;
    private String name;
    private ExecutionStatus state;
    private String dependType;

    public DependCheckVo(DependsVo dependsVo,String projectName) {
        this.name = dependsVo.getName();
        this.processId = dependsVo.getProcessId();
        this.definitionId = dependsVo.getDefinitionId();
        this.state = dependsVo.getState();
        this.dependType = dependsVo.getDependType();
        this.projectName = projectName;
    }

    public DependCheckVo() {
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

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    @Override
    public String toString() {
        return "DependCheckVo{" +
                "projectName='" + projectName + '\'' +
                ", processId=" + processId +
                ", definitionId=" + definitionId +
                ", name='" + name + '\'' +
                ", state=" + state +
                ", dependType='" + dependType + '\'' +
                '}';
    }
}
