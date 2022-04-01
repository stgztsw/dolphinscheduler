package org.apache.dolphinscheduler.dao.entity.vo.depend;

import org.apache.dolphinscheduler.common.enums.DependentViewRelation;
import org.apache.dolphinscheduler.common.enums.ExecutionStatus;

import java.util.List;

/**
 * @program: dolphinscheduler
 * @description:
 * @author: Mr.Yang
 * @create: 2021-12-30 13:48
 **/
public class DependTreeViewVo {

    private Integer processId;

    private Integer definitionId;

    private Integer treeType;

    private String name;

    private ExecutionStatus state;

    /**
     * 父parentIds：name,processId,instanceId,state
     */
    private List<DependsVo> parents;

    /**
     * 子childIds：name,processId,instanceId,state
     */
    private List<DependsVo> childs;

    @Override
    public String toString() {
        return "DependTreeViewVo{" +
                "processId=" + processId +
                ", definitionId=" + definitionId +
                ", treeType=" + treeType +
                ", name='" + name + '\'' +
                ", state=" + state +
                ", parents size=" + (parents!=null? parents.size() : 0) +
                ", childs size=" + (childs!=null ? childs.size() : 0) +
                ", relation=" + relation +
                '}';
    }

    public List<DependsVo> getParents() {
        return parents;
    }

    public List<DependsVo> getChilds() {
        return childs;
    }

    public DependentViewRelation getRelation() {
        return relation;
    }

    public void setRelation(DependentViewRelation relation) {
        this.relation = relation;
    }

    private DependentViewRelation relation;

    public DependTreeViewVo(Integer processId, Integer definitionId, Integer treeType, String name, ExecutionStatus state,
                            List<DependsVo> parents, List<DependsVo> childs, DependentViewRelation relation) {
        this.processId = processId;
        this.definitionId = definitionId;
        this.treeType = treeType;
        this.name = name;
        this.state = state;
        this.parents = parents;
        this.childs = childs;
        this.relation = relation;
    }

    public DependTreeViewVo(Integer processId, Integer definitionId, Integer treeType, String name, ExecutionStatus state, DependentViewRelation relation) {
        this.processId = processId;
        this.definitionId = definitionId;
        this.treeType = treeType;
        this.name = name;
        this.state = state;
        this.relation = relation;
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

    public void setParents(List<DependsVo> parents) {
        this.parents = parents;
    }

    public void setChilds(List<DependsVo> childs) {
        this.childs = childs;
    }

    public Integer getTreeType() {
        return treeType;
    }

    public void setTreeType(Integer treeType) {
        this.treeType = treeType;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ExecutionStatus getState() {
        return state;
    }

    public void setState(ExecutionStatus state) {
        this.state = state;
    }


}
