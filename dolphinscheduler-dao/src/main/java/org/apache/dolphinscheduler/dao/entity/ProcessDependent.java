package org.apache.dolphinscheduler.dao.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

import java.util.Date;
import java.util.Objects;

/**
 * process dependent
 */
@TableName("t_ds_process_dependent")
public class ProcessDependent {

    /**
     * id
     */
    @TableId(value="id", type= IdType.AUTO)
    private int id;

    /**
     * dependent id
     */
    private int dependentId;

    /**
     * process id
     */
    private int processId;

    /**
     * create time
     */
    private Date createTime;

    /**
     * update time
     */
    private Date updateTime;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getDependentId() {
        return dependentId;
    }

    public void setDependentId(int dependentId) {
        this.dependentId = dependentId;
    }

    public int getProcessId() {
        return processId;
    }

    public void setProcessId(int processId) {
        this.processId = processId;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProcessDependent that = (ProcessDependent) o;
        return dependentId == that.dependentId && processId == that.processId;
    }

    @Override
    public String toString() {
        return "ProcessDependent{" +
                "id=" + id +
                ", dependentId=" + dependentId +
                ", processId=" + processId +
                ", createTime=" + createTime +
                ", updateTime=" + updateTime +
                '}';
    }
}
