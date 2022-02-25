package org.apache.dolphinscheduler.dao.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import org.apache.dolphinscheduler.common.enums.FlgLock;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * @program: dolphinscheduler
 * @description:
 * @author: Mr.Yang
 * @create: 2022-01-25 10:32
 **/
@Component
@TableName("t_ds_depend_check_param")
public class DependCheckParam {

    @TableId(value="id", type= IdType.AUTO)
    private int id;

    /**
     * 是否允许添加job
     */
    @TableField("is_add_job")
    private FlgLock isAddJob;

    /**
     * 是否允许执行quartz execute逻辑
     */
    @TableField("is_execute_job")
    private FlgLock isExecuteJob;

    /**
     * 控制开关 0：开 1：关
      */
    @TableField("control_switch")
    private FlgLock controlSwitch;

    @TableField("datetime")
    private Date dateTime;

    public DependCheckParam(FlgLock isAddJob, FlgLock isExecuteJob, FlgLock controlSwitch) {
        this.isAddJob = isAddJob;
        this.isExecuteJob = isExecuteJob;
        this.controlSwitch = controlSwitch;
    }

    public DependCheckParam() {
    }

    public Date getDateTime() {
        return dateTime;
    }

    public void setDateTime() {

        Date date = new Date();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.DAY_OF_YEAR,-1);
        this.dateTime = calendar.getTime();
    }

    public void setDateTime(Date dateTime){
        this.dateTime = dateTime;
    }

    public FlgLock getIsAddJob() {
        return isAddJob;
    }

    public void setIsAddJob(FlgLock addJob) {
        isAddJob = addJob;
    }

    public FlgLock getIsExecuteJob() {
        return isExecuteJob;
    }

    public void setIsExecuteJob(FlgLock executeJob) {
        isExecuteJob = executeJob;
    }

    public FlgLock getControlSwitch() {
        return controlSwitch;
    }

    public void setControlSwitch(FlgLock controlSwitch) {
        this.controlSwitch = controlSwitch;
    }
}
