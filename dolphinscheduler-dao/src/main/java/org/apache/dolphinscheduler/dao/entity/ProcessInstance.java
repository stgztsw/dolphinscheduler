/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dolphinscheduler.dao.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.core.toolkit.StringUtils;
import org.apache.dolphinscheduler.common.enums.*;
import org.apache.dolphinscheduler.common.model.DateInterval;
import org.apache.dolphinscheduler.common.utils.DependentUtils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Objects;

/**
 * process instance
 */
@TableName("t_ds_process_instance")
public class ProcessInstance {

    /**
     * id
     */
    @TableId(value="id", type=IdType.AUTO)
    private int id;
    /**
     * process definition id
     */
    private int processDefinitionId;
    /**
     * process state
     */
    private ExecutionStatus state;
    /**
     * recovery flag for failover
     */
    private Flag recovery;
    /**
     * start time
     */
    private Date startTime;

    /**
     * end time
     */
    private Date endTime;

    /**
     * run time
     */
    private int runTimes;

    /**
     * name
     */
    private String name;

    /**
     * host
     */
    private String host;

    /**
     * process definition structure
     */
    @TableField(exist = false)
    private ProcessDefinition processDefinition;
    /**
     * process command type
     */
    private CommandType commandType;

    /**
     * command parameters
     */
    private String commandParam;

    /**
     * node depend type
     */
    private TaskDependType taskDependType;

    /**
     * task max try times
     */
    private int maxTryTimes;

    /**
     * failure strategy when task failed.
     */
    private FailureStrategy failureStrategy;

    /**
     *  warning type
     */
    private WarningType warningType;

    /**
     * warning group
     */
    private Integer warningGroupId;

    /**
     * schedule time
     */
    private Date scheduleTime;

    /**
     * command start time
     */
    private Date commandStartTime;

    /**
     * user define parameters string
     */
    private String globalParams;

    /**
     * process instance json
     */
    private String processInstanceJson;

    /**
     * executor id
     */
    private int executorId;

    /**
     * executor name
     */
    @TableField(exist = false)
    private String executorName;

    /**
     * tenant code
     */
    @TableField(exist = false)
    private String tenantCode;

    /**
     * queue
     */
    @TableField(exist = false)
    private String queue;

    /**
     * process is sub process
     */
    private Flag isSubProcess;

    /**
     * task locations for web
     */
    private String locations;

    /**
     * task connects for web
     */
    private String connects;

    /**
     * history command
     */
    private String historyCmd;

    /**
     * depend processes schedule time
     */
    private String dependenceScheduleTimes;

    /**
     * process duration
     * @return
     */
    @TableField(exist = false)
    private String duration;// update desc string->long

    /**
     * process instance priority
     */
    private Priority processInstancePriority;

    /**
     * worker group
     */
    private String workerGroup;

    /**
     * process timeout for warning
     */
    private int timeout;

    /**
     * tenant id
     */
    private int tenantId;

    /**
     * receivers for api
     */
    @TableField(exist = false)
    private String receivers;

    /**
     * receivers cc for api
     */
    @TableField(exist = false)
    private String receiversCc;

    /**
     * scheduler interval
     * 0 minute; 1 hour; 2 day; 3 week; 4 month; 5 year; 9 default;
     */
    @TableField("scheduler_interval")
    private int schedulerInterval;

    /**
     * scheduler batch no
     */
    @TableField("scheduler_batch_no")
    private int schedulerBatchNo;

    @TableField("process_type")
    private ProcessType processType;

    @TableField("dependent_scheduler_type")
    private DependentSchedulerType dependentSchedulerType;

    @TableField("dependent_scheduler_flag")
    private boolean dependentSchedulerFlag;

    /**
     * 周期实例
     */
    @TableField(exist = false)
    private String interval;

    @TableField("scheduler_start_id")
    private int schedulerStartId;

    @TableField("scheduler_rerun_no")
    private String schedulerRerunNo;

    @TableField("rerun_scheduler_flag")
    private boolean rerunSchedulerFlag;

    /**
     * 显示上下游依赖的标记
     */
//    @TableField(exist = false)
//    private Integer viewDependentFlag;

    public ProcessInstance(){

    }

    /**
     * set the process name with process define version and timestamp
     * @param processDefinition processDefinition
     */
    public ProcessInstance(ProcessDefinition processDefinition){
        this.processDefinition = processDefinition;
        this.name = processDefinition.getName() + "-" +
                processDefinition.getVersion() + "-" +
                System.currentTimeMillis();
    }

    public ProcessDefinition getProcessDefinition() {
        return processDefinition;
    }

    public void setProcessDefinition(ProcessDefinition processDefinition) {
        this.processDefinition = processDefinition;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getProcessDefinitionId() {
        return processDefinitionId;
    }

    public void setProcessDefinitionId(int processDefinitionId) {
        this.processDefinitionId = processDefinitionId;
    }

    public ExecutionStatus getState() {
        return state;
    }

    public void setState(ExecutionStatus state) {
        this.state = state;
    }

    public Flag getRecovery() {
        return recovery;
    }

    public void setRecovery(Flag recovery) {
        this.recovery = recovery;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    public int getRunTimes() {
        return runTimes;
    }

    public void setRunTimes(int runTimes) {
        this.runTimes = runTimes;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }


    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }


    public CommandType getCommandType() {
        return commandType;
    }

    public void setCommandType(CommandType commandType) {
        this.commandType = commandType;
    }

    public String getCommandParam() {
        return commandParam;
    }

    public void setCommandParam(String commandParam) {
        this.commandParam = commandParam;
    }

    public TaskDependType getTaskDependType() {
        return taskDependType;
    }

    public void setTaskDependType(TaskDependType taskDependType) {
        this.taskDependType = taskDependType;
    }


    public int getMaxTryTimes() {
        return maxTryTimes;
    }

    public void setMaxTryTimes(int maxTryTimes) {
        this.maxTryTimes = maxTryTimes;
    }

    public FailureStrategy getFailureStrategy() {
        return failureStrategy;
    }

    public void setFailureStrategy(FailureStrategy failureStrategy) {
        this.failureStrategy = failureStrategy;
    }


    public boolean isProcessInstanceStop(){
        return this.state.typeIsFinished();
    }

    public WarningType getWarningType() {
        return warningType;
    }

    public void setWarningType(WarningType warningType) {
        this.warningType = warningType;
    }

    public Integer getWarningGroupId() {
        return warningGroupId;
    }

    public void setWarningGroupId(Integer warningGroupId) {
        this.warningGroupId = warningGroupId;
    }

    public Date getScheduleTime() {
        return scheduleTime;
    }

    public void setScheduleTime(Date scheduleTime) {
        this.scheduleTime = scheduleTime;
        setInterval();// update
    }

    public Date getCommandStartTime() {
        return commandStartTime;
    }

    public void setCommandStartTime(Date commandStartTime) {
        this.commandStartTime = commandStartTime;
    }

    public String getGlobalParams() {
        return globalParams;
    }

    public void setGlobalParams(String globalParams) {
        this.globalParams = globalParams;
    }

    public String getProcessInstanceJson() {
        return processInstanceJson;
    }

    public void setProcessInstanceJson(String processInstanceJson) {
        this.processInstanceJson = processInstanceJson;
    }

    public String getTenantCode() {
        return tenantCode;
    }

    public void setTenantCode(String tenantCode) {
        this.tenantCode = tenantCode;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public int getExecutorId() {
        return executorId;
    }

    public void setExecutorId(int executorId) {
        this.executorId = executorId;
    }


    public Flag getIsSubProcess() {
        return isSubProcess;
    }

    public void setIsSubProcess(Flag isSubProcess) {
        this.isSubProcess = isSubProcess;
    }

    public Priority getProcessInstancePriority() {
        return processInstancePriority;
    }

    public void setProcessInstancePriority(Priority processInstancePriority) {
        this.processInstancePriority = processInstancePriority;
    }
    public String getLocations() {
        return locations;
    }

    public void setLocations(String locations) {
        this.locations = locations;
    }

    public String getConnects() {
        return connects;
    }

    public void setConnects(String connects) {
        this.connects = connects;
    }

    public String getHistoryCmd() {
        return historyCmd;
    }

    public String getExecutorName() {
        return executorName;
    }

    public void setExecutorName(String executorName) {
        this.executorName = executorName;
    }

    public void setHistoryCmd(String historyCmd) {
        this.historyCmd = historyCmd;
    }

    public int getSchedulerInterval() {
        return schedulerInterval;
    }

    public void setSchedulerInterval(int schedulerInterval) {
        this.schedulerInterval = schedulerInterval;
        setInterval();
    }

    public int getSchedulerBatchNo() {
        return schedulerBatchNo;
    }

    public void setSchedulerBatchNo(int schedulerBatchNo) {
        this.schedulerBatchNo = schedulerBatchNo;
    }

    /**
     * add command to history
     * @param cmd cmd
     */
    public void addHistoryCmd(CommandType cmd){
        if(StringUtils.isNotEmpty(this.historyCmd)){
            this.historyCmd = String.format("%s,%s", this.historyCmd, cmd.toString());
        }else{
            this.historyCmd = cmd.toString();
        }
    }

    /**
     * check this process is start complement data
     * @return whether complement data
     */
    public boolean isComplementData(){
        if(StringUtils.isEmpty(this.historyCmd)){
            return false;
        }
        return historyCmd.startsWith(CommandType.COMPLEMENT_DATA.toString());
    }

    /**
     * get current command type,
     * if start with complement data,return complement
     * @return CommandType
     */
    public CommandType getCmdTypeIfComplement(){
        if(isComplementData()){
            return CommandType.COMPLEMENT_DATA;
        }
        return commandType;
    }

    public String getDependenceScheduleTimes() {
        return dependenceScheduleTimes;
    }

    public void setDependenceScheduleTimes(String dependenceScheduleTimes) {
        this.dependenceScheduleTimes = dependenceScheduleTimes;
    }

    public String getDuration() {
        return duration;
    }

    public void setDuration(String duration) {
        this.duration = duration;
    }

    public String getWorkerGroup() {
        return workerGroup;
    }

    public void setWorkerGroup(String workerGroup) {
        this.workerGroup = workerGroup;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }


    public void setTenantId(int tenantId) {
        this.tenantId = tenantId;
    }

    public int getTenantId() {
        return this.tenantId ;
    }

    public String getReceivers() {
        return receivers;
    }

    public void setReceivers(String receivers) {
        this.receivers = receivers;
    }

    public String getReceiversCc() {
        return receiversCc;
    }

    public void setReceiversCc(String receiversCc) {
        this.receiversCc = receiversCc;
    }

    public ProcessType getProcessType() {
        return processType;
    }

    public void setProcessType(ProcessType processType) {
        this.processType = processType;
    }

    public boolean isDependentSchedulerFlag() {
        return dependentSchedulerFlag;
    }

    public void setDependentSchedulerFlag(boolean dependentSchedulerFlag) {
        this.dependentSchedulerFlag = dependentSchedulerFlag;
        setInterval();
    }

    public DependentSchedulerType getDependentSchedulerType() {
        return dependentSchedulerType;
    }

    public void setDependentSchedulerType(DependentSchedulerType dependentSchedulerType) {
        this.dependentSchedulerType = dependentSchedulerType;
    }

    public String getInterval() {
        return interval;
    }

    public void setInterval() {
        if (interval != null || !dependentSchedulerFlag || scheduleTime == null || schedulerInterval == 0) {
            return;
        }
        List<DateInterval> dateIntervals = DependentUtils.getDateIntervalListForDependent(scheduleTime, schedulerInterval);
        SimpleDateFormat sdf = null;
        if (schedulerInterval == 1) {
            sdf = new SimpleDateFormat("yyyyMMddHH");
        }  else if (schedulerInterval == 4) {
            sdf = new SimpleDateFormat("yyyyMM");
        } else {
            sdf = new SimpleDateFormat("yyyyMMdd");
        }
        this.interval = sdf.format(dateIntervals.get(0).getStartTime());
    }

    public int getSchedulerStartId() {
        return schedulerStartId;
    }

    public void setSchedulerStartId(int schedulerStartId) {
        this.schedulerStartId = schedulerStartId;
    }

    public String getSchedulerRerunNo() {
        return schedulerRerunNo;
    }

    public void setSchedulerRerunNo(String schedulerRerunNo) {
        this.schedulerRerunNo = schedulerRerunNo;
    }

    public boolean isRerunSchedulerFlag() {
        return rerunSchedulerFlag;
    }

    public void setRerunSchedulerFlag(boolean rerunSchedulerFlag) {
        this.rerunSchedulerFlag = rerunSchedulerFlag;
    }

    public Integer getViewDependentFlag() {
        return viewDependentFlag;
    }

    public void setViewDependentFlag(Integer viewDependentFlag) {
        this.viewDependentFlag = viewDependentFlag;
    }

    @Override
    public String toString() {
        return "ProcessInstance{" +
                "id=" + id +
                ", processDefinitionId=" + processDefinitionId +
                ", state=" + state +
                ", recovery=" + recovery +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", runTimes=" + runTimes +
                ", name='" + name + '\'' +
                ", host='" + host + '\'' +
                ", processDefinition=" + processDefinition +
                ", commandType=" + commandType +
                ", commandParam='" + commandParam + '\'' +
                ", taskDependType=" + taskDependType +
                ", maxTryTimes=" + maxTryTimes +
                ", failureStrategy=" + failureStrategy +
                ", warningType=" + warningType +
                ", warningGroupId=" + warningGroupId +
                ", scheduleTime=" + scheduleTime +
                ", commandStartTime=" + commandStartTime +
                ", globalParams='" + globalParams + '\'' +
                ", processInstanceJson='" + processInstanceJson + '\'' +
                ", executorId=" + executorId +
                ", tenantCode='" + tenantCode + '\'' +
                ", queue='" + queue + '\'' +
                ", isSubProcess=" + isSubProcess +
                ", locations='" + locations + '\'' +
                ", connects='" + connects + '\'' +
                ", historyCmd='" + historyCmd + '\'' +
                ", dependenceScheduleTimes='" + dependenceScheduleTimes + '\'' +
                ", duration=" + duration +
                ", processInstancePriority=" + processInstancePriority +
                ", workerGroup='" + workerGroup + '\'' +
                ", timeout=" + timeout +
                ", tenantId=" + tenantId +
                ", receivers='" + receivers + '\'' +
                ", receiversCc='" + receiversCc + '\'' +
                ", schedulerInterval='" + schedulerInterval + '\'' +
                ", schedulerBatchNo='" + schedulerBatchNo + '\'' +
                ", processType='" + processType + '\'' +
                ", dependentSchedulerType='" + dependentSchedulerType + '\'' +
                ", interval='" + interval + '\'' +
                ", schedulerStartId='" + schedulerStartId + '\'' +
                ", schedulerRerunNo='" + schedulerRerunNo + '\'' +
                ", rerunSchedulerFlag='" + rerunSchedulerFlag + '\'' +
                ", dependentSchedulerFlag='" + dependentSchedulerFlag + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ProcessInstance that = (ProcessInstance) o;

        return id == that.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
