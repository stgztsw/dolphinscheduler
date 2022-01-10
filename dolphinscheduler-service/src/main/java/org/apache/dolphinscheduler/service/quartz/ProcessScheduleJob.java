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
package org.apache.dolphinscheduler.service.quartz;


import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.enums.CommandType;
import org.apache.dolphinscheduler.common.enums.DependentSchedulerType;
import org.apache.dolphinscheduler.common.enums.ProcessType;
import org.apache.dolphinscheduler.common.enums.ReleaseState;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.entity.*;
import org.apache.dolphinscheduler.service.bean.SpringApplicationContext;
import org.apache.dolphinscheduler.service.process.ProcessService;
import org.apache.dolphinscheduler.service.quartz.cron.SchedulingBatch;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.dolphinscheduler.common.Constants.CMDPARAM_INFORMAL_SCHEDULER;
import static org.apache.dolphinscheduler.common.Constants.CMDPARAM_RECOVER_PROCESS_ID_STRING;

/**
 * process schedule job
 */
public class ProcessScheduleJob implements Job {

    /**
     * logger of ProcessScheduleJob
     */
    private static final Logger logger = LoggerFactory.getLogger(ProcessScheduleJob.class);

    public ProcessService getProcessService(){
        return SpringApplicationContext.getBean(ProcessService.class);
    }

    /**
     * Called by the Scheduler when a Trigger fires that is associated with the Job
     *
     * @param context JobExecutionContext
     * @throws JobExecutionException if there is an exception while executing the job.
     */
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {

        Assert.notNull(getProcessService(), "please call init() method first");

        JobDataMap dataMap = context.getJobDetail().getJobDataMap();

        int projectId = dataMap.getInt(Constants.PROJECT_ID);
        int scheduleId = dataMap.getInt(Constants.SCHEDULE_ID);


        Date scheduledFireTime = context.getScheduledFireTime();


        Date fireTime = context.getFireTime();

        logger.info("scheduled fire time :{}, fire time :{}, process id :{}", scheduledFireTime, fireTime, scheduleId);

        // query schedule
        Schedule schedule = getProcessService().querySchedule(scheduleId);
        if (schedule == null) {
            logger.warn("process schedule does not exist in db，delete schedule job in quartz, projectId:{}, scheduleId:{}", projectId, scheduleId);
            deleteJob(projectId, scheduleId);
            return;
        }

        ProcessDefinition processDefinition = getProcessService().findProcessDefineById(schedule.getProcessDefinitionId());
        // release state : online/offline
        ReleaseState releaseState = processDefinition.getReleaseState();
        if (processDefinition == null || releaseState == ReleaseState.OFFLINE) {
            logger.warn("process definition does not exist in db or offline，need not to create command, projectId:{}, processId:{}", projectId, scheduleId);
            return;
        }
        synchronized (ProcessScheduleJob.class) {
            Command command = new Command();
            SchedulingBatch sb = getProcessService().getSchedulingBatch(schedule, scheduledFireTime, processDefinition.getId());
            if (processDefinition.getProcessType() == ProcessType.NORMAL) {
                List<ProcessDependent> pd = getProcessService().findProcessDependentsByProcessId(processDefinition.getId());
                if (!pd.isEmpty()) {
                    List<ProcessInstance> parent = getProcessService().findProcessInstances(pd.stream().mapToInt(ProcessDependent::getDependentId).toArray(), sb);
                    if (parent.isEmpty()) {
                        getProcessService().generateInformalFakeProcessInstance(processDefinition, sb);
                        return;
                    }
                    ProcessInstance processInstance = getProcessService().generateInformalProcessInstance(processDefinition, sb, parent.get(0));
                    Map<String, String> cmdParam = new HashMap<>();
                    cmdParam.put(CMDPARAM_RECOVER_PROCESS_ID_STRING, String.valueOf(processInstance.getId()));
                    cmdParam.put(CMDPARAM_INFORMAL_SCHEDULER, CMDPARAM_INFORMAL_SCHEDULER);
                    command.setCommandParam(JSONUtils.toJson(cmdParam));
                }
            }
            command.setCommandType(CommandType.SCHEDULER);
            command.setExecutorId(schedule.getUserId());
            command.setFailureStrategy(schedule.getFailureStrategy());
            command.setProcessDefinitionId(schedule.getProcessDefinitionId());
            command.setScheduleTime(scheduledFireTime);
            command.setStartTime(fireTime);
            command.setWarningGroupId(schedule.getWarningGroupId());
            String workerGroup = StringUtils.isEmpty(schedule.getWorkerGroup()) ? Constants.DEFAULT_WORKER_GROUP : schedule.getWorkerGroup();
            command.setWorkerGroup(workerGroup);
            command.setWarningType(schedule.getWarningType());
            command.setProcessInstancePriority(schedule.getProcessInstancePriority());
            command.setSchedulerInterval(sb.getSchedulerInterval());
            command.setSchedulerBatchNo(sb.getNextBatchNo());
            command.setDependentSchedulerType(DependentSchedulerType.SCHEDULER);
            if (processDefinition.getProcessType() == ProcessType.SCHEDULER) {
                command.setDependentSchedulerFlag(true);
            }
            getProcessService().createCommand(command);
        }
    }


    /**
     * delete job
     */
    private void deleteJob(int projectId, int scheduleId) {
        String jobName = QuartzExecutors.buildJobName(scheduleId);
        String jobGroupName = QuartzExecutors.buildJobGroupName(projectId);
        QuartzExecutors.getInstance().deleteJob(jobName, jobGroupName);
    }
}
