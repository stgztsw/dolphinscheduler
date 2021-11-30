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
package org.apache.dolphinscheduler.server.master.runner;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.enums.CommandType;
import org.apache.dolphinscheduler.common.enums.ExecutionStatus;
import org.apache.dolphinscheduler.common.enums.UserType;
import org.apache.dolphinscheduler.common.model.DateInterval;
import org.apache.dolphinscheduler.common.thread.Stopper;
import org.apache.dolphinscheduler.common.thread.ThreadUtils;
import org.apache.dolphinscheduler.common.utils.DependentUtils;
import org.apache.dolphinscheduler.common.utils.NetUtils;
import org.apache.dolphinscheduler.common.utils.OSUtils;
import org.apache.dolphinscheduler.dao.entity.*;
import org.apache.dolphinscheduler.remote.NettyRemotingClient;
import org.apache.dolphinscheduler.remote.config.NettyClientConfig;
import org.apache.dolphinscheduler.server.master.config.MasterConfig;
import org.apache.dolphinscheduler.server.master.zk.ZKMasterClient;
import org.apache.dolphinscheduler.service.process.ProcessService;

import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;

import java.util.*;
import java.util.concurrent.*;

import javax.annotation.PostConstruct;

import org.apache.poi.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import static org.apache.dolphinscheduler.common.Constants.CMDPARAM_RECOVER_PROCESS_ID_STRING;

/**
 *  master scheduler thread
 */
@Service
public class MasterSchedulerService extends Thread {

    /**
     * logger of MasterSchedulerThread
     */
    private static final Logger logger = LoggerFactory.getLogger(MasterSchedulerService.class);

    /**
     * dolphinscheduler database interface
     */
    @Autowired
    private ProcessService processService;

    /**
     * zookeeper master client
     */
    @Autowired
    private ZKMasterClient zkMasterClient;

    /**
     * master config
     */
    @Autowired
    private MasterConfig masterConfig;

    /**
     *  netty remoting client
     */
    private NettyRemotingClient nettyRemotingClient;

    /**
     * master exec service
     */
    private ThreadPoolExecutor masterExecService;

    /**
     * dependent process queue
     */
    private final ConcurrentLinkedQueue<Future<ProcessInstance>> dependentProcessQueue = new ConcurrentLinkedQueue();


    /**
     * constructor of MasterSchedulerThread
     */
    @PostConstruct
    public void init(){
        this.masterExecService = (ThreadPoolExecutor)ThreadUtils.newDaemonFixedThreadExecutor("Master-Exec-Thread", masterConfig.getMasterExecThreads());
        NettyClientConfig clientConfig = new NettyClientConfig();
        this.nettyRemotingClient = new NettyRemotingClient(clientConfig);
        DependentScheduler s = new DependentScheduler("master dependent scheduler");
        s.start();
    }

    @Override
    public void start(){
        super.setName("MasterSchedulerThread");
        super.start();
    }

    public void close() {
        masterExecService.shutdown();
        boolean terminated = false;
        try {
            terminated = masterExecService.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException ignore) {}
        if(!terminated){
            logger.warn("masterExecService shutdown without terminated, increase await time");
        }
        nettyRemotingClient.close();
        logger.info("master schedule service stopped...");
    }

    /**
     * run of MasterSchedulerThread
     */
    @Override
    public void run() {
        logger.info("master scheduler started");
        while (Stopper.isRunning()){
            try {
                boolean runCheckFlag = OSUtils.checkResource(masterConfig.getMasterMaxCpuloadAvg(), masterConfig.getMasterReservedMemory());
                if(!runCheckFlag) {
                    Thread.sleep(Constants.SLEEP_TIME_MILLIS);
                    continue;
                }
                if (zkMasterClient.getZkClient().getState() == CuratorFrameworkState.STARTED) {
                    scheduleProcess();
                }
            } catch (Exception e) {
                logger.error("master scheduler thread error", e);
            }
        }
    }

    private void scheduleProcess() throws Exception {
        InterProcessMutex mutex = null;
        try {
                    mutex = zkMasterClient.blockAcquireMutex();

                    int activeCount = masterExecService.getActiveCount();
                    // make sure to scan and delete command  table in one transaction
                    Command command = processService.findOneCommand();
                    if (command != null) {
                        logger.info("find one command: id: {}, type: {}", command.getId(),command.getCommandType());

                        try{
                            ProcessInstance processInstance = processService.handleCommand(logger,
                                    getLocalAddress(),
                                    this.masterConfig.getMasterExecThreads() - activeCount, command);
                            if (processInstance != null) {
                                logger.info("start master exec thread , split DAG ...");
                                Future<ProcessInstance> future;
                                if (processInstance.getState() == ExecutionStatus.RUNNING_EXECUTION) {
                                    future = masterExecService.submit(
                                            new MasterExecThread(
                                                    processInstance
                                                    , processService
                                                    , nettyRemotingClient
                                            ));
                                } else {
                                    logger.debug("process={} is not in running status, add it to dependentProcessQueue", processInstance.getId());
                                    future = CompletableFuture.completedFuture(processInstance);
                                }
                                dependentProcessQueue.offer(future);
                            }
                        }catch (Exception e){
                            logger.error("scan command error ", e);
                            processService.moveToErrorCommand(command, e.toString());
                        }
                    } else{
                        //indicate that no command ,sleep for 1s
                        Thread.sleep(Constants.SLEEP_TIME_MILLIS);
                    }
            } finally{
                zkMasterClient.releaseMutex(mutex);
            }
        }

    private String getLocalAddress() {
        return NetUtils.getAddr(masterConfig.getListenPort());
    }


    class DependentScheduler extends Thread{

        private final int pageSize = 10;

        public DependentScheduler(String name) {
            super(name);
        }

        @Override
        public void run() {
            logger.info("master dependent scheduler started");
            while (true) {
                try {
                    if (dependentProcessQueue.isEmpty()) {
                        logger.debug("dependentProcessQueue is empty, no dependent process need to be fired");
                        Thread.sleep(Constants.SLEEP_TIME_MILLIS_10S);
                        continue;
                    }
                    Iterator<Future<ProcessInstance>> futureIterator = dependentProcessQueue.iterator();
                    while (futureIterator.hasNext()) {
                        Future<ProcessInstance> future = futureIterator.next();
                        if (!future.isDone()) {
                            continue;
                        }
                        ProcessInstance parentProcessInstance = future.get();
                        if (!parentProcessInstance.getState().typeIsFinished()) {
                            continue;
                        }
                        if (!parentProcessInstance.getState().typeIsSuccess()) {
                            logger.debug("parentProcessInstance={} is not success, not need to fire the dependent process",
                                    parentProcessInstance.getProcessDefinitionId());
                            futureIterator.remove();
                            continue;
                        }
                        if (!parentProcessInstance.isDependentSchedulerFlag()) {
                            logger.debug("parentProcessInstance={} DependentSchedulerFlag is false, " +
                                            "not need to fire the dependent process", parentProcessInstance.getProcessDefinitionId());
                            futureIterator.remove();
                            continue;
                        }
                        int pageNo = 1;
                        IPage<ProcessDependent> iPage;
                        Page<ProcessDependent> page;
                        do {
                            iPage = new Page<>(pageNo++,pageSize);
                            page = processService.queryByDependentIdListPaging(iPage,parentProcessInstance.getProcessDefinitionId());
                            List<ProcessDependent> processDependents = page.getRecords();
                            if (page.getTotal() == 0) {
                                logger.debug("process={} has no dependent process", parentProcessInstance.getProcessDefinitionId());
                                break;
                            }
                            schedulerProcess(parentProcessInstance, processDependents);
                        } while (page.hasNext());
                        futureIterator.remove();
                    }
                } catch (Throwable e) {
                    logger.error("master scheduler thread error", e);
                }
            }
        }

        private void schedulerProcess(ProcessInstance parentProcessInstance, List<ProcessDependent> processDependents) {
            for (ProcessDependent processDependent : processDependents) {
                ProcessDefinition processDefinition = processService
                        .findProcessDefineById(processDependent.getProcessId());
                Date schedulerTime = parentProcessInstance.getScheduleTime();
                int schedulerInterval = parentProcessInstance.getSchedulerInterval();
                int batchNo = parentProcessInstance.getSchedulerBatchNo();
                List<DateInterval> dateIntervals = DependentUtils
                        .getDateIntervalListForDependent(schedulerTime, schedulerInterval);
                if (processService.dependentProcessIsFired(processDefinition.getId(), dateIntervals, batchNo)) {
                    logger.debug("ProcessDependent which dependentId={} processId={} has been fired in command queue",
                            processDependent.getDependentId(), processDependent.getProcessId());
                    continue;
                }
                //查询对应的时间周期内的批次，并且dependent_scheduler_flag为true的数据
                ProcessInstance processInstance= processService
                        .findProcessInstanceByProcessIdInInterval(processDefinition.getId(), dateIntervals, batchNo, null, null);
                Command command;
                if (processInstance != null) {
                    if (!processInstance.getState().typeIsFinished() && ExecutionStatus.INITED != processInstance.getState()) {
                        logger.debug("ProcessDependent which dependentId={} processId={} has exist processInstance={} in running now",
                                processDependent.getDependentId(), processDependent.getProcessId(), processInstance.getId());
                        continue;
                    }
                    if (ExecutionStatus.SUCCESS == processInstance.getState()) {
                        logger.debug("ProcessDependent which dependentId={} processId={} has exist processInstance={}, and it's state is success, " +
                                "no need to submit to executor, and add it to dependentProcessQueue",
                                processDependent.getDependentId(), processDependent.getProcessId(), processInstance.getId());
                        dependentProcessQueue.offer(CompletableFuture.completedFuture(processInstance));
                        continue;
                    }
                    logger.debug("processInstance={} is exist, and not in success state, do START_FAILURE_TASK_PROCESS", processInstance.getId());
                    command = generateCommand(parentProcessInstance, processDefinition, processInstance.getState());
                    command.setCommandParam(String.format("{\"%s\":%d}",
                            CMDPARAM_RECOVER_PROCESS_ID_STRING, processInstance.getId()));
                    logger.debug("ProcessDependent which dependentId={} processId={} has exist processInstance={}, need to be fired in {} mode",
                            processDependent.getDependentId(), processDependent.getProcessId(), processInstance.getId(), command.getCommandType());
                } else {
                    command = generateCommand(parentProcessInstance, processDefinition, null);
                }
                processService.createCommand(command);
                logger.debug("ProcessDependent which dependentId={} processId={} fired in {} mode",
                        processDependent.getDependentId(), processDependent.getProcessId(), command.getCommandType());
            }
        }

        private Command generateCommand(ProcessInstance parentProcessInstance, ProcessDefinition processDefinition, ExecutionStatus currentStatus) {
            Date schedulerTime = parentProcessInstance.getScheduleTime();
            int schedulerInterval = parentProcessInstance.getSchedulerInterval();
            int batchNo = parentProcessInstance.getSchedulerBatchNo();
            Command command = new Command();
            if (currentStatus == null) {
                command.setCommandType(CommandType.SCHEDULER);
            } else {
                command.setCommandType(CommandType.RECOVER_SINGLE_FAILURE_PROCESS_IN_SCHEDULER);
            }
            command.setExecutorId(processDefinition.getUserId());
            command.setFailureStrategy(parentProcessInstance.getFailureStrategy());
            command.setProcessDefinitionId(processDefinition.getId());
            command.setScheduleTime(schedulerTime);
            command.setStartTime(new Date());
            command.setWarningGroupId(parentProcessInstance.getWarningGroupId());
            String workerGroup = StringUtils.isEmpty(parentProcessInstance.getWorkerGroup()) ? Constants.DEFAULT_WORKER_GROUP : parentProcessInstance.getWorkerGroup();
            command.setWorkerGroup(workerGroup);
            command.setWarningType(parentProcessInstance.getWarningType());
            command.setProcessInstancePriority(parentProcessInstance.getProcessInstancePriority());
            command.setSchedulerInterval(schedulerInterval);
            command.setSchedulerBatchNo(batchNo);
            command.setDependentSchedulerFlag(parentProcessInstance.isDependentSchedulerFlag());
            return command;
        }


    }
}
