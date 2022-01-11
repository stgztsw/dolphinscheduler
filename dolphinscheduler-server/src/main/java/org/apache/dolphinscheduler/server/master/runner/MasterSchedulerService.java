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

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.enums.*;
import org.apache.dolphinscheduler.common.function.TriConsumer;
import org.apache.dolphinscheduler.common.thread.Stopper;
import org.apache.dolphinscheduler.common.thread.ThreadUtils;
import org.apache.dolphinscheduler.common.utils.*;
import org.apache.dolphinscheduler.dao.entity.Command;
import org.apache.dolphinscheduler.dao.entity.ProcessDefinition;
import org.apache.dolphinscheduler.dao.entity.ProcessDependent;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.remote.NettyRemotingClient;
import org.apache.dolphinscheduler.remote.config.NettyClientConfig;
import org.apache.dolphinscheduler.server.master.config.MasterConfig;
import org.apache.dolphinscheduler.server.master.zk.ZKMasterClient;
import org.apache.dolphinscheduler.service.process.ProcessService;
import org.apache.dolphinscheduler.service.quartz.cron.SchedulingBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.dolphinscheduler.common.Constants.*;

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
                                    future = CompletableFuture.completedFuture(processInstance);
                                }
                                if (!isSubProcess(command)) {
                                    dependentProcessQueue.offer(future);
                                }
                                logger.info("instanceId={} definitionId={} is added to dependentProcessQueue", processInstance.getId(), processInstance.getProcessDefinitionId());
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

    private boolean isSubProcess(Command command) {
        Map<String, String> cmdParam = JSONUtils.toMap(command.getCommandParam());
        if (cmdParam == null) {
            return false;
        }
        return cmdParam.containsKey(Constants.CMDPARAM_SUB_PROCESS);
    }

    private String getLocalAddress() {
        return NetUtils.getAddr(masterConfig.getListenPort());
    }


    class DependentScheduler extends Thread{

        private final int pageSize = 10;

        private long lastCheckTime = System.currentTimeMillis();

        private final TriConsumer<Command, ProcessInstance, ProcessInstance> SchedulerConsumer = (command, processInstance, parentProcessInstance)-> {
            setCommandType(command, parentProcessInstance.getDependentSchedulerType());
        };

        private final TriConsumer<Command, ProcessInstance, ProcessInstance> recoveryFailureConsumer = (command, processInstance, parentProcessInstance) -> {
            command.setCommandType(CommandType.RECOVER_SINGLE_FAILURE_PROCESS_IN_SCHEDULER);
            Map<String, String> cmdParam = this.convert2Map(command.getCommandParam());
            setProcessId(cmdParam, processInstance.getId());
            command.setCommandParam(map2String(cmdParam));
        };

        private final TriConsumer<Command, ProcessInstance, ProcessInstance> reRunConsumer = (command, processInstance, parentProcessInstance) -> {
            command.setCommandType(CommandType.REPEAT_RUNNING_SCHEDULER);
            Map<String, String> cmdParam = this.convert2Map(command.getCommandParam());
            if (processInstance != null) {
                setProcessId(cmdParam, processInstance.getId());
            }
            setRerunNo(cmdParam, parentProcessInstance.getSchedulerRerunNo());
            command.setCommandParam(map2String(cmdParam));
        };

        private final TriConsumer<Command, ProcessInstance, ProcessInstance> informalConsumer = (command, processInstance, parentProcessInstance) -> {
            setCommandType(command, parentProcessInstance.getDependentSchedulerType());
            Map<String, String> cmdParam = this.convert2Map(command.getCommandParam());
            setProcessId(cmdParam, processInstance.getId());
            setInformal(cmdParam);
            command.setCommandParam(map2String(cmdParam));
        };

        private void setCommandType(Command command, DependentSchedulerType type) {
            if (type == DependentSchedulerType.SCHEDULER) {
                command.setCommandType(CommandType.SCHEDULER);
            } else if (type == DependentSchedulerType.MANUAL_SCHEDULER) {
                command.setCommandType(CommandType.MANUAL_SCHEDULER);
            } else if (type == DependentSchedulerType.RECOVER) {
                command.setCommandType(CommandType.RECOVER_SINGLE_FAILURE_PROCESS_IN_SCHEDULER);
            } else if (type == DependentSchedulerType.REPEAT) {
                command.setCommandType(CommandType.REPEAT_RUNNING_SCHEDULER);
            }
        }

        /**
         * abnormal Process Queue
         */
        private final ConcurrentLinkedQueue<Integer> abnormalProcessQueue = new ConcurrentLinkedQueue();

        public DependentScheduler(String name) {
            super(name);
        }

        @Override
        public void run() {
            logger.info("master dependent scheduler started");
            while (true) {
                try {
                    if (dependentProcessQueue.isEmpty()) {
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
                        logger.info("instanceId={} definitionId={} whose execution thread is complete, take it from dependentProcessQueue",
                                parentProcessInstance.getId(), parentProcessInstance.getProcessDefinitionId());
                        if (!parentProcessInstance.isDependentSchedulerFlag()) {
                            logger.info("instanceId={} definitionId={} whose DependentSchedulerFlag is false, " +
                                    "no need to fire the dependent process", parentProcessInstance.getId(), parentProcessInstance.getProcessDefinitionId());
                            futureIterator.remove();
                            continue;
                        }
                        if (!parentProcessInstance.getState().typeIsFinished()) {
                            //future已经完成，虽然任务没有完成，但其实任务状态不会再被执行线程改变，所以这边需要检查数据库内的状态
                            parentProcessInstance = processService.findProcessInstanceDetailById(parentProcessInstance.getId());
                            if (!parentProcessInstance.getState().typeIsFinished()) {
                                abnormalProcessQueue.offer(parentProcessInstance.getId());
                                logger.error("instanceId={} definitionId={} whose execution thread is complete, but status={} is still not finished, transfer it to abnormalProcessQueue",
                                        parentProcessInstance.getId(), parentProcessInstance.getProcessDefinitionId(), parentProcessInstance.getState().getDescp());
                                futureIterator.remove();
                                continue;
                            }
                        }
                        if (!parentProcessInstance.getState().typeIsSuccess()) {
                            logger.info("instanceId={} definitionId={} whose status={} is not success, no need to fire the dependent process",
                                    parentProcessInstance.getId(), parentProcessInstance.getProcessDefinitionId(), parentProcessInstance.getState().getDescp());
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
                                logger.info("instanceId={} definitionId={} whose status={} is success has no dependent process need to be fire",
                                        parentProcessInstance.getId(), parentProcessInstance.getProcessDefinitionId(), parentProcessInstance.getState().getDescp());
                                break;
                            }
                            schedulerProcess(parentProcessInstance, processDependents);
                        } while (page.hasNext());
                        futureIterator.remove();
                    }
                    //检查异常实例
                    if (System.currentTimeMillis() - lastCheckTime > 60000) {
                        if (abnormalProcessQueue.isEmpty()) {
                            lastCheckTime = System.currentTimeMillis();
                            continue;
                        }
                        Iterator<Integer> iterator = abnormalProcessQueue.iterator();
                        while (iterator.hasNext()) {
                            ProcessInstance abnormalProcessInstance = processService.findProcessInstanceDetailById(iterator.next());
                            if (!abnormalProcessInstance.getState().typeIsFinished()) {
                                if (DateUtils.differSec(new Date(), abnormalProcessInstance.getStartTime()) > 86400) {
                                    logger.info("in abnormalProcessQueue instanceId={} definitionId={} whose status={} running more than 24h, discard it from abnormalProcessQueue",
                                            abnormalProcessInstance.getId(),  abnormalProcessInstance.getProcessDefinitionId(), abnormalProcessInstance.getState().getDescp());
                                    iterator.remove();
                                }
                                continue;
                            }
                            logger.info("in abnormalProcessQueue instanceId={} definitionId={} whose status={} is complete, transfer it to dependentProcessQueue",
                                    abnormalProcessInstance.getId(),  abnormalProcessInstance.getProcessDefinitionId(), abnormalProcessInstance.getState().getDescp());
                            dependentProcessQueue.offer(CompletableFuture.completedFuture(abnormalProcessInstance));
                            iterator.remove();
                        }
                        lastCheckTime = System.currentTimeMillis();
                    }
                } catch (Throwable e) {
                    logger.error("master scheduler thread error", e);
                }
            }
        }

        private void schedulerProcess(ProcessInstance parentProcessInstance, List<ProcessDependent> processDependents) {
            for (ProcessDependent processDependent : processDependents) {
                logger.info("definitionId={} is need to be fired by parentInstanceId={} parentDefinitionId={}",
                        processDependent.getProcessId(), parentProcessInstance.getId(), parentProcessInstance.getProcessDefinitionId());
                ProcessDefinition processDefinition = processService
                        .findDefineSchedulerById(processDependent.getProcessId());
                if (processDefinition == null) {
                    logger.error("definitionId={} is not exist", processDependent.getProcessId());
                    continue;
                }
                if (ReleaseState.OFFLINE == processDefinition.getReleaseState()) {
                    logger.info("definitionId={} is offline, no need to fire it", processDefinition.getId());
                    continue;
                }
                SchedulingBatch sb = new SchedulingBatch(parentProcessInstance);
                if (processService.dependentProcessIsFired(sb, processDefinition.getId())) {
                    logger.info("definitionId={} has been fired in command queue; SchedulingBatch info: startTime={} endTime={} batchNO={}",
                            processDefinition.getId(), sb.getStartTime(), sb.getEndTime(), sb.getBatchNo());
                    continue;
                }
                boolean hasValidateFireDate = false;
                if (processDefinition.getScheduleStartTime() != null && processDefinition.getScheduleEndTime() != null
                        && !processDefinition.getScheduleCrontab().isEmpty()) {
                    hasValidateFireDate = processService.hasValidateFireDate(new Date(), processDefinition.getScheduleEndTime(), processDefinition.getScheduleCrontab());
                }
                //上游节点为定时调度并且当前normal节点设置了调度时间
                if (parentProcessInstance.getDependentSchedulerType() == DependentSchedulerType.SCHEDULER && hasValidateFireDate) {
                    if (!processService.findAndUpdateInformalProcessInstance(processDefinition.getId(), sb, parentProcessInstance)) {
                        logger.info("definitionId={} has crontab timing, the state is not the INFORMAL_FAKE, need do nothing", processDefinition.getId());
                        continue;
                    }
                }
                //查询对应的时间周期内的批次，并且dependent_scheduler_flag为true的数据
                ProcessInstance processInstance= processService
                        .findProcessInstanceByProcessIdInInterval(sb, processDefinition.getId(),null, null, null, true);
                initOlderProperty(processInstance);
                if (parentProcessInstance.isRerunSchedulerFlag()) {
                    if (processInstance == null || !parentProcessInstance.getSchedulerRerunNo().equals(processInstance.getSchedulerRerunNo())) {
                        //发起重跑调度这个processInstance
                        generateCommand(parentProcessInstance, processDefinition, processInstance, reRunConsumer);
                    }
                    continue;
                }
                if (processInstance != null) {
                    if (!processInstance.getState().typeIsFinished() && ExecutionStatus.INITED != processInstance.getState() && !processInstance.getState().typeIsInformal()) {
                        logger.info("definitionId={} has existed instance={} in running now, no need to be fired", processDefinition.getId(), processInstance.getId());
                        continue;
                    }else if (ExecutionStatus.SUCCESS == processInstance.getState()) {
                        dependentProcessQueue.offer(CompletableFuture.completedFuture(processInstance));
                        logger.info("definitionId={} has existed instance={}. the state is success, no need to be fired, and add it to dependentProcessQueue",
                                processDefinition.getId(), processInstance.getId());
                        continue;
                    }
                    if (processInstance.getState() == ExecutionStatus.INFORMAL) {
                        logger.info("definitionId={} has existed instance={}, the state is informal, do informal execution", processDefinition.getId(), processInstance.getId());
                        generateCommand(parentProcessInstance, processDefinition, processInstance, informalConsumer);
                    } else {
                        logger.info("definitionId={} has existed instance={}, the state is success, do START_FAILURE_TASK_PROCESS", processDefinition.getId(), processInstance.getId());
                        generateCommand(parentProcessInstance, processDefinition, processInstance, recoveryFailureConsumer);
                    }
                } else {
                    generateCommand(parentProcessInstance, processDefinition, null, SchedulerConsumer);
                }
            }
        }

        private void initOlderProperty(ProcessInstance processInstance) {
            if (processInstance == null) {
                return;
            }
            processInstance.setRerunSchedulerFlag(false);
        }

        private Command generateCommand(ProcessInstance parentProcessInstance, ProcessDefinition processDefinition,
                                        ProcessInstance processInstance, TriConsumer<Command, ProcessInstance, ProcessInstance> consumer) {
            Date schedulerTime = parentProcessInstance.getScheduleTime();
            int schedulerInterval = parentProcessInstance.getSchedulerInterval();
            int batchNo = parentProcessInstance.getSchedulerBatchNo();
            Command command = new Command();
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
            command.setDependentSchedulerType(parentProcessInstance.getDependentSchedulerType());
            command.setSchedulerRerunNo(parentProcessInstance.getSchedulerRerunNo());
            if (parentProcessInstance.getProcessType() == ProcessType.SCHEDULER) {
                command.setSchedulerStartId(parentProcessInstance.getId());
            } else {
                command.setSchedulerStartId(parentProcessInstance.getSchedulerStartId());
            }
            command.setRerunSchedulerFlag(parentProcessInstance.isRerunSchedulerFlag());
            consumer.accept(command, processInstance, parentProcessInstance);
            processService.createCommand(command);
            logger.info("definitionId={} fired in {} mode, command info: {}",
                    processDefinition.getId(), parentProcessInstance.getDependentSchedulerType(), command.toString());
            return command;
        }

        private Map<String, String> convert2Map(String cmdParam) {
            if(StringUtils.isEmpty(cmdParam)) {
                return new HashMap<>();
            }
            return JSONUtils.toMap(cmdParam);
        }

        private Map<String, String> setProcessId(Map<String, String> cmdParam, int processInstanceId) {
            cmdParam.put(CMDPARAM_RECOVER_PROCESS_ID_STRING, String.valueOf(processInstanceId));
            return cmdParam;
        }

        private Map<String, String> setRerunNo(Map<String, String> cmdParam, String rerunNo) {
            cmdParam.put(CMDPARAM_RERUN_SCHEDULER, rerunNo);
            return cmdParam;
        }

        private Map<String, String> setInformal(Map<String, String> cmdParam) {
            cmdParam.put(CMDPARAM_INFORMAL_SCHEDULER, CMDPARAM_INFORMAL_SCHEDULER);
            return cmdParam;
        }

        private String map2String(Map<String, String> cmdParam) {
            if (cmdParam == null || cmdParam.isEmpty()) {
                return null;
            }
            return JSONUtils.toJson(cmdParam);
        }

    }
}
