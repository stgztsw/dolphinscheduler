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

package org.apache.dolphinscheduler.server.master.dispatch;


import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.enums.TaskType;
import org.apache.dolphinscheduler.common.utils.StringUtils;
import org.apache.dolphinscheduler.remote.utils.Host;
import org.apache.dolphinscheduler.server.master.dispatch.context.ExecutionContext;
import org.apache.dolphinscheduler.server.master.dispatch.enums.ExecutorType;
import org.apache.dolphinscheduler.server.master.dispatch.exceptions.ExecuteException;
import org.apache.dolphinscheduler.server.master.dispatch.executor.ExecutorManager;
import org.apache.dolphinscheduler.server.master.dispatch.executor.NettyExecutorManager;
import org.apache.dolphinscheduler.server.master.dispatch.host.HostManager;
import org.apache.dolphinscheduler.server.master.dispatch.host.LowerWeightHostManager;
import org.apache.dolphinscheduler.server.master.registry.ServerNodeManager;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;

/**
 * executor dispatcher
 */
@Service
public class ExecutorDispatcher implements InitializingBean {

    /**
     * netty executor manager
     */
    @Autowired
    private NettyExecutorManager nettyExecutorManager;

    /**
     * round robin host manager
     */
    @Autowired
    private HostManager hostManager;

    @Autowired
    protected ServerNodeManager serverNodeManager;

    /**
     * executor manager
     */
    private final ConcurrentHashMap<ExecutorType, ExecutorManager<Boolean>> executorManagers;

    /**
     * constructor
     */
    public ExecutorDispatcher(){
        this.executorManagers = new ConcurrentHashMap<>();
    }

    /**
     * task dispatch
     *
     * @param context context
     * @return result
     * @throws ExecuteException if error throws ExecuteException
     */
    public Boolean dispatch(final ExecutionContext context) throws ExecuteException, InterruptedException {
        /**
         * get executor manager
         */
        ExecutorManager<Boolean> executorManager = this.executorManagers.get(context.getExecutorType());
        if(executorManager == null){
            throw new ExecuteException("no ExecutorManager for type : " + context.getExecutorType());
        }

        TaskType taskType = TaskType.valueOf(context.getTaskType());
        //节点负载信息有延迟，最高可达到25秒，务必须获得最新的节点负载信息，否则有可能能导致内存使用过载
        //work通过心跳测试同步节点负载，所以睡眠1秒以拿到最新的节点负载信息
        if (TaskType.DATAX == taskType) {
            //datax耗内存，所以需要睡眠1秒以确保最新的负载信息刷新到zk中
            Thread.sleep(Constants.SLEEP_TIME_MILLIS);
        } else if (TaskType.SQL == taskType) {
            //do nothing
        } else {
            Thread.sleep(500);
        }
        //其他类型的任务不等待时间，如果还是出现负载高的话，可以调整等待一定的时间
        Runnable workerNodeInfoAndGroupDbSyncTask = serverNodeManager.getWorkerNodeInfoAndGroupDbSyncTask();
        Runnable refreshResourceTask = ((LowerWeightHostManager)hostManager).getRefreshResourceTask();
        //这边直接执行run方法而不是启动新线程的目的是通过对象调用成员方法达到阻塞同步最新的节点负载信息
        workerNodeInfoAndGroupDbSyncTask.run();
        refreshResourceTask.run();

        /**
         * host select
         */
        Host host = hostManager.select(context);
        if (StringUtils.isEmpty(host.getAddress())) {
            throw new ExecuteException(String.format("[Task dispatch] fail to execute : %s due to no suitable worker, "
                            + "current task needs worker group %s to execute",
                    context.getCommand(),context.getWorkerGroup()));
        }
        context.setHost(host);
        executorManager.beforeExecute(context);
        try {
            /**
             * task execute
             */
            return executorManager.execute(context);
        } finally {
            executorManager.afterExecute(context);
        }
    }

    /**
     * register init
     * @throws Exception if error throws Exception
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        register(ExecutorType.WORKER, nettyExecutorManager);
        register(ExecutorType.CLIENT, nettyExecutorManager);
    }

    /**
     *  register
     * @param type executor type
     * @param executorManager executorManager
     */
    public void register(ExecutorType type, ExecutorManager executorManager){
        executorManagers.put(type, executorManager);
    }
}
