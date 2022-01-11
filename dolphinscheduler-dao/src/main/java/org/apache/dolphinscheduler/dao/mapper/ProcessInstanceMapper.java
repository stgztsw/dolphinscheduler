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
package org.apache.dolphinscheduler.dao.mapper;

import org.apache.dolphinscheduler.dao.entity.vo.depend.DependsVo;
import org.apache.dolphinscheduler.common.enums.ExecutionStatus;
import org.apache.dolphinscheduler.dao.entity.Command;
import org.apache.dolphinscheduler.dao.entity.ExecuteStatusCount;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;

import org.apache.ibatis.annotations.Param;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;

/**
 * process instance mapper interface
 */
public interface ProcessInstanceMapper extends BaseMapper<ProcessInstance> {

    /**
     * query process instance detail info by id
     * @param processId processId
     * @return process instance
     */
    ProcessInstance queryDetailById(@Param("processId") int processId);

    /**
     * query process instance by host and stateArray
     * @param host host
     * @param stateArray stateArray
     * @return process instance list
     */
    List<ProcessInstance> queryByHostAndStatus(@Param("host") String host,
                                               @Param("states") int[] stateArray);

    /**
     * query process instance by tenantId and stateArray
     * @param tenantId tenantId
     * @param states states array
     * @return process instance list
     */
    List<ProcessInstance> queryByTenantIdAndStatus(@Param("tenantId") int tenantId,
                                               @Param("states") int[] states);

    /**
     * query process instance by worker group and stateArray
     * @param workerGroupName workerGroupName
     * @param states states array
     * @return process instance list
     */
    List<ProcessInstance> queryByWorkerGroupNameAndStatus(@Param("workerGroupName") String workerGroupName,
                                                          @Param("states") int[] states);

    /**
     * process instance page
     * @param page page
     * @param projectId projectId
     * @param processDefinitionId processDefinitionId
     * @param searchVal searchVal
     * @param statusArray statusArray
     * @param host host
     * @param startTime startTime
     * @param endTime endTime
     * @return process instance IPage
     */

    /**
     * process instance page
     * @param page page
     * @param projectId projectId
     * @param processDefinitionId processDefinitionId
     * @param searchVal searchVal
     * @param executorId executorId
     * @param statusArray statusArray
     * @param host host
     * @param startTime startTime
     * @param endTime endTime
     * @return process instance page
     */
    IPage<ProcessInstance> queryProcessInstanceListPaging(Page<ProcessInstance> page,
                                                          @Param("projectId") int projectId,
                                                          @Param("processDefinitionId") Integer processDefinitionId,
                                                          @Param("searchVal") String searchVal,
                                                          @Param("executorId") Integer executorId,
                                                          @Param("states") int[] statusArray,
                                                          @Param("host") String host,
                                                          @Param("startTime") Date startTime,
                                                          @Param("endTime") Date endTime);

    /**
     * set failover by host and state array
     * @param host host
     * @param stateArray stateArray
     * @return set result
     */
    int setFailoverByHostAndStateArray(@Param("host") String host,
                                       @Param("states") int[] stateArray);

    /**
     * update process instance by state
     * @param originState  originState
     * @param destState destState
     * @return update result
     */
    int updateProcessInstanceByState(@Param("originState") ExecutionStatus originState,
                                     @Param("destState") ExecutionStatus destState);

    /**
     *  update process instance by tenantId
     * @param originTenantId originTenantId
     * @param destTenantId destTenantId
     * @return update result
     */
    int updateProcessInstanceByTenantId(@Param("originTenantId") int originTenantId,
                                        @Param("destTenantId") int destTenantId);

    /**
     * update process instance by worker group name
     * @param originWorkerGroupName originWorkerGroupName
     * @param destWorkerGroupName destWorkerGroupName
     * @return update result
     */
    int updateProcessInstanceByWorkerGroupName(@Param("originWorkerGroupName") String originWorkerGroupName,
                                               @Param("destWorkerGroupName") String destWorkerGroupName);

    /**
     * count process instance state by user
     * @param startTime startTime
     * @param endTime endTime
     * @param projectIds projectIds
     * @return ExecuteStatusCount list
     */
    List<ExecuteStatusCount> countInstanceStateByUser(
            @Param("startTime") Date startTime,
            @Param("endTime") Date endTime,
            @Param("projectIds") Integer[] projectIds);

    /**
     * query process instance by processDefinitionId
     * @param processDefinitionId processDefinitionId
     * @param size size
     * @return process instance list
     */
    List<ProcessInstance> queryByProcessDefineId(
            @Param("processDefinitionId") int processDefinitionId,
            @Param("size") int size);

    /**
     * query last scheduler process instance
     * @param definitionId processDefinitionId
     * @param startTime startTime
     * @param endTime endTime
     * @return process instance
     */
    ProcessInstance queryLastSchedulerProcess(@Param("processDefinitionId") int definitionId,
                                              @Param("startTime") Date startTime,
                                              @Param("endTime") Date endTime,
                                              @Param("batchNo") Integer batchNo);

    /**
     * query last running process instance
     * @param definitionId definitionId
     * @param startTime startTime
     * @param endTime endTime
     * @param stateArray stateArray
     * @param batchNo batchNo
     * @return process instance
     */
    ProcessInstance queryLastRunningProcess(@Param("processDefinitionId") int definitionId,
                                            @Param("startTime") Date startTime,
                                            @Param("endTime") Date endTime,
                                            @Param("states") int[] stateArray,
                                            @Param("batchNo") Integer batchNo);

    /**
     * query last manual process instance
     * @param definitionId definitionId
     * @param startTime startTime
     * @param endTime endTime
     * @param batchNo batchNo
     * @return process instance
     */
    ProcessInstance queryLastManualProcess(@Param("processDefinitionId") int definitionId,
                                           @Param("startTime") Date startTime,
                                           @Param("endTime") Date endTime,
                                           @Param("batchNo") Integer batchNo);

    /**
     * find the ProcessInstance by processId in interval
     * @param processId processId
     * @param startTime startTime
     * @param endTime endTime
     * @param stateArray stateArray
     * @param commandTypes commandTypes
     * @param batchNo batchNo
     * @return ProcessInstance list
     */
    ProcessInstance findProcessInstanceByProcessIdInInterval(
            @Param("processId") int processId,
            @Param("startTime") Date startTime,
            @Param("endTime") Date endTime,
            @Param("states") int[] stateArray,
            @Param("commandTypes") int[] commandTypes,
            @Param("batchNo") int batchNo,
            @Param("dependentSchedulerTypes") int[] dependentSchedulerTypes,
            @Param("dependentSchedulerFlag") boolean dependentSchedulerFlag);

    /**
     * get the current batchNo by processId in interval
     * @param processId processId
     * @param startTime startTime
     * @param endTime endTime
     * @return current batchNo
     */
    Integer getCurrentSchedulerBatchNo(
            @Param("processId") int processId,
            @Param("startTime") Date startTime,
            @Param("endTime") Date endTime);


    /**
     * update the state in batch
     * @param startTime startTime
     * @param endTime endTime
     * @param stateArray stateArray
     * @param batchNo batchNo
     * @param newState newState
     * @param schedulerStartId schedulerStartId
     * @return ProcessInstance list
     */
    int updateProcessStateInBatch(@Param("startTime") Date startTime,
                                  @Param("endTime") Date endTime,
                                  @Param("states") int[] stateArray,
                                  @Param("batchNo") int batchNo,
                                  @Param("newState") int newState,
                                  @Param("schedulerStartId") int schedulerStartId);

    /**
     * find the ProcessInstance by processId in interval
     * @param page page
     * @param processId processId
     * @param processIds processIds
     * @param schedulerStartId schedulerStartId
     * @param startTime startTime
     * @param endTime endTime
     * @param stateArray stateArray
     * @param commandTypes commandTypes
     * @param batchNo batchNo
     * @return ProcessInstance page
     */
    Page<ProcessInstance> querySchedulerProcessInstances(
            IPage<ProcessInstance> page,
            @Param("processId") int processId,
            @Param("processIds") int[] processIds,
            @Param("schedulerStartId") int schedulerStartId,
            @Param("startTime") Date startTime,
            @Param("endTime") Date endTime,
            @Param("states") int[] stateArray,
            @Param("commandTypes") int[] commandTypes,
            @Param("batchNo") int batchNo,
            @Param("dependentSchedulerTypes") int[] dependentSchedulerTypes,
            @Param("dependentSchedulerFlag") boolean dependentSchedulerFlag);

    List<ProcessInstance> querySchedulerProcessInstances(
            @Param("processId") int processId,
            @Param("processIds") int[] processIds,
            @Param("schedulerStartId") int schedulerStartId,
            @Param("startTime") Date startTime,
            @Param("endTime") Date endTime,
            @Param("states") int[] stateArray,
            @Param("commandTypes") int[] commandTypes,
            @Param("batchNo") int batchNo,
            @Param("dependentSchedulerTypes") int[] dependentSchedulerTypes,
            @Param("dependentSchedulerFlag") boolean dependentSchedulerFlag);


    List<DependsVo> findLastBatchDependByProcessIdInIntervalList(
            @Param("processId") int processId,
            @Param("startTime") Date startTime,
            @Param("endTime")Date endTime,
            @Param("batchNo")int batchNo);

    List<DependsVo> findLastBatchDependByDependentIdInIntervalList(
            @Param("processId") int processId,
            @Param("startTime") Date startTime,
            @Param("endTime")Date endTime,
            @Param("batchNo")int batchNo);

    List<DependsVo> findLastBatchDependByProcessInstanceInIntervalList(
            @Param("processId") int processId,
            @Param("startTime") Date startTime,
            @Param("endTime")Date endTime,
            @Param("batchNo")int batchNo);

    List<DependsVo> findLastBatchDependByProcessDefinitionInIntervalList(
            @Param("processId") int processId
    );
}
