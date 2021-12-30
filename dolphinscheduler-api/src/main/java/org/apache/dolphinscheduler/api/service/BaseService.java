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
package org.apache.dolphinscheduler.api.service;

import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.enums.DependentViewRelation;
import org.apache.dolphinscheduler.common.enums.UserType;
import org.apache.dolphinscheduler.common.utils.DateUtils;
import org.apache.dolphinscheduler.common.utils.HadoopUtils;
import org.apache.dolphinscheduler.common.utils.StringUtils;
import org.apache.dolphinscheduler.dao.entity.ProcessDefinition;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.User;

import java.text.MessageFormat;
import java.util.*;

import static org.apache.dolphinscheduler.common.Constants.DATA_LIST;
import static org.apache.dolphinscheduler.common.enums.DependentViewRelation.ONE_ASCEND;
import static org.apache.dolphinscheduler.common.enums.DependentViewRelation.ONE_DESCEND;

/**
 * base service
 */
public class BaseService {

    /**
     * check admin
     *
     * @param user input user
     * @return ture if administrator, otherwise return false
     */
    protected boolean isAdmin(User user) {
        return user.getUserType() == UserType.ADMIN_USER;
    }

    /**
     * check admin
     *
     * @param loginUser login user
     * @param result result code
     * @return true if administrator, otherwise false
     */
    protected boolean checkAdmin(User loginUser, Map<String, Object> result) {
        //only admin can operate
        if (!isAdmin(loginUser)) {
            putMsg(result, Status.USER_NO_OPERATION_PERM);
            return true;
        }
        return false;
    }

    /**
     * put message to map
     *
     * @param result result code
     * @param status status
     * @param statusParams status message
     */
    protected void putMsg(Map<String, Object> result, Status status, Object... statusParams) {
        result.put(Constants.STATUS, status);
        if (statusParams != null && statusParams.length > 0) {
            result.put(Constants.MSG, MessageFormat.format(status.getMsg(), statusParams));
        } else {
            result.put(Constants.MSG, status.getMsg());
        }
    }

    /**
     * put message to result object
     *
     * @param result result code
     * @param status status
     * @param statusParams status message
     */
    protected void putMsg(Result result, Status status, Object... statusParams) {
        result.setCode(status.getCode());

        if (statusParams != null && statusParams.length > 0) {
            result.setMsg(MessageFormat.format(status.getMsg(), statusParams));
        } else {
            result.setMsg(status.getMsg());
        }
    }

    protected <T> void putResult(Map<String, Object> result, List<T> allOneProcessDependentsObject,
                           List<T> descendProcessDependentsObject, Object... statusParams) {

        if (isAllOneEmpty(allOneProcessDependentsObject)) {
            putMsg(result, Status.QUERY_PROCESS_IS_NOT_DEPENDENCIES, statusParams);
        } else {
            putMsg(result, Status.SUCCESS, statusParams);
            result.put(DATA_LIST,allOneProcessDependentsObject);
        }
    }

    protected <T> void putResult(Map<String, Object> result, List<T> processDependentsObject,
                                 DependentViewRelation dependentViewRelation, Object paramObject) {



        if (processDependentsObject == null || processDependentsObject.isEmpty()) {
            if (ONE_ASCEND == dependentViewRelation) {
                if (paramObject instanceof ProcessInstance) {
                    putMsg(result, Status.QUERY_PROCESS_DEPENDENCIES_ASCEND_IS_FAILD, ((ProcessInstance) paramObject).getId());
                }else if (paramObject instanceof ProcessDefinition) {
                    putMsg(result, Status.QUERY_DEFINITION_DEPENDENCIES_ASCEND_IS_FAILD, ((ProcessDefinition) paramObject).getId());
                }
            } else if (ONE_DESCEND == dependentViewRelation) {
                if (paramObject instanceof ProcessInstance) {
                    putMsg(result, Status.QUERY_PROCESS_DEPENDENCIES_DESCEND_IS_FAILD, ((ProcessInstance) paramObject).getId());
                }else if (paramObject instanceof ProcessDefinition) {
                    putMsg(result, Status.QUERY_DEFINITION_DEPENDENCIES_DESCEND_IS_FAILD, ((ProcessDefinition) paramObject).getId());
                }
            }
        } else {
            if (paramObject instanceof ProcessInstance) {
                putMsg(result, Status.SUCCESS, ((ProcessInstance) paramObject).getId());
            }else if (paramObject instanceof ProcessDefinition) {
                putMsg(result, Status.SUCCESS, ((ProcessDefinition) paramObject).getId());
            }
            result.put(DATA_LIST,processDependentsObject);
        }
    }

    private <T> boolean isAllOneEmpty(List<T> allOneProcessDependentsObject) {
        return allOneProcessDependentsObject==null || allOneProcessDependentsObject.isEmpty();
    }

    private <T> boolean isDescendEmpty(List<T> descendProcessDependentsObject) {
        return descendProcessDependentsObject!=null && !descendProcessDependentsObject.isEmpty();
    }

    /**
     * check
     *
     * @param result result
     * @param bool bool
     * @param userNoOperationPerm status
     * @return check result
     */
    protected boolean check(Map<String, Object> result, boolean bool, Status userNoOperationPerm) {
        //only admin can operate
        if (bool) {
            result.put(Constants.STATUS, userNoOperationPerm);
            result.put(Constants.MSG, userNoOperationPerm.getMsg());
            return true;
        }
        return false;
    }

    /**
     * create tenant dir if not exists
     * @param tenantCode tenant code
     * @throws Exception if hdfs operation exception
     */
    protected void createTenantDirIfNotExists(String tenantCode)throws Exception{

        String resourcePath = HadoopUtils.getHdfsResDir(tenantCode);
        String udfsPath = HadoopUtils.getHdfsUdfDir(tenantCode);
        /**
         * init resource path and udf path
         */
        HadoopUtils.getInstance().mkdir(resourcePath);
        HadoopUtils.getInstance().mkdir(udfsPath);
    }

    protected boolean hasPerm(User operateUser, int createUserId){
        return operateUser.getId() == createUserId || isAdmin(operateUser);
    }

    /**
     * check and parse date parameters
     *
     * @param startDateStr start date string
     * @param endDateStr end date string
     * @return map<status,startDate,endDate>
     */
    Map<String, Object> checkAndParseDateParameters(String startDateStr, String endDateStr){
        Map<String, Object> result = new HashMap<>();
        Date start = null;
        if (StringUtils.isNotEmpty(startDateStr)) {
            start = DateUtils.getScheduleDate(startDateStr);
            if (Objects.isNull(start)) {
                putMsg(result, Status.REQUEST_PARAMS_NOT_VALID_ERROR, Constants.START_END_DATE);
                return result;
            }
        }
        result.put(Constants.START_TIME, start);

        Date end = null;
        if (StringUtils.isNotEmpty(endDateStr)) {
            end = DateUtils.getScheduleDate(endDateStr);
            if (Objects.isNull(end)) {
                putMsg(result, Status.REQUEST_PARAMS_NOT_VALID_ERROR, Constants.START_END_DATE);
                return result;
            }
        }
        result.put(Constants.END_TIME, end);

        putMsg(result, Status.SUCCESS);
        return result;
    }
}
