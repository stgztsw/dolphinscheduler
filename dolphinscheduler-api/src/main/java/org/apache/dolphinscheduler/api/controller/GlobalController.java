package org.apache.dolphinscheduler.api.controller;

import io.swagger.annotations.*;
import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.exceptions.ApiException;
import org.apache.dolphinscheduler.api.service.GlobalService;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.enums.ExecutionStatus;
import org.apache.dolphinscheduler.common.utils.ParameterUtils;
import org.apache.dolphinscheduler.dao.entity.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import springfox.documentation.annotations.ApiIgnore;

import java.util.List;
import java.util.Map;

import static org.apache.dolphinscheduler.api.enums.Status.*;

/**
 * @program: dolphinscheduler
 * @description: global interface
 * @author: Mr.Yang
 * @create: 2022-04-06 11:54
 **/

@Api(tags = "GLOBAL_TAG", position = 10)
@RestController
@RequestMapping("projects/global")
public class GlobalController extends BaseController{
    private static final Logger logger = LoggerFactory.getLogger(GlobalController.class);

    @Autowired
    private GlobalService globalService;

    /**
     * query process instance list paging
     *
     * @param loginUser           login user
     * @param pageNo              page number
     * @param pageSize            page size
     * @param processDefinitionId process definition id
     * @param searchVal           search value
     * @param stateType           state type
     * @param host                host
     * @param startTime           start time
     * @param endTime             end time
     * @return process instance list
     */
    @ApiOperation(value = "queryProcessInstanceList", notes = "QUERY_PROCESS_INSTANCE_LIST_NOTES")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "processDefinitionId", value = "PROCESS_DEFINITION_ID", dataType = "Int", example = "100"),
            @ApiImplicitParam(name = "searchVal", value = "SEARCH_VAL", type = "String"),
            @ApiImplicitParam(name = "executorName", value = "EXECUTOR_NAME", type = "String"),
            @ApiImplicitParam(name = "stateType", value = "EXECUTION_STATUS", type = "String"),
            @ApiImplicitParam(name = "host", value = "HOST", type = "String"),
            @ApiImplicitParam(name = "startDate", value = "START_DATE", type = "String"),
            @ApiImplicitParam(name = "endDate", value = "END_DATE", type = "String"),
            @ApiImplicitParam(name = "pageNo", value = "PAGE_NO", dataType = "Int", example = "100"),
            @ApiImplicitParam(name = "pageSize", value = "PAGE_SIZE", dataType = "Int", example = "100")
    })
    @GetMapping(value = "process-instance-list-paging")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(QUERY_PROCESS_INSTANCE_LIST_PAGING_ERROR)
    public Result queryProcessInstanceList(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                           @RequestParam(value = "processDefinitionId", required = false, defaultValue = "0") Integer processDefinitionId,
                                           @RequestParam(value = "searchVal", required = false) String searchVal,
                                           @RequestParam(value = "executorName", required = false) String executorName,
                                           @RequestParam(value = "stateType", required = false) String stateType,
                                           @RequestParam(value = "host", required = false) String host,
                                           @RequestParam(value = "globalStartDate", required = false) String startTime,
                                           @RequestParam(value = "globalEndDate", required = false) String endTime,
                                           @RequestParam("pageNo") Integer pageNo,
                                           @RequestParam("pageSize") Integer pageSize) {
        logger.info("query all process instance list, login user:{}, define id:{}," +
                        "search value:{},executor name:{},state type:{},host:{},global start time:{}, global "
                        + "end time:{},page number:{}, page size:{}",
                loginUser.getUserName(), processDefinitionId, searchVal, executorName, stateType, host,
                startTime, endTime, pageNo, pageSize);
        searchVal = ParameterUtils.handleEscapes(searchVal);
        Map<String, Object> result = globalService.queryProcessInstanceList(
                loginUser, processDefinitionId, startTime, endTime, searchVal, executorName, stateType, host, pageNo, pageSize);
        return returnDataListPaging(result);
    }

    /**
     * query task list paging
     *
     * @param loginUser         login user
     * @param processInstanceId process instance id
     * @param searchVal         search value
     * @param stateType         state type
     * @param host              host
     * @param startTime         start time
     * @param endTime           end time
     * @param pageNo            page number
     * @param pageSize          page size
     * @return task list page
     */
    @ApiOperation(value = "queryTaskListPaging", notes = "QUERY_TASK_INSTANCE_LIST_PAGING_NOTES")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "processInstanceId", value = "PROCESS_INSTANCE_ID", required = false, dataType = "Int", example = "100"),
            @ApiImplicitParam(name = "searchVal", value = "SEARCH_VAL", type = "String"),
            @ApiImplicitParam(name = "taskName", value = "TASK_NAME", type = "String"),
            @ApiImplicitParam(name = "executorName", value = "EXECUTOR_NAME", type = "String"),
            @ApiImplicitParam(name = "stateType", value = "EXECUTION_STATUS", type = "ExecutionStatus"),
            @ApiImplicitParam(name = "host", value = "HOST", type = "String"),
            @ApiImplicitParam(name = "startDate", value = "START_DATE", type = "String"),
            @ApiImplicitParam(name = "endDate", value = "END_DATE", type = "String"),
            @ApiImplicitParam(name = "pageNo", value = "PAGE_NO", dataType = "Int", example = "1"),
            @ApiImplicitParam(name = "pageSize", value = "PAGE_SIZE", dataType = "Int", example = "20")
    })
    @GetMapping("task-instance-list-paging")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(QUERY_TASK_LIST_PAGING_ERROR)
    public Result queryTaskListPaging(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                      @RequestParam(value = "processInstanceId", required = false, defaultValue = "0") Integer processInstanceId,
                                      @RequestParam(value = "searchVal", required = false) String searchVal,
                                      @RequestParam(value = "executorName", required = false) String executorName,
                                      @RequestParam(value = "stateType", required = false) String stateType,
                                      @RequestParam(value = "host", required = false) String host,
                                      @RequestParam(value = "globalStartDate", required = false) String startTime,
                                      @RequestParam(value = "globalEndDate", required = false) String endTime,
                                      @RequestParam("pageNo") Integer pageNo,
                                      @RequestParam("pageSize") Integer pageSize) {

        logger.info("query task instance list, search value:{}, executor name: {},state type:{}, host:{}, start:{}, end:{}", searchVal, executorName, stateType, host, startTime, endTime);
        searchVal = ParameterUtils.handleEscapes(searchVal);
        Map<String, Object> result = globalService.queryTaskListPaging(
                loginUser, processInstanceId, executorName, startTime, endTime, searchVal, stateType, host, pageNo, pageSize);
        return returnDataListPaging(result);
    }

    /**
     * query process definition list paging
     *
     * @param loginUser   login user
     * @param projectName project name
     * @param searchVal   search value
     * @param pageNo      page number
     * @param pageSize    page size
     * @param userId      user id
     * @return process definition page
     */
    @ApiOperation(value = "queryProcessDefinitionListPaging", notes= "QUERY_PROCESS_DEFINITION_LIST_PAGING_NOTES")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "pageNo", value = "PAGE_NO", required = true, dataType = "Int", example = "100"),
            @ApiImplicitParam(name = "searchVal", value = "SEARCH_VAL", required = false, type = "String"),
            @ApiImplicitParam(name = "userId", value = "USER_ID", required = false, dataType = "Int", example = "100"),
            @ApiImplicitParam(name = "pageSize", value = "PAGE_SIZE", required = true, dataType = "Int", example = "100")
    })
    @GetMapping(value = "process-definition-list-paging")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(QUERY_PROCESS_DEFINITION_LIST_PAGING_ERROR)
    public Result queryProcessDefinitionListPaging(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                                   @RequestParam("pageNo") Integer pageNo,
                                                   @RequestParam(value = "searchVal", required = false) String searchVal,
                                                   @RequestParam(value = "userId", required = false, defaultValue = "0") Integer userId,
                                                   @RequestParam("pageSize") Integer pageSize) {
        logger.info("query process definition list paging, login user:{}, searchVal:{}, userId:{}", loginUser.getUserName(), searchVal, userId);
        Map<String, Object> result = checkPageParams(pageNo, pageSize);
        if (result.get(Constants.STATUS) != Status.SUCCESS) {
            return returnDataListPaging(result);
        }
        searchVal = ParameterUtils.handleEscapes(searchVal);
        result = globalService.queryProcessDefinitionListPaging(loginUser, searchVal, pageNo, pageSize, userId);
        return returnDataListPaging(result);
    }
}
