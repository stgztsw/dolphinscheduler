package org.apache.dolphinscheduler.api.service;

import com.alibaba.fastjson.JSON;
import org.apache.dolphinscheduler.common.enums.ExecutionStatus;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.utils.PageInfo;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.utils.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.apache.dolphinscheduler.dao.entity.*;
import org.apache.dolphinscheduler.dao.mapper.*;

import java.util.*;

/**
 * @program: dolphinscheduler
 * @description:
 * @author: Mr.Yang
 * @create: 2022-04-06 11:58
 **/
@Service
public class GlobalService extends BaseService {
    private static final Logger logger = LoggerFactory.getLogger(GlobalService.class);

    @Autowired
    GlobalProcessInstanceMapper globalProcessInstanceMapper;

    @Autowired
    GlobalTaskInstanceMapper globalTaskInstanceMapper;

    @Autowired
    UsersService usersService;

    /**
     * paging query process instance list, filtering according to project, process definition, time range, keyword, process status
     *
     * @param loginUser login user
     * @param projectName project name
     * @param pageNo page number
     * @param pageSize page size
     * @param processDefineId process definition id
     * @param searchVal search value
     * @param stateType state type
     * @param host host
     * @param startDate start time
     * @param endDate end time
     * @return process instance list
     */
    public Map<String, Object> queryProcessInstanceList(User loginUser, Integer processDefineId,
                                                        String startDate, String endDate,
                                                        String searchVal, String executorName,String stateType, String host,
                                                        Integer pageNo, Integer pageSize) {

        Map<String, Object> result = new HashMap<>(5);

        // TODO 权限问题

        int[] statusArray = null;
        // filter by state
        if (stateType != null && !stateType.equals("[]")) {
            statusArray = JSON.parseArray(stateType).stream().mapToInt(var -> ExecutionStatus.valueOf((String) var).ordinal()).toArray();
        }
        Map<String, Object> checkAndParseDateResult = checkAndParseDateParameters(startDate, endDate);
        if (checkAndParseDateResult.get(Constants.STATUS) != Status.SUCCESS) {
            return checkAndParseDateResult;
        }
        Date start = (Date) checkAndParseDateResult.get(Constants.START_TIME);
        Date end = (Date) checkAndParseDateResult.get(Constants.END_TIME);

        Page<ProcessInstance> page = new Page(pageNo, pageSize);
        PageInfo pageInfo = new PageInfo<ProcessInstance>(pageNo, pageSize);
        int executorId = usersService.getUserIdByName(executorName);

        IPage<ProcessInstance> processInstanceList =
                globalProcessInstanceMapper.queryGlobalProcessInstanceListPaging(page,
                        processDefineId, searchVal, executorId,statusArray, host, start, end);

        List<ProcessInstance> processInstances = processInstanceList.getRecords();

        if (processInstances.size()<=0) {
            logger.info("don't exists this searchvar :{} in global",searchVal);
            result.put(Constants.MSG,"don't exists this searchvar :" + searchVal + " in global");
            putMsg(result,Status.GLOBAL_PROCESS_INSTANCE_NOT_EXIST);
            return result;
        }

        for(ProcessInstance processInstance: processInstances){// update desc 从数据库中找到实例列表，插入的时候是从队列中插入
            processInstance.setDuration(DateUtils.format2Duration(processInstance.getStartTime(),processInstance.getEndTime()));
            User executor = usersService.queryUser(processInstance.getExecutorId());
            if (null != executor) {
                processInstance.setExecutorName(executor.getUserName());
            }
        }

        pageInfo.setTotalCount((int) processInstanceList.getTotal());
        pageInfo.setLists(processInstances);
        result.put(Constants.DATA_LIST, pageInfo);
        putMsg(result, Status.SUCCESS);
        return result;
    }
}
