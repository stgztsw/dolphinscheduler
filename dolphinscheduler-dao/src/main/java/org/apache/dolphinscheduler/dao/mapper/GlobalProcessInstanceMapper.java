package org.apache.dolphinscheduler.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.ProcessInstanceMap;
import org.apache.ibatis.annotations.Param;

import java.util.Date;

/**
 * @program: dolphinscheduler
 * @description:
 * @author: Mr.Yang
 * @create: 2022-04-06 12:02
 **/

public interface GlobalProcessInstanceMapper extends BaseMapper<ProcessInstanceMap> {

    /**
     * process instance page
     * @param page page
     * @param processDefinitionId processDefinitionId
     * @param searchVal searchVal
     * @param executorId executorId
     * @param statusArray statusArray
     * @param host host
     * @param startTime startTime
     * @param endTime endTime
     * @return process instance page
     */
    IPage<ProcessInstance> queryGlobalProcessInstanceListPaging(Page<ProcessInstance> page,
                                                          @Param("processDefinitionId") Integer processDefinitionId,
                                                          @Param("searchVal") String searchVal,
                                                          @Param("executorId") Integer executorId,
                                                          @Param("states") int[] statusArray,
                                                          @Param("host") String host,
                                                          @Param("startTime") Date startTime,
                                                          @Param("endTime") Date endTime);
}
