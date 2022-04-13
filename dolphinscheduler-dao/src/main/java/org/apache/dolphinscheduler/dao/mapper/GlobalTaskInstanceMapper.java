package org.apache.dolphinscheduler.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.ibatis.annotations.Param;

import java.util.Date;

public interface GlobalTaskInstanceMapper extends BaseMapper<TaskInstance> {
    /**
     * task instance page
     * @param page page
     * @param processInstanceId processInstanceId
     * @param searchVal searchVal
     * @param executorId executorId
     * @param statusArray statusArray
     * @param host host
     * @param startTime startTime
     * @param endTime endTime
     * @return process instance page
     */
    IPage<TaskInstance> queryGlobalTaskInstanceListPaging(Page<TaskInstance> page,
                                                                @Param("processInstanceId") Integer processInstanceId,
                                                                @Param("searchVal") String searchVal,
                                                                @Param("executorId") Integer executorId,
                                                                @Param("states") int[] statusArray,
                                                                @Param("host") String host,
                                                                @Param("startTime") Date startTime,
                                                                @Param("endTime") Date endTime);
}
