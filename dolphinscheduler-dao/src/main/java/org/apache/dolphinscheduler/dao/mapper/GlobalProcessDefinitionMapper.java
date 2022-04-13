package org.apache.dolphinscheduler.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import org.apache.dolphinscheduler.dao.entity.ProcessDefinition;
import org.apache.dolphinscheduler.dao.entity.ProcessInstanceMap;
import org.apache.ibatis.annotations.Param;

public interface GlobalProcessDefinitionMapper extends BaseMapper<ProcessDefinition> {
    /**
     * process definition page
     * @param page page
     * @param searchVal searchVal
     * @param userId userId
     * @param isAdmin isAdmin
     * @return process definition IPage
     */
    IPage<ProcessDefinition> queryDefineListPaging(IPage<ProcessDefinition> page,
                                                   @Param("searchVal") String searchVal,
                                                   @Param("userId") int userId,
                                                   @Param("isAdmin") boolean isAdmin);
}
