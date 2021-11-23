package org.apache.dolphinscheduler.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import org.apache.dolphinscheduler.dao.entity.ProcessDependent;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * process dependent mapper interface
 */
public interface ProcessDependentMapper extends BaseMapper<ProcessDependent> {

    /**
     * batch insert process dependent
     * @param  processDependents processDependents
     * @return flag
     */
    int insertBatch(@Param("processDependents")List<ProcessDependent> processDependents);

    /**
     * delete de data by processId
     * @param  processId processId
     * @return flag
     */
    int deleteByProcessId(@Param("processId")int processId);

    /**
     * query process dependent by process id
     * @param  processId processId
     * @return process dependent
     */
    List<ProcessDependent> queryByProcessId(@Param("processId") int processId);

    /**
     * query process dependent by dependent id
     * @param  dependentId dependentId
     * @return process dependent
     */
    List<ProcessDependent> queryByDependentId(@Param("dependentId") int dependentId);

    /**
     * query one data by dependent id
     * @param  dependentId dependentId
     * @return process dependent
     */
    List<ProcessDependent> queryOneDataByDependentId(@Param("dependentId") int dependentId);

    /**
     * project page
     * @param page page
     * @param dependentId dependentId
     * @return ProcessDependent Ipage
     */
    Page<ProcessDependent> queryByDependentIdListPaging(IPage<ProcessDependent> page,
                                                        @Param("dependentId") int dependentId);
}
