package org.apache.dolphinscheduler.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.dolphinscheduler.dao.entity.DependCheckParam;

/**
 * @program: dolphinscheduler
 * @description:
 * @author: Mr.Yang
 * @create: 2022-01-25 10:42
 **/

public interface DependCheckParamMapper extends BaseMapper<DependCheckParam> {

    DependCheckParam getOne();

}
