package org.apache.dolphinscheduler.api.runner;

import org.apache.dolphinscheduler.api.service.SchedulerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/**
 * @program: dolphinscheduler
 * @description:
 * @author: Mr.Yang
 * @create: 2022-02-24 14:24
 **/

@Component(value = "ApiApplicationRunner")
public class ApiApplicationRunner implements ApplicationRunner {

    private static final Logger logger = LoggerFactory.getLogger(ApiApplicationRunner.class);
    private static Integer num = 0;
    @Autowired
    private SchedulerService schedulerService;

    @Override
    public void run(ApplicationArguments var1) throws Exception{
        num = num+1;

        logger.info("start depend while check module,{},{},num:{}",Thread.currentThread().getName(),Thread.currentThread().getId(),num);

        // 开启依赖巡检 quartz 定时计划
        schedulerService.setDependStateCheckSchedule(-252,-252);
        // 开启巡检线程
//        dependCheckRunnerService.start();
    }
}