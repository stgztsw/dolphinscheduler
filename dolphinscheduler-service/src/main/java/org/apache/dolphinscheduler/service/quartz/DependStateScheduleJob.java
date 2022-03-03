package org.apache.dolphinscheduler.service.quartz;

import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.enums.ShowType;
import org.apache.dolphinscheduler.service.bean.SpringApplicationContext;
import org.apache.dolphinscheduler.service.depend.DependStateCheckExecutor;
import org.apache.dolphinscheduler.service.depend.email.manager.EmailManager;
import org.apache.dolphinscheduler.service.depend.email.utils.PropertyUtils;
import org.apache.dolphinscheduler.service.process.ProcessService;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


/**
 * @program: dolphinscheduler
 * @description: cron被真正定时调度的时候就执行execute方法
 * @author: Mr.Yang
 * @create: 2022-01-19 17:59
 **/
public class DependStateScheduleJob implements Job {
    private static Integer num = 0;
    /**
     * Logger
     */
    private static final Logger logger = LoggerFactory.getLogger(DependStateScheduleJob.class);

    private EmailManager emailManager = new EmailManager();

    /**
     * get ProcessService Object
     * @return
     */
    public ProcessService getProcessService(){
        return SpringApplicationContext.getBean(ProcessService.class);
    }

    /**
     * 主线程 入口
     * @param context
     * @throws JobExecutionException
     */
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {

        Assert.notNull(getProcessService(),"please call init() method first");

        Trigger trigger = context.getTrigger();

        // 当前触发时间
        Date finalFireTime = trigger.getFinalFireTime();

        JobDataMap dataMap = context.getJobDetail().getJobDataMap();

        if (dataMap==null || dataMap.get("className")==null){
            return;
        }
        num = num+1;
        logger.info("execute 执行第 "+num+" 次");

        int projectId = dataMap.getInt(Constants.PROJECT_ID);
        int scheduleId = dataMap.getInt(Constants.SCHEDULE_ID);

        Date scheduledFireTime = context.getScheduledFireTime();

        Date fireTime = context.getFireTime();

        Date previousFireTime = context.getPreviousFireTime();

        logger.info("scheduled fire time :{}, fire time:{}, previousFire time:{}, finalFire time:{}, process id :{}",
                scheduledFireTime,fireTime,previousFireTime,finalFireTime,scheduleId);

        FutureTask<String> future = threadRunner(DependStateCheckExecutor.class.getName(),fireTime,previousFireTime);
        try {
            String resultReport = future.get(28L, TimeUnit.MINUTES);
            //send email
            emailManager.send(getRecipients(),
                    "巡检报表",resultReport, ShowType.MSGTABKE.getDescp());

        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            logger.info("get depend state thread is timeout or other error, please check");
        }

    }

    private List<String> getRecipients() {
        String mailRecipients = org.apache.dolphinscheduler.service.depend.email.utils.Constants.MAIL_RECIPIENTS;
        String recipients = PropertyUtils.getString(mailRecipients);
        logger.info("send to user mail :"+recipients);
        String[] split = StringUtils.split(recipients, ",");
        return Arrays.asList((split==null && recipients!=null) ? new String[]{recipients} :split);
    }


    /**
     * create thread and runner
     * @param className
     * @param fireTime
     * @param nextFireTime
     * @return
     */
    private FutureTask<String> threadRunner(String className, Date fireTime, Date previousFireTime) {
        FutureTask<String> futureTask = null;
        if (DependStateCheckExecutor.class.getName().equals(className)) {
            // new thread
            futureTask = new FutureTask<String>(new DependStateCheckExecutor(fireTime,previousFireTime));
            new Thread(futureTask).start();
        }
        return futureTask;
    }

    /**
     * delete job
     */
    private void deleteJob(int projectId, int scheduleId) {
        String jobName = QuartzExecutors.buildJobName(scheduleId);
        String jobGroupName = QuartzExecutors.buildJobGroupName(projectId);
        QuartzExecutors.getInstance().deleteJob(jobName, jobGroupName);
    }
}
