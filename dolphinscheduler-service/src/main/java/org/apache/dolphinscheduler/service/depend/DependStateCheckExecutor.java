package org.apache.dolphinscheduler.service.depend;

import org.apache.dolphinscheduler.common.enums.*;
import org.apache.dolphinscheduler.common.utils.DateUtils;
import org.apache.dolphinscheduler.dao.entity.ProcessDefinition;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.vo.depend.DependCheckVo;
import org.apache.dolphinscheduler.dao.entity.vo.depend.DependTreeViewVo;
import org.apache.dolphinscheduler.dao.entity.vo.depend.DependsVo;
import org.apache.dolphinscheduler.dao.mapper.ProcessDefinitionMapper;
import org.apache.dolphinscheduler.dao.mapper.ProcessInstanceMapper;
import org.apache.dolphinscheduler.service.bean.SpringApplicationContext;
import org.apache.dolphinscheduler.service.depend.enums.IntervalType;
import org.apache.dolphinscheduler.service.depend.pojo.DependSendMail;
import org.apache.dolphinscheduler.service.depend.pojo.DependSendMailBase;
import org.apache.dolphinscheduler.service.depend.pojo.DependsOnSendingMailObj;
import org.apache.dolphinscheduler.service.process.ProcessService;
import org.apache.dolphinscheduler.service.quartz.cron.CronUtils;
import org.apache.dolphinscheduler.service.quartz.cron.SchedulingBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.*;
import java.util.concurrent.Callable;

import static org.apache.dolphinscheduler.common.enums.ExecutionStatus.*;
import static org.apache.dolphinscheduler.common.enums.ExecutionStatus.SUCCESS;

/**
 * @program: dolphinscheduler
 * @description:
 * @author: Mr.Yang
 * @create: 2022-01-19 17:04
 *      * 新增：
 *      * 1、项目名
 *      * 2、definition和schedule都要是online 才 需要遍历
 *      * 3、周、月、年 调度周期的 判断逻辑调整
 *      * 4、修改msg 返回的值 重构 返回的msg对象 hour调度周期 和 其他调度周期分开 显示
 **/
//@Service
//@Component
public class DependStateCheckExecutor implements Callable<String>{

    /**
     * logger
     */
    private static final Logger logger = LoggerFactory.getLogger(DependStateCheckExecutor.class);

    private static final DependSendMailBase defaultMail = new DependSendMailBase(IntervalType.DEFAULT);

    private static final DependSendMail hourMail = new DependSendMail(IntervalType.HOUR);

    private static final DependSendMail dayMail = new DependSendMail(IntervalType.DAY);

    private static final DependSendMail weekMail = new DependSendMail(IntervalType.WEEK);

    private static final DependSendMail monthMail = new DependSendMail(IntervalType.MONTH);

    public static HashSet<Integer> searchedIds = new HashSet<>();

    /**
     * 当前process的调度周期 默认default
     */
    private IntervalType intervalType = IntervalType.DEFAULT;

    /**
     * 定时器触发时间和下次触发时间
     */
    private Date fireTime;
    private Date previousFireTime;
    private Date nextFireTime;

    /**
     * 每个schedule process 的cron
     */
    private String cron;

    /**
     * 子节点如果有设置定时的话
     */
    private String subCron = null;

    private final ProcessService processService = SpringApplicationContext.getBean(ProcessService.class);

    private final ProcessInstanceMapper processInstanceMapper = SpringApplicationContext.getBean(ProcessInstanceMapper.class);

    private final ProcessDefinitionMapper processDefineMapper = SpringApplicationContext.getBean(ProcessDefinitionMapper.class);

    public DependStateCheckExecutor(Date fireTime) {
        this.fireTime = fireTime;
    }

    public DependStateCheckExecutor() {
    }

    private ProcessService getProcessService(){
        return SpringApplicationContext.getBean(ProcessService.class);
    }


    @Override
    public String call() throws Exception {

        try {
            List<Integer> processIds = processService.queryAllProcessIdByProcessTypeAndReleaseState(ProcessType.SCHEDULER, ReleaseState.ONLINE);

            for (Integer processId : processIds) {
                setIntervalType(IntervalType.DEFAULT);
                // 递归版本
//                queryDepends(processId, true);
                // loop版本
                cron = getProcessService().queryCronByProcessDefinitionId(processId);
                queryDependsByLoop(processId,true,null);
            }
            String reportObj = buildDependReportStr();

            clearCache();
            return reportObj;
        }catch (RuntimeException e) {
            e.printStackTrace();
            logger.warn("DependStateCheckExecutor thread : {}",e.toString());
            clearCache();
            return null;
        }
    }

    private void clearCache() {
        searchedIds.clear();
        defaultMail.clear();
        hourMail.clear();
        dayMail.clear();
        weekMail.clear();
        monthMail.clear();
    }

    private String buildDependReportStr() {
        String defaultMsg = new DependsOnSendingMailObj(defaultMail).toString();
        StringBuffer bf = new StringBuffer().append(defaultMsg);

        if (hourMail.getTotalCount()!=0){
            String hourMsg = new DependsOnSendingMailObj(hourMail).toString();
            bf.append(DependsOnSendingMailObj.delimiterHour()).append(hourMsg);
        }

        if (dayMail.getTotalCount()!=0){
            String dayMsg = new DependsOnSendingMailObj(dayMail).toString();
            bf.append(DependsOnSendingMailObj.delimiterDay()).append(dayMsg);
        }

        if (weekMail.getTotalCount()!=0){
            String weekMsg = new DependsOnSendingMailObj(weekMail).toString();
            bf.append(DependsOnSendingMailObj.delimiterWeek()).append(weekMsg);
        }

        if (monthMail.getTotalCount()!=0){
            String monthMsg = new DependsOnSendingMailObj(monthMail).toString();
            bf.append(DependsOnSendingMailObj.delimiterMonth()).append(monthMsg);
        }
        System.out.println(bf);
        return bf.toString();
    }

    // recursion depend version
    private void queryDepends(Integer processId, Boolean existInstance){
        DependTreeViewVo dependTreeViewVo = null;
        if (existInstance) {
            ProcessInstance processInstance = processService.queryLastInstanceByProcessId(processId,getNowDateZero());
            // start process 非null 过滤
            if (processInstance!=null){
                setStartProcessState(processInstance);
                // start 节点创建实例失败
                SchedulingBatch sb = new SchedulingBatch(processInstance);
                dependTreeViewVo = newDependTreeView(processInstance, DependentViewRelation.ONE_ALL);

                // 因为是遍历出了所有的start节点，所以无需考虑有多个上游的情况，会充分遍历到所有节点。
                // 所以只需要查询出需要child 依赖即可
                processService.queryChildDepends(sb, processId, dependTreeViewVo);
            } else {// start节点没有运行 直接查询definition构造depend
                logger.info("processInstance is null because start process id not be scheduled");
                queryDepends(processId, false);
            }
        } else {
            ProcessDefinition processDefinition = processDefineMapper.selectById(processId);
            if (processDefinition!=null){
                setStartProcessState(processDefinition);

                dependTreeViewVo = newDependTreeView(processDefinition, DependentViewRelation.ONE_ALL);

                int id = processDefinition.getId();
                processService.queryChildDepends(null, id, dependTreeViewVo);
            } else {
                logger.info("processDefinition is null , because inconsistent with dependency table");
            }
        }

        if (dependTreeViewVo!=null) {
            List<DependsVo> childs = dependTreeViewVo.getChilds();

            // 递归出口 上层或者下层的依赖列表为空的时候 return
            // 如果是未执行的工作流 此时状态为null 但是当次查询的时候sql依然会查出，只是并未生成实例，需要读取depend表的依赖信息并列出下游的依赖
            if (isDependEntry(childs)) {
                return;
            }
            // 递归遍历依赖
            recursivelyTraverseDepend(childs);
        }
    }

    // 递归遍历
    private void recursivelyTraverseDepend(List<DependsVo> dependsList) {
        for (DependsVo dependsVo : dependsList) {
            //不存在DefinitionId 在 遍历过的set中 则继续遍历
            if (!isExistDefinitionId(dependsVo)){
                if (SUCCESS == dependsVo.getState()) {
                    // 成功状态需要递归 查询 上下一层依赖
//                    System.out.println("11111111111111111111"+dependsVo.getName()+":: in recursivelyTraverseDepend");
                    defaultMail.addTotalCount(1);
                    defaultMail.addSuccessCount(1);
                    searchedIds.add(dependsVo.getDefinitionId());
                    queryDepends(dependsVo.getDefinitionId(),true);

                } else if (FAILURE == dependsVo.getState()) {
                    // 失败状态的下游可能存在拉起的情况 此时也会失败 所以需要向下再查一层
//                    defaultMail.addFaildObjs(dependsVo.getDefinitionId(), dependsVo);
                    defaultMail.addTotalCount(1);
                    defaultMail.addFaildCount(1);
                    searchedIds.add(dependsVo.getDefinitionId());
                    // 可能被其他节点拉起 下游也失败了 所以需要再查一层
                    queryDepends(dependsVo.getDefinitionId(),true);

                } else if (dependsVo.getState() == null) {
//                    defaultMail.addUnExecObjs(dependsVo.getDefinitionId(), dependsVo);
                    defaultMail.addTotalCount(1);
                    defaultMail.addUnExecCount(1);
                    searchedIds.add(dependsVo.getDefinitionId());
                    queryDepends(dependsVo.getDefinitionId(),false);
                } else {
//                    defaultMail.addExecObjs(dependsVo.getDefinitionId(), dependsVo);
                    defaultMail.addTotalCount(1);
                    defaultMail.addExecCount(1);
                    searchedIds.add(dependsVo.getDefinitionId());
                    queryDepends(dependsVo.getDefinitionId(),true);
                }
            }
        }
    }

    /**
     * 实例存在 -> 查上线以来最晚的一个process -> 获取cron -> 无process 进入实例不存在分支
     *                                              -> 有process 查询周期 -> 判断是否在这个周期内有运行过的实例 ，且为hour 标记
     *                                                  ->运行过实例 -> 进入状态check判断
     *                                                  ->没运行实例 -> 进入unexec分支
     * @param processId
     * @param existInstance
     */
    private void queryDependsByLoop(Integer processId, Boolean existInstance,IntervalType intervalType) throws ParseException {
        DependTreeViewVo dependTreeViewVo = null;

        if (intervalType!=null) {setIntervalType(intervalType);}

        // 如果存在实例
        if (existInstance) {

            // 自上线以来的所有实例中的最新的一个
            ProcessInstance processInstance = processService.queryLastExecInstanceByProcessId(processId);

            String crontab = getProcessCrontab(cron,processId);
            // 头节点默认是 存在实例的 但是如果并没有跑过 则 实例为null 重定位到 不存在实例的if分支
            if (processInstance==null){
                logger.info("processInstance wei null "+processId);
                queryDependsByLoop(processId,false,this.intervalType);
                return;
            }
            int interval = processInstance.getSchedulerInterval();
            Date scheduleTime = processInstance.getScheduleTime();

            try {
                boolean needRunCheckDepend = needCheckDepend(interval,scheduleTime,crontab);

                // (start process 非null) && 最新的调度周期有运行实例 过滤
                if (processInstance!=null && needRunCheckDepend){
                    setStartProcessState(processInstance);
                    // start 节点创建实例失败
                    SchedulingBatch sb = new SchedulingBatch(processInstance);
                    dependTreeViewVo = newDependTreeView(processInstance, DependentViewRelation.ONE_ALL);
                    // 因为是遍历出了所有的start节点，所以无需考虑有多个上游的情况，会充分遍历到所有节点。
                    // 所以只需要查询出需要child 依赖即可
                    processInstance = null;
                    processService.queryChildDepends(sb, processId, dependTreeViewVo);
                } else {// start节点没有运行 || start运行过但最新的调度周期没有运行实例 直接查询definition构造depend
                    logger.info("processInstance is null because start process id not be scheduled");
                    processInstance = null;
                    queryDependsByLoop(processId, false,this.intervalType);
                }
            } catch (RuntimeException e) {
                logger.warn(e.getMessage());
            }
            // 实例不存在 通过definition构建 查一层依赖
        } else {
            ProcessDefinition processDefinition = processDefineMapper.selectById(processId);
            if (processDefinition!=null){
                setStartProcessState(processDefinition);

                dependTreeViewVo = newDependTreeView(processDefinition, DependentViewRelation.ONE_ALL);

                int id = processDefinition.getId();
                processDefinition = null;
                processService.queryChildDepends(null, id, dependTreeViewVo);
            } else {
                logger.info("processDefinition is null , because inconsistent with dependency table");
            }
        }

        if (dependTreeViewVo!=null) {
            List<DependsVo> childs = dependTreeViewVo.getChilds();

            // 递归出口 上层或者下层的依赖列表为空的时候 return
            // 如果是未执行的工作流 此时状态为null 但是当次查询的时候sql依然会查出，只是并未生成实例，需要读取表depend的依赖信息并列出下游的依赖
            if (isDependEntry(childs)) {
                return;
            }
            // 置为null 释放内存
            dependTreeViewVo = null;
            // 递归遍历依赖
            Stack<DependsVo> stack = new Stack<>();
            childs.forEach(stack::push);
            while (!stack.isEmpty()){
                DependsVo dependsVo = stack.pop();

                //不存在DefinitionId 在 遍历过的set中 则继续遍历
                if (!isExistDefinitionId(dependsVo)){

                    if (this.intervalType==IntervalType.DAY) {
                        logger.info("intervalType:{},dependVo:{}","day",dependsVo);
                        addSendMail(dayMail,dependsVo,stack);
                    } else if (this.intervalType==IntervalType.HOUR) {
                        logger.info("intervalType:{},dependVo:{}","hour",dependsVo);
                        addSendMail(hourMail,dependsVo,stack);
                    } else if (this.intervalType==IntervalType.WEEK) {
                        logger.info("intervalType:{},dependVo:{}","week",dependsVo);
                        addSendMail(weekMail,dependsVo,stack);
                    } else if (this.intervalType==IntervalType.MONTH) {
                        logger.info("intervalType:{},dependVo:{}","month",dependsVo);
                        addSendMail(weekMail,dependsVo,stack);
                    }
                }
            }
        }
    }

    private String getProcessCrontab(String cron, Integer processId) {
//        subCron = getProcessService().queryCronByProcessDefinitionId(processId);
//        if (cron.equals(subCron)){
//            return cron;
//        }
//        return subCron;
        return cron;
    }

    private boolean needCheckDepend(int interval, Date scheduleTime, String crontab) throws ParseException {
        logger.info("22222222222222222222222222222222222");
//        logger.info("interval:{},scheduleTime:{},crontab:{}",interval,scheduleTime.toString(),crontab);
        // 调度时间的下一次的cron 表达式生成的调度时间
        boolean needCheckDepend = false;
        Date nextScheduleTime = null;
        try{
            nextScheduleTime = CronUtils.nextExecDate(crontab,scheduleTime);
        }catch (RuntimeException e){
            logger.info("error of get nextExecDate");
            logger.warn("error",e);
        }

        // 获取调度时间的间隔 last 10.00 next 13.00 触发时间 10.00.20 check 10.00-9.30 之间触发过 需要check 没有 则不需
        //                      9.30        10.30        10.00.20
        logger.info("scheduleTime:{},nextScheduleTime:{}",scheduleTime,nextScheduleTime);

        switch (CycleEnum.valueOf(interval)){
            case MINUTE:
                throw new RuntimeException("don't support minute depend check interval");
            case HOUR:
                // 下一次触发的时间在当前check 时间之前 则表示 有一次cron 定时任务未触发 返回 false
                needCheckDepend = !nextScheduleTime.before(fireTime);
                setIntervalType(IntervalType.HOUR);
                logger.info("set hour interval success");
                break;
            case DAY:
                needCheckDepend = !nextScheduleTime.before(fireTime);
                setIntervalType(IntervalType.DAY);
                logger.info("set day interval success");
                break;
            case WEEK:
                needCheckDepend = !nextScheduleTime.before(fireTime);
                setIntervalType(IntervalType.WEEK);
                logger.info("set week interval success");
                break;
            case MONTH:
                needCheckDepend = !nextScheduleTime.before(fireTime);
                setIntervalType(IntervalType.MONTH);
                logger.info("set month interval success");
                break;
            case YEAR:
                throw new RuntimeException("don't support year depend check interval");
            default:
                throw new RuntimeException("dont't support interval to check depend ...");
        }
        return needCheckDepend;
    }

    /**
     * 获取上一次的 processInstance 调度时间 如果是首次调度 则根据下一次的定期调度的时间 和当次调度时间的 时间间隔 计算出 模拟的 上一次的调度时间
     * 不支持非固定周期的调度时间
     * @param dates
     * @param nowDate
     * @param cronInterval
     * @param scheduleTime
     * @return
     */
    private Date getPreFireTime(List<Date> dates, Date nowDate, Long cronInterval, Date scheduleTime) {

        Date nextFireDate = dates.get(0);

        Date preFireDate = DateUtils.getOffsetMin(nextFireDate, -cronInterval);

        Date fireDate;

        if (nowDate.equals(preFireDate)) {
            fireDate = preFireDate;
            preFireDate = DateUtils.getOffsetMin(fireDate,-cronInterval);
        } else {
            fireDate = dates.get(0);
            nextFireDate = dates.get(1);
        }

        logger.info("processInstance crontab preFire time:{}, fire time:{}, next time:{} schedule time:{}",preFireDate,fireDate,nextFireDate,scheduleTime);

        return preFireDate;
    }

    /**
     * 获取上一次的 check depend 调度时间 如果是首次调度 则根据下一次的定期调度的时间 和当次调度时间的 时间间隔 计算出 模拟的 上一次的调度时间
     * @return
     */
    private Date getPreFireTime() {
        if (previousFireTime!=null){
            return previousFireTime;
        } else {
            long diffMin = DateUtils.diffMin(fireTime, nextFireTime);
            return DateUtils.getOffsetMin(fireTime, -diffMin);
        }
    }

    private void setIntervalType(IntervalType intervalType) {
        this.intervalType = intervalType;
    }

    /**
     * loop depend version
     * @param processId
     * @param instanceId
     * @param existInstance
     * @param stack
     */
    private void dependAddStack(Integer processId, Integer instanceId, Boolean existInstance, Stack<DependsVo> stack) throws ParseException {
//        setIntervalType(IntervalType.DEFAULT);
        DependTreeViewVo dependTreeViewVo = null;
        if (existInstance) {

            // 自上线以来的所有实例中的最新的一个
//            ProcessInstance processInstance = processService.queryLastExecInstanceByProcessId(processId);
            // parent instance
            ProcessInstance processInstance = processInstanceMapper.selectById(instanceId);
            // 继承parent 属性
            String crontab = getProcessCrontab(cron,processId);
            int interval = processInstance.getSchedulerInterval();
            Date scheduleTime = processInstance.getScheduleTime();
            logger.info("11111111111111111111111111111111,scheduleTime:{}",scheduleTime);
            try {
                boolean needRunCheckDepend = needCheckDepend(interval,scheduleTime,crontab);

                // start process 非null 过滤
                if (processInstance!=null && needRunCheckDepend){
                    setStartProcessState(processInstance);
                    logger.info("success set start scheduler process state");
                    // start 节点创建实例失败
                    SchedulingBatch sb = new SchedulingBatch(processInstance);
                    dependTreeViewVo = newDependTreeView(processInstance, DependentViewRelation.ONE_ALL);
                    logger.info(dependTreeViewVo.toString());
                    // 因为是遍历出了所有的start节点，所以无需考虑有多个上游的情况，会充分遍历到所有节点。
                    // 所以只需要查询出需要child 依赖即可
                    processInstance = null;
                    processService.queryChildDepends(sb, processId, dependTreeViewVo);
                    logger.info(dependTreeViewVo.toString());
                } else {// start节点没有运行 直接查询definition构造depend
                    logger.info("processInstance is null because start process id not be scheduled");
                    processInstance = null;
                    queryDependsByLoop(processId, false,this.intervalType);
                }
            } catch (RuntimeException e) {
                logger.error("error",e);
                logger.info("process:::{}",processInstance.toString());
            }
        } else {
            ProcessDefinition processDefinition = processDefineMapper.selectById(processId);
            if (processDefinition!=null){
                setStartProcessState(processDefinition);

                dependTreeViewVo = newDependTreeView(processDefinition, DependentViewRelation.ONE_ALL);

                int id = processDefinition.getId();
                processDefinition = null;
                processService.queryChildDepends(null, id, dependTreeViewVo);
            } else {
                logger.info("processDefinition is null , because inconsistent with dependency table");
            }
        }

        if (dependTreeViewVo!=null) {
            List<DependsVo> childs = dependTreeViewVo.getChilds();

            // 置为null 释放内存
            dependTreeViewVo = null;

            // 出口 上层或者下层的依赖列表为空的时候 return
            // 如果是未执行的工作流 此时状态为null 但是当次查询的时候sql依然会查出，只是并未生成实例，需要读取depend表的依赖信息并列出下游的依赖
            if (isDependEntry(childs)) {
                return;
            }

            childs.forEach(stack::push);
        }
    }

    private Date getNowDateZero(){
        // 查询最新的一条数据
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        Date zero = calendar.getTime();
        return zero;
    }

    private Date getNowDate(){
        // 查询最新的一条数据
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        Date zero = calendar.getTime();
        return zero;
    }

    private boolean isDependEntry(List<DependsVo> childs) {
        return childs==null || childs.size()==0;
    }

    private boolean isExistDefinitionId(DependsVo dependsVo) {
        Integer id = dependsVo.getDefinitionId();
        Iterator<Integer> iterator = searchedIds.iterator();
        while(iterator.hasNext()){
            if (id.equals(iterator.next())) {
                return true;
            }
        }
        return false;
    }

    /**
     * 设置初始的process的状态
     * @param process
     */
    private void setStartProcessState(Object process) {

        Integer definitionId = null;
        Integer processId = null;
        String name;
        ExecutionStatus state = null;
        if (process instanceof ProcessInstance){
            ProcessInstance processInstance = (ProcessInstance) process;
            processId = processInstance.getId();
            definitionId = processInstance.getProcessDefinitionId();
            name = processInstance.getName();
            state = processInstance.getState();
        } else {
            ProcessDefinition processDefinition = (ProcessDefinition) process;
            definitionId = processDefinition.getId();
            name = processDefinition.getName();
        }
        logger.info("processId:{},definitionId:{},name:{},state:{}",processId,definitionId,name,state);
        String projectName = processDefineMapper.queryProjectNameBydefinitionId(definitionId);
        logger.info("projectName:{}",projectName);
        DependCheckVo dependCheckVo = new DependCheckVo(new DependsVo(processId, definitionId, name, state, "scheduler"),projectName);
        logger.info("dependCheckVo:{}",dependCheckVo);
        Boolean isExist = false;
        Iterator<Integer> iterator = searchedIds.iterator();
        while(iterator.hasNext()){
            if (definitionId.equals(iterator.next())) {
                isExist = true;
                break;
            }
        }

        if (!isExist){
            if (this.intervalType==IntervalType.DAY) {
                logger.info("intervalType:{},dependCheckVo:{}","day",dependCheckVo);
                addSendMail(dayMail, state, definitionId, dependCheckVo);
            } else if (this.intervalType==IntervalType.HOUR) {
                logger.info("intervalType:{},dependCheckVo:{}","hour",dependCheckVo);
                addSendMail(hourMail, state, definitionId, dependCheckVo);
            } else if (this.intervalType==IntervalType.WEEK) {
                logger.info("intervalType:{},dependCheckVo:{}","week",dependCheckVo);
                addSendMail(weekMail, state, definitionId, dependCheckVo);
            } else if (this.intervalType==IntervalType.MONTH) {
                logger.info("intervalType:{},dependCheckVo:{}","month",dependCheckVo);
                addSendMail(monthMail, state, definitionId, dependCheckVo);
            }
            searchedIds.add(definitionId);
        }
    }

    private void addSendMail(DependSendMail sendMail,ExecutionStatus status,Integer id,DependCheckVo dependCheckVo){

        if (SUCCESS == status) {
            sendMail.addTotalCount(1);
            sendMail.addSuccessCount(1);
            defaultMail.addTotalCount(1);
            defaultMail.addSuccessCount(1);
            searchedIds.add(dependCheckVo.getDefinitionId());
        } else if (FAILURE == status) {
            sendMail.addFaildObjs(id, dependCheckVo);
            sendMail.addTotalCount(1);
            sendMail.addFaildCount(1);
            defaultMail.addTotalCount(1);
            defaultMail.addFaildCount(1);
            searchedIds.add(dependCheckVo.getDefinitionId());
        } else if (status == null) {
            sendMail.addUnExecObjs(id, dependCheckVo);
            sendMail.addTotalCount(1);
            sendMail.addUnExecCount(1);
            defaultMail.addTotalCount(1);
            defaultMail.addUnExecCount(1);
            searchedIds.add(dependCheckVo.getDefinitionId());
        } else {
            sendMail.addExecObjs(id, dependCheckVo);
            sendMail.addTotalCount(1);
            sendMail.addExecCount(1);
            defaultMail.addTotalCount(1);
            defaultMail.addExecCount(1);
            searchedIds.add(dependCheckVo.getDefinitionId());
        }
    }

    private void addSendMail(DependSendMail sendMail, DependsVo dependsVo, Stack<DependsVo> stack) throws ParseException {
            if (SUCCESS == dependsVo.getState()) {
                // 成功状态需要递归 查询 上下一层依赖
//                    System.out.println("11111111111111111111"+dependsVo.getName()+":: in recursivelyTraverseDepend");
                sendMail.addTotalCount(1);
                sendMail.addSuccessCount(1);
                defaultMail.addTotalCount(1);
                defaultMail.addSuccessCount(1);
                searchedIds.add(dependsVo.getDefinitionId());
                dependAddStack(dependsVo.getDefinitionId(), dependsVo.getProcessId(),true,stack);
            } else if (FAILURE == dependsVo.getState()) {
                // 失败状态的下游可能存在拉起的情况 此时也会失败 所以需要向下再查一层
                // 获取项目名
                String projectName = processDefineMapper.queryProjectNameBydefinitionId(dependsVo.getDefinitionId());
                sendMail.addFaildObjs(dependsVo.getDefinitionId(), new DependCheckVo(dependsVo,projectName));
                sendMail.addTotalCount(1);
                sendMail.addFaildCount(1);
                defaultMail.addTotalCount(1);
                defaultMail.addFaildCount(1);
                searchedIds.add(dependsVo.getDefinitionId());
                // 可能被其他节点拉起 下游也失败了 所以需要再查一层
                dependAddStack(dependsVo.getDefinitionId(), dependsVo.getProcessId(),true,stack);

            } else if (dependsVo.getState() == null) {
                // 获取项目名
                String projectName = processDefineMapper.queryProjectNameBydefinitionId(dependsVo.getDefinitionId());
                sendMail.addUnExecObjs(dependsVo.getDefinitionId(), new DependCheckVo(dependsVo,projectName));
                sendMail.addTotalCount(1);
                sendMail.addUnExecCount(1);
                defaultMail.addTotalCount(1);
                defaultMail.addUnExecCount(1);
                searchedIds.add(dependsVo.getDefinitionId());
                dependAddStack(dependsVo.getDefinitionId(), dependsVo.getProcessId(),false,stack);
            } else {
                // 获取项目名
                String projectName = processDefineMapper.queryProjectNameBydefinitionId(dependsVo.getDefinitionId());
                sendMail.addExecObjs(dependsVo.getDefinitionId(), new DependCheckVo(dependsVo,projectName));
                sendMail.addTotalCount(1);
                sendMail.addExecCount(1);
                defaultMail.addTotalCount(1);
                defaultMail.addExecCount(1);
                searchedIds.add(dependsVo.getDefinitionId());
                dependAddStack(dependsVo.getDefinitionId(), dependsVo.getProcessId(),true,stack);
            }

    }


    private DependTreeViewVo newDependTreeView(ProcessInstance processInstance, DependentViewRelation dependentViewRelation) {
        return new DependTreeViewVo(
                processInstance.getId(),
                processInstance.getProcessDefinitionId(),
                DependViewType.PROCESS_INSTANCE.getCode(),
                processInstance.getName(),
                processInstance.getState(),
                dependentViewRelation);
    }

    private DependTreeViewVo newDependTreeView(ProcessDefinition processDefinition, DependentViewRelation dependentViewRelation) {
        return new DependTreeViewVo(
                null,
                processDefinition.getId(),
                DependViewType.PROCESS_INSTANCE.getCode(),
                processDefinition.getName(),
                null,
                dependentViewRelation);
    }
}