package org.apache.dolphinscheduler.service.depend;

import org.apache.dolphinscheduler.common.enums.*;
import org.apache.dolphinscheduler.common.utils.DateUtils;
import org.apache.dolphinscheduler.dao.entity.ProcessDefinition;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.vo.depend.DependCheckVo;
import org.apache.dolphinscheduler.dao.entity.vo.depend.DependTreeViewVo;
import org.apache.dolphinscheduler.dao.entity.vo.depend.DependsVo;
import org.apache.dolphinscheduler.dao.mapper.ProcessDefinitionMapper;
import org.apache.dolphinscheduler.service.bean.SpringApplicationContext;
import org.apache.dolphinscheduler.service.depend.pojo.DependsOnSendingMailObj;
import org.apache.dolphinscheduler.service.process.ProcessService;
import org.apache.dolphinscheduler.service.quartz.cron.SchedulingBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;

/**
 * @program: dolphinscheduler
 * @description:
 * @author: Mr.Yang
 * @create: 2022-01-19 17:04
 *      * 新增：
 *      * 1、项目名
 *      * 2、definition和schedule都要是online 才 需要遍历
 *      * 3、周、月、年 调度周期的 判断逻辑调整
 **/
//@Service
public class DependStateCheckExecutor implements Callable<String>{

    /**
     * logger
     */
    private static final Logger logger = LoggerFactory.getLogger(DependStateCheckExecutor.class);

    public static  Integer totalCount = 0;

    public static Integer successCount = 0;

    public static  Integer faildCount = 0;

    public static  Integer execCount = 0;

    public static  Integer unExecCount = 0;

    public static HashSet<Integer> searchedIds = new HashSet<>();

    public static  LinkedHashMap<Integer, Object> faildObjs = new LinkedHashMap<>();

    public static  LinkedHashMap<Integer, Object> execObjs = new LinkedHashMap<>();

    public static  LinkedHashMap<Integer, Object> unExecObjs = new LinkedHashMap<>();

    /**
     * 定时器触发时间和下次触发时间
     */
    private final Date fireTime;
    private final Date previousFireTime;

    private ProcessService processService = SpringApplicationContext.getBean(ProcessService.class);

    private ProcessDefinitionMapper processDefineMapper = SpringApplicationContext.getBean(ProcessDefinitionMapper.class);

    public DependStateCheckExecutor(Date fireTime, Date previousFireTime) {
        this.fireTime = fireTime;
        this.previousFireTime = previousFireTime;
    }


    @Override
    public String call() throws Exception {

        try {
            List<Integer> processIds = processService.queryAllProcessIdByProcessTypeAndReleaseState(ProcessType.SCHEDULER, ReleaseState.ONLINE);

            for (Integer processId : processIds) {
                // 递归版本
                queryDepends(processId, true);
                // loop版本
//                queryDependsByLoop(processId,true);
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
        totalCount = 0;
        successCount = 0;
        faildCount = 0;
        execCount = 0;
        unExecCount = 0;
        searchedIds.clear();
        faildObjs.clear();
        execObjs.clear();
        unExecObjs.clear();
    }

    private String buildDependReportStr() {
        return new DependsOnSendingMailObj(
                totalCount,
                successCount,
                faildCount,
                execCount,
                unExecCount,
                faildObjs,
                execObjs,
                unExecObjs).toString();

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
                if (ExecutionStatus.SUCCESS == dependsVo.getState()) {
                    // 成功状态需要递归 查询 上下一层依赖
//                    System.out.println("11111111111111111111"+dependsVo.getName()+":: in recursivelyTraverseDepend");
                    totalCount++;
                    successCount++;
                    searchedIds.add(dependsVo.getDefinitionId());
                    queryDepends(dependsVo.getDefinitionId(),true);

                } else if (ExecutionStatus.FAILURE == dependsVo.getState()) {
                    // 失败状态的下游可能存在拉起的情况 此时也会失败 所以需要向下再查一层
                    faildObjs.put(dependsVo.getDefinitionId(), dependsVo);
                    totalCount++;
                    faildCount++;
                    searchedIds.add(dependsVo.getDefinitionId());
                    // 可能被其他节点拉起 下游也失败了 所以需要再查一层
                    queryDepends(dependsVo.getDefinitionId(),true);

                } else if (dependsVo.getState() == null) {
                    unExecObjs.put(dependsVo.getDefinitionId(), dependsVo);
                    totalCount++;
                    unExecCount++;
                    searchedIds.add(dependsVo.getDefinitionId());
                    queryDepends(dependsVo.getDefinitionId(),false);
                } else {
                    execObjs.put(dependsVo.getDefinitionId(), dependsVo);
                    totalCount++;
                    execCount++;
                    searchedIds.add(dependsVo.getDefinitionId());
                    queryDepends(dependsVo.getDefinitionId(),true);
                }
            }
        }
    }

    /**
     * @param processId
     * @param existInstance
     */
    private void queryDependsByLoop(Integer processId, Boolean existInstance) {
        DependTreeViewVo dependTreeViewVo = null;
        // 如果存在实例
        if (existInstance) {
//            ProcessInstance processInstance = processService.queryLastInstanceByProcessId(processId,getNowDateZero());

            // 自上线以来的所有实例中的最新的一个
            ProcessInstance processInstance = processService.queryLastExecInstanceByProcessId(processId);
            // 头节点默认是 存在实例的 但是如果并没有跑过 则 实例为null 重定位到 不存在实例的分支
            if (processInstance==null){
                logger.info("processInstance wei null "+processId);
                queryDependsByLoop(processId,false);
            }
            int interval = processInstance.getSchedulerInterval();
            Date scheduleTime = processInstance.getScheduleTime();
            boolean needRunCheckDepend = needCheckDepend(interval,getNowDate(),scheduleTime);

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
                queryDependsByLoop(processId, false);
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
                    if (ExecutionStatus.SUCCESS == dependsVo.getState()) {
                        // 成功状态需要递归 查询 上下一层依赖
//                    System.out.println("11111111111111111111"+dependsVo.getName()+":: in recursivelyTraverseDepend");
                        totalCount++;
                        successCount++;
                        searchedIds.add(dependsVo.getDefinitionId());
                        dependAddStack(dependsVo.getDefinitionId(),true,stack);
                    } else if (ExecutionStatus.FAILURE == dependsVo.getState()) {
                        // 失败状态的下游可能存在拉起的情况 此时也会失败 所以需要向下再查一层
                        // 获取项目名
                        String projectName = processDefineMapper.queryProjectNameBydefinitionId(dependsVo.getDefinitionId());
                        faildObjs.put(dependsVo.getDefinitionId(), new DependCheckVo(dependsVo,projectName));
                        totalCount++;
                        faildCount++;
                        searchedIds.add(dependsVo.getDefinitionId());
                        // 可能被其他节点拉起 下游也失败了 所以需要再查一层
                        dependAddStack(dependsVo.getDefinitionId(),true,stack);

                    } else if (dependsVo.getState() == null) {
                        // 获取项目名
                        String projectName = processDefineMapper.queryProjectNameBydefinitionId(dependsVo.getDefinitionId());
                        unExecObjs.put(dependsVo.getDefinitionId(), new DependCheckVo(dependsVo,projectName));
                        totalCount++;
                        unExecCount++;
                        searchedIds.add(dependsVo.getDefinitionId());
                        dependAddStack(dependsVo.getDefinitionId(),false,stack);
                    } else {
                        // 获取项目名
                        String projectName = processDefineMapper.queryProjectNameBydefinitionId(dependsVo.getDefinitionId());
                        execObjs.put(dependsVo.getDefinitionId(), new DependCheckVo(dependsVo,projectName));
                        totalCount++;
                        execCount++;
                        searchedIds.add(dependsVo.getDefinitionId());
                        dependAddStack(dependsVo.getDefinitionId(),true,stack);
                    }
                }
            }
        }
    }

    private boolean needCheckDepend(int interval, Date nowDate, Date scheduleTime) {
        boolean needCheckDepend = false;
        switch (interval){
            case 0:
                logger.warn("don't support minute depend check interval");
                // 调度的时间 是 当前的小时数
//                if (previousFireTime.getHours()==fireTime.getHours()) {
//                    // 10:00 10:30 10:45 9:45
//                    if (scheduleTime.getHours()!=previousFireTime.getHours()) {
//                        long diffMin = DateUtils.diffMin(previousFireTime, fireTime);
//                        Date lastPreFireTime = DateUtils.getOffsetMin(previousFireTime, diffMin);
//                        // 上上次调度的日期在scheduleTime之后 说明已经对当条实例进行过 check
//                        if (lastPreFireTime.before(scheduleTime)){
//                            needCheck = false;
//                        } else {
//                            needCheck = true;
//                        }
//                    }
//                    // 两次调度时间之间 跨小时 check 上小时的实例
//                } else {
//                    if (scheduleTime.getHours()==previousFireTime.getHours()){
//                        needCheck = true;
//
//                    } else if (scheduleTime.getHours()==fireTime.getHours()){
//                        needCheck = true;
//                    }
//                }
                break;
            case 1:
                long diffHours = DateUtils.diffHours(fireTime, previousFireTime);
                // 调度时间在上次调度时间之后
                needCheckDepend = scheduleTime.before(previousFireTime);
                break;
            case 2:
//                needCheckDepend = fireTime.getDay()==scheduleTime.getDay();
                needCheckDepend = scheduleTime.after(previousFireTime);
                break;
            case 3:
//                needCheckDepend = fireTime.getDay()==scheduleTime.getDay();
                needCheckDepend = scheduleTime.after(previousFireTime);
                break;
            case 4:
//                needCheckDepend = fireTime.getDay()==scheduleTime.getDay();
                needCheckDepend = scheduleTime.after(previousFireTime);
                break;
            case 5:
                logger.warn("don't support year depend check interval");
                break;
            default:
                logger.warn("dont't support interval to check depend ...");
                break;
        }
        return needCheckDepend;
    }

    // loop depend version
    private void dependAddStack(Integer processId, Boolean existInstance,Stack<DependsVo> stack){
        DependTreeViewVo dependTreeViewVo = null;
        if (existInstance) {
            // 自上线以来的所有实例中的最新的一个
            ProcessInstance processInstance = processService.queryLastExecInstanceByProcessId(processId);
            int interval = processInstance.getSchedulerInterval();
            Date scheduleTime = processInstance.getScheduleTime();
            boolean needRunCheckDepend = needCheckDepend(interval,getNowDate(),scheduleTime);

            // start process 非null 过滤
            if (processInstance!=null && needRunCheckDepend){
                setStartProcessState(processInstance);
                // start 节点创建实例失败
                SchedulingBatch sb = new SchedulingBatch(processInstance);
                dependTreeViewVo = newDependTreeView(processInstance, DependentViewRelation.ONE_ALL);
                // 因为是遍历出了所有的start节点，所以无需考虑有多个上游的情况，会充分遍历到所有节点。
                // 所以只需要查询出需要child 依赖即可
                processInstance = null;
                processService.queryChildDepends(sb, processId, dependTreeViewVo);
            } else {// start节点没有运行 直接查询definition构造depend
                logger.info("processInstance is null because start process id not be scheduled");
                processInstance = null;
                queryDependsByLoop(processId, false);
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

            // 递归出口 上层或者下层的依赖列表为空的时候 return
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
        String projectName = processDefineMapper.queryProjectNameBydefinitionId(definitionId);
        DependCheckVo dependCheckVo = new DependCheckVo(new DependsVo(processId, definitionId, name, state, "scheduler"),projectName);

        Boolean isExist = false;
        Iterator<Integer> iterator = searchedIds.iterator();
        while(iterator.hasNext()){
            if (definitionId.equals(iterator.next())) {
                isExist = true;
                break;
            }
        }

        if (!isExist){
            if (ExecutionStatus.SUCCESS == state) {
//                System.out.println("11111111111111111111"+name+":: in setStartProcessState");
                totalCount++;
                successCount++;
            } else if (ExecutionStatus.FAILURE == state) {
                faildObjs.put(definitionId, dependCheckVo);
                totalCount++;
                faildCount++;
            } else if (dependCheckVo.getState() == null) {
                unExecObjs.put(definitionId, dependCheckVo);
                totalCount++;
                unExecCount++;
            } else {
                execObjs.put(definitionId, dependCheckVo);
                totalCount++;
                execCount++;
            }
            searchedIds.add(definitionId);
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