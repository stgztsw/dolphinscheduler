package org.apache.dolphinscheduler.service.depend;

import org.apache.dolphinscheduler.common.enums.*;
import org.apache.dolphinscheduler.dao.entity.ProcessDefinition;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.vo.depend.DependTreeViewVo;
import org.apache.dolphinscheduler.dao.entity.vo.depend.DependsVo;
import org.apache.dolphinscheduler.dao.mapper.ProcessDefinitionMapper;
import org.apache.dolphinscheduler.service.bean.SpringApplicationContext;
import org.apache.dolphinscheduler.service.depend.pojo.DependsOnSendingMailObj;
import org.apache.dolphinscheduler.service.depend.zk.ZKDependStateCheckClient;
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

    private ProcessService processService = SpringApplicationContext.getBean(ProcessService.class);

    private ProcessDefinitionMapper processDefineMapper = SpringApplicationContext.getBean(ProcessDefinitionMapper.class);

    private ZKDependStateCheckClient zkDependStateCheckClient = SpringApplicationContext.getBean(ZKDependStateCheckClient.class);


    @Override
    public String call() throws Exception {

        try {
            List<Integer> processIds = processService.queryAllProcessIdByProcessType(ProcessType.SCHEDULER, ReleaseState.ONLINE);

            for (Integer processId : processIds) {
                queryDepends(processId, true);
            }
            String reportObj = buildDependReportStr();
//            System.out.println(report);

            clearCache();
            return reportObj;
        }catch (RuntimeException e) {
            logger.warn("DependStateCheckExecutor thread : {}",e.toString());
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

    // todo unchecked 需要在单次递归中 将完成对象 被jvm回收
    private void queryDepends(Integer processId, Boolean existInstance){
        DependTreeViewVo dependTreeViewVo = null;
        if (existInstance) {
            Date checkDate = getNowDateZero();
            ProcessInstance processInstance = processService.queryLastInstanceByProcessId(processId,checkDate);
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

                processService.queryChildDepends(null, processDefinition.getId(), dependTreeViewVo);
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

    private boolean isDependEntry(List<DependsVo> childs) {
        return childs==null || childs.size()==0;
    }


    // 递归遍历
    private void recursivelyTraverseDepend(List<DependsVo> dependsList) {
        for (DependsVo dependsVo : dependsList) {
            //不存在DefinitionId 在 遍历过的set中 则继续遍历
            if (!isExistDefinitionId(dependsVo)){
                if (ExecutionStatus.SUCCESS == dependsVo.getState()) {
                    // 成功状态需要递归 查询 上下一层依赖
                    System.out.println("11111111111111111111"+dependsVo.getName()+":: in recursivelyTraverseDepend");
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
        DependsVo dependsVo = new DependsVo(processId, definitionId, name, state, "scheduler");

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
                System.out.println("11111111111111111111"+name+":: in setStartProcessState");
                totalCount++;
                successCount++;
            } else if (ExecutionStatus.FAILURE == state) {
                faildObjs.put(definitionId, dependsVo);
                totalCount++;
                faildCount++;
            } else if (dependsVo.getState() == null) {
                unExecObjs.put(definitionId, dependsVo);
                totalCount++;
                unExecCount++;
            } else {
                execObjs.put(definitionId, dependsVo);
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