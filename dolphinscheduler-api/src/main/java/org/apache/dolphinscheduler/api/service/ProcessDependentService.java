package org.apache.dolphinscheduler.api.service;

import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.exceptions.ProcessDependentSqlException;
import org.apache.dolphinscheduler.common.enums.TaskType;
import org.apache.dolphinscheduler.common.model.DependentItem;
import org.apache.dolphinscheduler.common.model.DependentTaskModel;
import org.apache.dolphinscheduler.common.model.TaskNode;
import org.apache.dolphinscheduler.common.task.dependent.DependentParameters;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.entity.ProcessData;
import org.apache.dolphinscheduler.dao.entity.ProcessDefinition;
import org.apache.dolphinscheduler.dao.entity.ProcessDependent;
import org.apache.dolphinscheduler.dao.mapper.ProcessDependentMapper;
import org.apache.poi.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class ProcessDependentService extends BaseService{

    private static final Logger logger = LoggerFactory.getLogger(ProcessDependentService.class);

    private static final String depTask = "ALL";

    @Autowired
    private ProcessDependentMapper processDependentMapper;

    public Map<String, Object> createProcessDependent(ProcessDefinition processDefinition) {
        Map<String, Object> result = new HashMap<>(5);
        List<DependentItem> dependentItems = getProcessDependent(processDefinition);
        List<ProcessDependent> processDependents = createProcessDependent(processDefinition.getId(), dependentItems);

        if (!processDependents.isEmpty()) {
            if (processDependentMapper.insertBatch(processDependents) == processDependents.size()) {
                logger.info(String.format("create the dag dependency relation; processId = %s, dependent = %s",
                        processDefinition.getId(), StringUtil.join(processDependents.toArray(), ",")));
                putMsg(result, Status.SUCCESS);
            } else {
                logger.error(String.format("failed to batch insert the process dependents info; " +
                        "processId= %s, dependentId= %s", processDefinition.getId(),
                        StringUtil.join(processDependents.toArray(), ",")));
                throw new ProcessDependentSqlException("failed to batch insert the process dependents info");
            }
        } else {
            putMsg(result, Status.SUCCESS);
        }
        return result;
    }

    public Map<String, Object> updateProcessDependent(ProcessDefinition processDefinition) {
        Map<String, Object> result = new HashMap<>(5);
        List<DependentItem> dependentItems = getProcessDependent(processDefinition);
        List<ProcessDependent> processDependents = createProcessDependent(processDefinition.getId(), dependentItems);
        processDependentMapper.deleteByProcessId(processDefinition.getId());
        logger.info(String.format("clear the dag dependency relation of process; processId=%s", processDefinition.getId()));
        if (!processDependents.isEmpty()) {
            if (processDependentMapper.insertBatch(processDependents) == processDependents.size()) {
                logger.info(String.format("create the dag dependency relation; processId = %s, dependent = %s",
                        processDefinition.getId(), StringUtil.join(processDependents.toArray(), ",")));
                putMsg(result, Status.SUCCESS);
            } else {
                logger.error(String.format("failed to batch insert the process dependents info; " +
                                "processId= %s, dependentId= %s", processDefinition.getId(),
                        StringUtil.join(processDependents.toArray(), ",")));
                throw new ProcessDependentSqlException("failed to batch insert the process dependents info");
            }
        } else {
            putMsg(result, Status.SUCCESS);
        }
        return result;
    }

    public int deleteProcessDependentByProcessId(int processId) {
        return processDependentMapper.deleteByProcessId(processId);
    }

    public boolean hasProcessDependent(int processId) {
        List<ProcessDependent> processDependents =  processDependentMapper.queryOneDataByDependentId(processId);
        return processDependents.size() == 1;
    }

    private List<DependentItem> getProcessDependent(ProcessDefinition processDefinition) {
        List<DependentItem> dependentItems = new ArrayList<>();
        ProcessData processData = JSONUtils.parseObject(processDefinition.getProcessDefinitionJson(), ProcessData.class);
        List<TaskNode> taskNodes = processData.getTasks();
        for (TaskNode taskNode : taskNodes) {
            if (TaskType.DEPENDENT.getDescp().equalsIgnoreCase(taskNode.getType())) {
                DependentParameters dependentParameters =
                        JSONUtils.parseObject(taskNode.getDependence(), DependentParameters.class);
                for (DependentTaskModel dependentTaskModel: dependentParameters.getDependTaskList()) {
                    dependentItems.addAll(dependentTaskModel.getDependItemList());
                }
            }
        }
        return dependentItems;
    }

    private List<ProcessDependent> createProcessDependent(Integer processId, List<DependentItem> dependentItems) {
        List<ProcessDependent> processDependents = new ArrayList<>();
        for (DependentItem dependentItem : dependentItems) {
            if (!depTask.equals(dependentItem.getDepTasks())) {
                continue;
            }
            ProcessDependent processDependent = new ProcessDependent();
            processDependent.setDependentId(dependentItem.getDefinitionId());
            processDependent.setProcessId(processId);
            processDependent.setCreateTime(new Date());
            processDependent.setUpdateTime(new Date());
            if (!processDependents.contains(processDependent)) {
                processDependents.add(processDependent);
            }
        }
        return processDependents;
    }



}
