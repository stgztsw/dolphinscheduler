package org.apache.dolphinscheduler.service.depend.pojo;

import com.alibaba.fastjson.JSONObject;
import org.apache.dolphinscheduler.service.depend.email.utils.Constants;
import org.apache.dolphinscheduler.service.depend.enums.IntervalType;

import java.util.LinkedHashMap;

/**
 * @program: dolphinscheduler
 * @description:
 * @author: Mr.Yang
 * @create: 2022-01-21 16:34
 **/

public class DependsOnSendingMailObj {
    private IntervalType intervalType;

    private   Integer totalCount = 0;

    private Integer successCount = 0;

    private  Integer faildCount = 0;

    private  Integer execCount = 0;

    private  Integer unExecCount = 0;

    private LinkedHashMap<Integer, Object> faildObjs;

    private  LinkedHashMap<Integer, Object> execObjs;

    private  LinkedHashMap<Integer, Object> unExecObjs;

    public static final String TEXT_Finally_BEGIN = ",[TEXT:";
    public static final String TEXT_BEGIN = "[TEXT:";
    public static final String MID_END = "],";
    public static final String TABLE_BEGIN = "[TABLE:";
    public static final String Finally_END = "]";


    public DependsOnSendingMailObj(Integer totalCount, Integer successCount, Integer faildCount, Integer execCount, Integer unExecCount, LinkedHashMap<Integer, Object> faildObjs, LinkedHashMap<Integer, Object> execObjs, LinkedHashMap<Integer, Object> unExecObjs) {
        this.totalCount = totalCount;
        this.successCount = successCount;
        this.faildCount = faildCount;
        this.execCount = execCount;
        this.unExecCount = unExecCount;
        this.faildObjs = faildObjs;
        this.execObjs = execObjs;
        this.unExecObjs = unExecObjs;
    }

    public DependsOnSendingMailObj(DependSendMailBase dependSendMail) {
        this.intervalType = dependSendMail.getIntervalType();
        this.totalCount = dependSendMail.getTotalCount();
        this.successCount = dependSendMail.getSuccessCount();
        this.faildCount = dependSendMail.getFaildCount();
        this.execCount = dependSendMail.getExecCount();
        this.unExecCount = dependSendMail.getUnExecCount();
        if (dependSendMail instanceof DependSendMail) {
            this.faildObjs = ((DependSendMail) dependSendMail).getFaildObjs();
            this.execObjs = ((DependSendMail) dependSendMail).getExecObjs();
            this.unExecObjs = ((DependSendMail) dependSendMail).getUnExecObjs();
        }
    }

    public String getTotalCount() {
        return String.format("计划运行总数：【%d】",this.totalCount);
    }

    public void setTotalCount(Integer totalCount) {
        this.totalCount = totalCount;
    }

    public String getSuccessCount() {
        return String.format("成功运行总数：【%d】",this.successCount);
    }

    public String getFaildCount() {
        return String.format("调度失败总数：【%d】",this.faildCount);
    }

    public String getExecCount() {
        return String.format("运行中总数：【%d】",this.execCount);
    }

    public String getUnExecCount() {
        return String.format("待运行总数：【%d】",this.unExecCount);
    }

    public LinkedHashMap<Integer, Object> getFaildObjs() {
        return faildObjs;
    }

    public LinkedHashMap<Integer, Object> getExecObjs() {
        return execObjs;
    }

    public LinkedHashMap<Integer, Object> getUnExecObjs() {
        return unExecObjs;
    }

    @Override
    public String toString() {

        StringBuffer bf = new StringBuffer();
        if (IntervalType.DEFAULT==this.intervalType){
            bf.append(delimiterDefault());
        }
        bf
                .append(TEXT_BEGIN).append(getTotalCount()).append(MID_END)
                .append(TEXT_BEGIN).append(getSuccessCount()).append(MID_END);

                bf.append(TEXT_BEGIN).append(getFaildCount()).append(MID_END);
                if (getFaildObjs()!=null && getFaildObjs().size()!=0) {
                    bf.append(TEXT_BEGIN).append("失败详情：").append(MID_END)
                    .append(TABLE_BEGIN).append(JSONObject.toJSONString(getFaildObjs())).append(MID_END);
                }

                bf.append(TEXT_BEGIN).append(getExecCount()).append(MID_END);
                if (getExecObjs()!=null && getExecObjs().size()!=0) {
                    bf.append(TEXT_BEGIN).append("运行中详情").append(MID_END)
                    .append(TABLE_BEGIN).append(JSONObject.toJSONString(getExecObjs())).append(MID_END);
                }

                bf.append(TEXT_BEGIN).append(getUnExecCount());
                if (getUnExecObjs()!=null && getUnExecObjs().size()!=0) {
                    bf.append(MID_END).append(TEXT_BEGIN).append("待运行详情").append(MID_END)
                    .append(TABLE_BEGIN).append(JSONObject.toJSONString(getUnExecObjs())).append(Finally_END);
                } else {
                    bf.append(Finally_END);
                }
        System.out.println(bf);
        return bf.toString();
    }

    public static String delimiterHour(){
        StringBuffer bf = new StringBuffer();
        return bf.append(TEXT_Finally_BEGIN).append(Constants.DELIMITER_HOUR).append(MID_END).toString();
    }

    public static String delimiterDay(){
        StringBuffer bf = new StringBuffer();
        return bf.append(TEXT_Finally_BEGIN).append(Constants.DELIMITER_DAY).append(MID_END).toString();
    }

    public static String delimiterWeek(){
        StringBuffer bf = new StringBuffer();
        return bf.append(TEXT_Finally_BEGIN).append(Constants.DELIMITER_WEEK).append(MID_END).toString();
    }

    public static String delimiterMonth(){
        StringBuffer bf = new StringBuffer();
        return bf.append(TEXT_Finally_BEGIN).append(Constants.DELIMITER_MONTH).append(MID_END).toString();
    }

    public String delimiterDefault(){
        StringBuffer bf = new StringBuffer();
        return bf.append(TEXT_BEGIN).append(Constants.DELIMITER_DEFAULT).append(MID_END).toString();
    }
}
