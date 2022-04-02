/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dolphinscheduler.service.quartz.cron;


import com.cronutils.model.Cron;
import com.cronutils.model.definition.CronDefinition;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import org.apache.dolphinscheduler.common.enums.CycleEnum;
import org.apache.dolphinscheduler.common.thread.Stopper;
import org.apache.dolphinscheduler.common.utils.DateUtils;
import org.quartz.CronExpression;
import org.quartz.TriggerUtils;
import org.quartz.impl.triggers.CronTriggerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.*;

import static com.cronutils.model.CronType.QUARTZ;
import static org.apache.dolphinscheduler.service.quartz.cron.CycleFactory.*;


/**
 * cron utils
 */
public class CronUtils {
  private CronUtils() {
    throw new IllegalStateException("CronUtils class");
  }
  private static final Logger logger = LoggerFactory.getLogger(CronUtils.class);


  private static final CronParser QUARTZ_CRON_PARSER = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(QUARTZ));

  /**
   * parse to cron
   * @param cronExpression cron expression, never null
   * @return Cron instance, corresponding to cron expression received
   */
  public static Cron parse2Cron(String cronExpression) {
    return QUARTZ_CRON_PARSER.parse(cronExpression);
  }


  /**
   * build a new CronExpression based on the string cronExpression
   * @param cronExpression String representation of the cron expression the new object should represent
   * @return CronExpression
   * @throws ParseException if the string expression cannot be parsed into a valid
   */
  public static CronExpression parse2CronExpression(String cronExpression) throws ParseException {
    return new CronExpression(cronExpression);
  }

  public static List<Date> getCronNext2TriggerTime(String cronExpression,Date nowDate) throws ParseException {
    CronExpression cron = new CronExpression(cronExpression);
    Date nextDate = nowDate;
    ArrayList<Date> cronTimes = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      nextDate = cron.getNextValidTimeAfter(nextDate);
      cronTimes.add(nextDate);
    }
    return cronTimes;
  }

  /**
   * get max cycle
   * @param cron cron
   * @return CycleEnum
   */
  public static CycleEnum getMaxCycle(Cron cron) {
    return min(cron).addCycle(hour(cron)).addCycle(day(cron)).addCycle(week(cron)).addCycle(month(cron)).getCycle();
  }

  /**
   * get min cycle
   * @param cron cron
   * @return CycleEnum
   */
  public static CycleEnum getMiniCycle(Cron cron) {
    return min(cron).addCycle(hour(cron)).addCycle(day(cron)).addCycle(week(cron)).addCycle(month(cron)).getMiniCycle();
  }
  
  public static int getSchedulerInterval(String crontab) {
    CycleEnum e = CronUtils.getMiniCycle(parse2Cron(crontab));
    return e.ordinal();
  }
  
  /**
   * get max cycle
   * @param crontab crontab
   * @return CycleEnum
   */
  public static CycleEnum getMaxCycle(String crontab) {
    return getMaxCycle(parse2Cron(crontab));
  }

  /**
   * gets all scheduled times for a period of time based on not self dependency
   * @param startTime startTime
   * @param endTime endTime
   * @param cronExpression cronExpression
   * @return date list
   */
  public static List<Date> getFireDateList(Date startTime, Date endTime, CronExpression cronExpression) {
    List<Date> dateList = new ArrayList<>();

    while (Stopper.isRunning()) {
      startTime = cronExpression.getNextValidTimeAfter(startTime);
      if (startTime.after(endTime)) {
        break;
      }
      dateList.add(startTime);
    }

    return dateList;
  }

  public static Date getNextFireDate(Date startTime, Date endTime, CronExpression cronExpression) {
    startTime = cronExpression.getNextValidTimeAfter(startTime);
    if (startTime.after(endTime)) {
      return null;
    }
    return startTime;
  }

  /**
   * gets expect scheduled times for a period of time based on self dependency
   * @param startTime startTime
   * @param endTime endTime
   * @param cronExpression cronExpression
   * @param fireTimes fireTimes
   * @return date list
   */
  public static List<Date> getSelfFireDateList(Date startTime, Date endTime, CronExpression cronExpression,int fireTimes) {
    List<Date> dateList = new ArrayList<>();
    while (fireTimes > 0) {
      startTime = cronExpression.getNextValidTimeAfter(startTime);
      if (startTime.after(endTime) || startTime.equals(endTime)) {
        break;
      }
      dateList.add(startTime);
      fireTimes--;
    }

    return dateList;
  }


  /**
   * gets all scheduled times for a period of time based on self dependency
   * @param startTime startTime
   * @param endTime endTime
   * @param cronExpression cronExpression
   * @return date list
   */
  public static List<Date> getSelfFireDateList(Date startTime, Date endTime, CronExpression cronExpression) {
    List<Date> dateList = new ArrayList<>();

    while (Stopper.isRunning()) {
      startTime = cronExpression.getNextValidTimeAfter(startTime);
      if (startTime.after(endTime) || startTime.equals(endTime)) {
        break;
      }
      dateList.add(startTime);
    }

    return dateList;
  }

  /**
   * gets all scheduled times for a period of time based on self dependency
   * @param startTime startTime
   * @param endTime endTime
   * @param cron cron
   * @return date list
   */
  public static List<Date> getSelfFireDateList(Date startTime, Date endTime, String cron) {
    CronExpression cronExpression = null;
    try {
      cronExpression = parse2CronExpression(cron);
    }catch (ParseException e){
      logger.error(e.getMessage(), e);
      return Collections.emptyList();
    }
    return getSelfFireDateList(startTime, endTime, cronExpression);
  }

  /**
   * get expiration time
   * @param startTime startTime
   * @param cycleEnum cycleEnum
   * @return date
   */
  public static Date getExpirationTime(Date startTime, CycleEnum cycleEnum) {
    Date maxExpirationTime = null;
    Date startTimeMax = null;
    try {
      startTimeMax = getEndTime(startTime);

      Calendar calendar = Calendar.getInstance();
      calendar.setTime(startTime);
      switch (cycleEnum) {
        case HOUR:
          calendar.add(Calendar.HOUR, 1);
          break;
        case DAY:
          calendar.add(Calendar.DATE, 1);
          break;
        case WEEK:
          calendar.add(Calendar.DATE, 1);
          break;
        case MONTH:
          calendar.add(Calendar.DATE, 1);
          break;
        default:
          logger.error("Dependent process definition's  cycleEnum is {},not support!!", cycleEnum);
          break;
      }
      maxExpirationTime = calendar.getTime();
    } catch (Exception e) {
      logger.error(e.getMessage(),e);
    }
    return DateUtils.compare(startTimeMax,maxExpirationTime)?maxExpirationTime:startTimeMax;
  }

  /**
   * get the end time of the day by value of date
   * @param date
   * @return date
   */
  private static Date getEndTime(Date date) {
    Calendar end = new GregorianCalendar();
    end.setTime(date);
    end.set(Calendar.HOUR_OF_DAY,23);
    end.set(Calendar.MINUTE,59);
    end.set(Calendar.SECOND,59);
    end.set(Calendar.MILLISECOND,999);
    return end.getTime();
  }

  /**
   * 获取给定cron表达式 当前时间 的上一次时触发的日期
   * @param cron
   * @return
   */
  public static Date lastExecDate(String cron) throws ParseException {
    List<Date> dates = computeFireTimes(cron);
    Date nowDate = new Date();
    return findNearExecution(dates,nowDate).get(0);
  }

  /**
   * 获取给定cron表达式 当前时间 的下一次时触发的日期
   * @param cron
   * @return
   */
  public static Date nextExecDate(String cron) throws ParseException {
    List<Date> dates = computeFireTimes(cron);
    Date nowDate = new Date();
    return findNearExecution(dates,nowDate).get(1);
  }

  /**
   * 获取近4个月的 cron 表达式所有触发的时间集合
   * @param cron
   * @return
   */
  public static List<Date> computeFireTimes(String cron) throws ParseException {
    List<Date> dates = null;
    CronTriggerImpl cronTriggerImpl = new CronTriggerImpl();

    cronTriggerImpl.setCronExpression(cron);

    Calendar nextDate = Calendar.getInstance();
    Date now = nextDate.getTime();
    nextDate.add(Calendar.MONTH, 2);
    Calendar preDate = Calendar.getInstance();
    preDate.add(Calendar.MONTH,-2);
    dates = TriggerUtils.computeFireTimesBetween(
            cronTriggerImpl, null, preDate.getTime(),
            nextDate.getTime());

    return dates;
  }

  /**
   * 查找和给定日期d 最近的 两个日期
   * @param list
   * @param d
   * @return
   */
  private static List<Date> findNearExecution(List<Date> list,Date d){
    if(list==null || list.size()<=0){
      return null;
    }
    long nextTime=Long.MAX_VALUE;
    long lastTime=Long.MIN_VALUE;
    Date rn = null;
    Date rl = null;
    long time=d.getTime();
    for(Date t:list){
      long tm=t.getTime()-time;
      // 小于传入的date
      if (tm>0){
        if(nextTime>tm){
          nextTime=tm;
          rn=t;
        }
      } else {
        if (lastTime<tm){
          lastTime = tm;
          rl=t;
        }
      }
    }
    ArrayList<Date> dates = new ArrayList<>();
    dates.add(rl);
    dates.add(rn);
    return dates;
  }


  // use Cron utils to get last and next Execution time
  private static final CronDefinition CRON_DEFINITION = CronDefinitionBuilder.instanceDefinitionFor(QUARTZ);

  /**
   * 查找上一次的cron表达式的执行时间
   * @param expression
   * @return
   */
  public static LocalDateTime lastExecution(String expression) {
    CronParser parser = new CronParser(CRON_DEFINITION);
    Cron quartzCron = parser.parse(expression);
    ZonedDateTime now = ZonedDateTime.now();
    ExecutionTime executionTime = ExecutionTime.forCron(quartzCron);
    Optional<ZonedDateTime> zonedDateTimeOptional = Optional.ofNullable(executionTime.lastExecution(now));
    if (zonedDateTimeOptional.isPresent()) {
      ZonedDateTime zonedDateTime = zonedDateTimeOptional.get();
      return zonedDateTime.toLocalDateTime();
    }
    return null;
  }

  /**
   * 查找下一次的cron表达式的执行时间
   * @param expression
   * @return LocalDateTime
   */
  public static LocalDateTime nextExecution(String expression) {
    CronParser parser = new CronParser(CRON_DEFINITION);
    Cron quartzCron = parser.parse(expression);
    ZonedDateTime now = ZonedDateTime.now();
    ExecutionTime executionTime = ExecutionTime.forCron(quartzCron);
    Optional<ZonedDateTime> zonedDateTimeOptional = Optional.ofNullable(executionTime.nextExecution(now));
    if (zonedDateTimeOptional.isPresent()) {
      ZonedDateTime zonedDateTime = zonedDateTimeOptional.get();
      return zonedDateTime.toLocalDateTime();
    }
    return null;
  }

  /**
   * 获取给定cron表达式 给定日期 的下一次时触发的日期
   * @param cron
   * @param d
   * @return
   */
  public static Date nextExecDate(String cron, Date d) throws ParseException {
    logger.info("now cron :{}, date :{}",cron,d.toString());
    List<Date> dates = computeFireTimes(cron);
    Date nextDate = findNearExecution(dates, d).get(1);
    logger.info(nextDate.toString());
    return nextDate;
  }
}
