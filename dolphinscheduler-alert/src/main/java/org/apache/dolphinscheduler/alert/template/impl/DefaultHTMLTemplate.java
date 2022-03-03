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
package org.apache.dolphinscheduler.alert.template.impl;

import org.apache.dolphinscheduler.alert.template.AlertTemplate;
import org.apache.dolphinscheduler.alert.utils.Constants;
import org.apache.dolphinscheduler.alert.utils.JSONUtils;
import org.apache.dolphinscheduler.common.enums.ShowType;
import org.apache.dolphinscheduler.common.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.dolphinscheduler.common.utils.Preconditions.checkNotNull;

/**
 * the default html alert message template
 */
public class DefaultHTMLTemplate implements AlertTemplate {

    public static final Logger logger = LoggerFactory.getLogger(DefaultHTMLTemplate.class);

    private static  List<HashMap<String, Object>> msgTableMap = new ArrayList<>();


    @Override
    public String getMessageFromTemplate(String content, ShowType showType,boolean showAll) {

        switch (showType){
            case TABLE:
                return getTableTypeMessage(content,showAll);
            case TEXT:
                return getTextTypeMessage(content,showAll);
            case MSGTABKE:
                return getMsgTableTypeMessage(content,showAll);
            default:
                throw new IllegalArgumentException(String.format("not support showType: %s in DefaultHTMLTemplate",showType));
        }
    }

    /**
     * get alert message which type is TABLE
     * @param content message content
     * @param showAll weather to show all
     * @return alert message
     */
    private String getTableTypeMessage(String content,boolean showAll){

        if (StringUtils.isNotEmpty(content)){
            List<LinkedHashMap> mapItemsList = JSONUtils.toList(content, LinkedHashMap.class);

            if(!showAll && mapItemsList.size() > Constants.NUMBER_1000){
                mapItemsList = mapItemsList.subList(0,Constants.NUMBER_1000);
            }

            StringBuilder contents = new StringBuilder(200);

            boolean flag = true;

            String title = "";
            for (LinkedHashMap mapItems : mapItemsList){

                Set<Map.Entry<String, Object>> entries = mapItems.entrySet();

                Iterator<Map.Entry<String, Object>> iterator = entries.iterator();

                StringBuilder t = new StringBuilder(Constants.TR);
                StringBuilder cs = new StringBuilder(Constants.TR);
                String processId = null;
                while (iterator.hasNext()){
                    Map.Entry<String, Object> entry = iterator.next();

                    if (entry.getKey().equals("task id")){
                        processId = (String) entry.getValue();
                    }

                    // 表头
                    t.append(Constants.TH).append(entry.getKey()).append(Constants.TH_END);
                    // rows Constants.TASK_LOG_LINK_DEV dev or prd
                    cs.append(Constants.TD).append(
                            (entry.getKey().equals("log path") && processId!=null) ?
                                    String.format("<a href=\"%s\">%s</a>",Constants.TASK_LOG_LINK_PRD+processId,entry.getValue()) :
                                    String.valueOf(entry.getValue()))
                            .append(Constants.TD_END);
//                    cs.append(Constants.TD).append(entry.getValue()).append(Constants.TD_END);
                }
                t.append(Constants.TR_END);
                cs.append(Constants.TR_END);
                if (flag){
                    title = t.toString();
                }
                flag = false;
                contents.append(cs);
            }

            return getMessageFromHtmlTemplate(title,contents.toString());
        }

        return content;
    }

    /**
     * get alert message which type is TEXT
     * @param content message content
     * @param showAll weather to show all
     * @return alert message
     */
    private String getTextTypeMessage(String content,boolean showAll){

        if (StringUtils.isNotEmpty(content)){
            List<String> list;
            try {
                list = JSONUtils.toList(content,String.class);
            }catch (Exception e){
                logger.error("json format exception",e);
                return null;
            }

            StringBuilder contents = new StringBuilder(100);
            for (String str : list){
                contents.append(Constants.TR);
                contents.append(Constants.TD).append(str).append(Constants.TD_END);
                contents.append(Constants.TR_END);
            }

            return getMessageFromHtmlTemplate(null,contents.toString());

        }

        return content;
    }

    private String getMsgTableTypeMessage(String content,boolean showAll){

        if (StringUtils.isNotEmpty(content)) {
            List<Map<String, Object>> list;
            try {
                // 拆分出 text 和 table
                pushElementInHtmlMap(content);
                // 遍历生成 html 代码

            } catch (Exception e) {
                logger.error("json format exception", e);
                return null;
            }

            StringBuilder contents = new StringBuilder(100);
            for (HashMap<String, Object> map: msgTableMap){
                Iterator<Map.Entry<String, Object>> iter = map.entrySet().iterator();
                for (String s : map.keySet()) {
                    
                }

//                contents.append(Constants.H4).append(str).append(Constants.H4_END);
            }

            return getMessageFromHtmlTemplate(null,contents.toString());
        }
        return "";
    }

    private String pushElementInHtmlMap(String content) {
        /**
         * 约定好的格式：
         *    文字用[text:]标记
         *    表格用[table:]标记
         *    中间用,分割
         */
        HashMap<String, Object> map = new HashMap<>();
        int startIdx = (content.startsWith("[text:"))? 6 : 7;
        String endStr = "],";
        int endIdx = content.indexOf(endStr);
        if (content.indexOf(endStr)==-1){
            endIdx = content.indexOf("]");
        }
        String obj = content.substring(startIdx, endIdx);
        if (content.startsWith("[text:")) {
            map.put("text",obj);
            msgTableMap.add(map);
            if (content.length()==endIdx+1){
                return "";
            }
            return pushElementInHtmlMap(content.substring(endIdx + endStr.length(), content.length()));
        }else if (content.startsWith("[table:")) {
            map.put("table",obj);
            msgTableMap.add(map);
            if (content.length()==endIdx+1){
                return "";
            }
            return pushElementInHtmlMap(content.substring(endIdx + endStr.length(), content.length()));
        }
        return "";
    }

    /**
     * get alert message from a html template
     * @param title     message title
     * @param content   message content
     * @return alert message which use html template
     */
    private String getMessageFromHtmlTemplate(String title,String content){

        checkNotNull(content);
        String htmlTableThead = StringUtils.isEmpty(title) ? "" : String.format("<thead>%s</thead>\n",title);

        return Constants.HTML_HEADER_PREFIX +htmlTableThead + content + Constants.TABLE_BODY_HTML_TAIL;
    }

}
