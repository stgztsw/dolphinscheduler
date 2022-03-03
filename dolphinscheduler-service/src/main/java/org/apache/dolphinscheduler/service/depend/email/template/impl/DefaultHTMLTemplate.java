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
package org.apache.dolphinscheduler.service.depend.email.template.impl;

import com.alibaba.fastjson.JSONObject;
import org.apache.dolphinscheduler.common.enums.ShowType;
import org.apache.dolphinscheduler.common.utils.StringUtils;
import org.apache.dolphinscheduler.service.depend.email.template.AlertTemplate;
import org.apache.dolphinscheduler.service.depend.email.utils.Constants;
import org.apache.dolphinscheduler.service.depend.email.utils.JSONUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.dolphinscheduler.common.utils.Preconditions.checkNotNull;

/**
 * the default html alert message template
 */
public class DefaultHTMLTemplate implements AlertTemplate {

    public static final Logger logger = LoggerFactory.getLogger(DefaultHTMLTemplate.class);

    private static  List<HashMap<String, Object>> msgTableMapList = new ArrayList<>();


    @Override
    public String getMessageFromTemplate(String content, ShowType showType, boolean showAll) {

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
                while (iterator.hasNext()){

                    Map.Entry<String, Object> entry = iterator.next();
                    // 表头
                    t.append(Constants.TH).append(entry.getKey()).append(Constants.TH_END);
                    // rows
                    cs.append(Constants.TD).append(String.valueOf(entry.getValue())).append(Constants.TD_END);

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
            System.out.println(content);
            List<Map<String, Object>> list;
            try {
                // 拆分出 text 和 table 存到 List属性msgTableMapList中
                pushElementInHtmlMap(content);
                // 遍历msgTableMapList生成 html 代码


            } catch (Exception e) {
                logger.error("json format exception", e);
                return null;
            }

            StringBuilder contents = new StringBuilder();
            String title = "";
            for (HashMap<String, Object> map: msgTableMapList){
                if (map.keySet().size()!=1){
                    throw new RuntimeException("email one row map keySet must be one number of size");
                }
                String key = map.keySet().iterator().next();
                if ("text".equals(key)){
                    contents.append(Constants.H4).append(map.get(key)).append(Constants.H4_END);
//                    contents.append(Constants.BR);
                }else if ("table".equals(key)){

                    LinkedHashMap<Integer, JSONObject> tableMap = (LinkedHashMap) map.get(key);
                    if (tableMap==null){
                        contents.append(Constants.P).append("无告警实例详情").append(Constants.P_END);
                        contents.append(Constants.BR);
                        contents.append(Constants.HR);
                    }else {
                        // data
                        Iterator<Map.Entry<Integer, JSONObject>> iterator = tableMap.entrySet().iterator();

                        StringBuilder t = new StringBuilder(Constants.TABLE);
                        StringBuilder cs = new StringBuilder();

                        // 标题
//                        t.append(Constants.CAPTION).append("告警工作流详情").append(Constants.CAPTION_END);

                        // 表头
                        t.append(Constants.TR);
//                        t.append(Constants.TH).append("projectName").append(Constants.TH_END);
                        t.append(Constants.TH).append("processId").append(Constants.TH_END);
                        t.append(Constants.TH).append("definitionId").append(Constants.TH_END);
                        t.append(Constants.TH).append("name").append(Constants.TH_END);
                        t.append(Constants.TH).append("state").append(Constants.TH_END);
                        t.append(Constants.TH).append("link").append(Constants.TH_END);
                        t.append(Constants.TR_END);
                        title = t.toString();
//                        contents.append(StringUtils.isEmpty(title) ? "" : String.format("<thead>%s</thead>\n", title));
                        contents.append(title);

                        while (iterator.hasNext()) {

                            Map.Entry<Integer, JSONObject> entry = iterator.next();
                            JSONObject jo = entry.getValue();
                            Integer processId = (Integer) jo.get("processId");

                            // rows data
                            cs.append(Constants.TR);
//                            cs.append(Constants.TD).append(jo.get("projectName")).append(Constants.TD_END);
                            cs.append(Constants.TD).append(jo.get("processId")).append(Constants.TD_END);
                            cs.append(Constants.TD).append(jo.get("definitionId")).append(Constants.TD_END);
                            cs.append(Constants.TD).append(jo.get("name")).append(Constants.TD_END);
                            cs.append(Constants.TD).append(jo.get("state")).append(Constants.TD_END);
                            cs.append(Constants.TD).append(
                                    processId!=null && !processId.equals(0)? String.format("<a href=\"%s\">实例链接</a>",Constants.INSTANCE_LINK_PRD+processId) : "").append(Constants.TD_END);
                            cs.append(Constants.TR_END);
                        }
                        contents.append(cs).append(Constants.TABLE_END);
                        contents.append(Constants.HR);
                    }
                }
            }
            String message = getMessageFromTableMsgTemplate(contents.toString());
            // 移除已经处理过的 html message
            System.out.println("移除已经处理过的 html message");
            msgTableMapList.clear();
            return message;
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
        int startIdx = (content.startsWith("[TEXT:"))? 6 : 7;
        String endStr = "],";
        int endIdx = content.indexOf(endStr);
        int length = content.length();
        // 找不到],说明是最后一条了 找]
        if (content.indexOf(endStr)==-1){
            endIdx = content.indexOf("]");
        }
        // 截取text的文本内容或table的表格内容
        String obj = content.substring(startIdx, endIdx);

        if (content.startsWith("[TEXT:")) { // 文本
            map.put("text",obj);
            msgTableMapList.add(map);
            if (content.length()==endIdx+1){
                return "";
            }

            return pushElementInHtmlMap(content.substring(endIdx + endStr.length(), length));
        }else if (content.startsWith("[TABLE:")) { // 表格
            LinkedHashMap<Integer,JSONObject> resMap;
            if ("{}".equals(obj)) {
                resMap = null;
            }else {
                resMap = JSONObject.parseObject(obj, LinkedHashMap.class);
            }
            map.put("table",resMap);

            msgTableMapList.add(map);
            if (length==endIdx+1){
                return "";
            }
            return pushElementInHtmlMap(content.substring(endIdx + endStr.length(), length));
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

    private String getMessageFromHtmlTemplate(String content){

        checkNotNull(content);
        System.out.println(Constants.HTML_HEADER_PREFIX + content + Constants.BOBY_HTML_TAIL);
        return Constants.HTML_HEADER_PREFIX + content + Constants.BOBY_HTML_TAIL;
    }

    private String getMessageFromTableMsgTemplate(String content){

        checkNotNull(content);
        System.out.println(Constants.HTML_HEADER_PREFIX + content + Constants.BOBY_HTML_TAIL);
        return Constants.HTML_HEADER + content + Constants.BOBY_HTML_TAIL;
    }

}
