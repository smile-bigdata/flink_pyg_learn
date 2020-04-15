package com.itheima.report.controller;

import com.alibaba.fastjson.JSON;
import com.itheima.report.bean.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
public class ReportController {

    @Autowired
    KafkaTemplate kafkaTemplate;

    @RequestMapping("/receive")
    public Map<String,String> receive(@RequestBody String json){

        Map<String,String> map = new HashMap();

        try {
            // 构建Message

            Message msg = new Message();
            msg.setMessage(json);
            msg.setCount(1);
            msg.setTimeStamp(System.currentTimeMillis());

            String msgJSON = JSON.toJSONString(msg);

            // 发送Message到Kafka
            kafkaTemplate.send("pyg", msgJSON);
            map.put("sucess", "true");
        }catch (Exception ex){
            ex.printStackTrace();
            map.put("sucess", "false");
        }


        return map;

    }
}
