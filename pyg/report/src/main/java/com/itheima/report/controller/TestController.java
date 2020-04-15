package com.itheima.report.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

// 表示这是一个Controller,并且其中的所有的方法都是带有@ResponseBody的注解
@RestController
public class TestController {

    @RequestMapping("/test")
    public String test(String json){
        System.out.println(json);
        return json;
    }
}
