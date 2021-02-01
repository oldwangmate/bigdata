package com.oldwang.controller;

import com.oldwang.model.EgoLogMessage;
import com.oldwang.service.LogsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class LogController {

    @Autowired
    private LogsService logsService;

    @GetMapping("/getLogs")
    public List<EgoLogMessage> getLogByPage(String q, @RequestParam(value = "page",defaultValue = "1") int page, @RequestParam(value = "rows",defaultValue = "2") int rows){
        return logsService.getLogByPage(q,page,rows);
    }
}
