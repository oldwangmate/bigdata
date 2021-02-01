package com.oldwang.service;

import com.oldwang.model.EgoLogMessage;

import java.util.List;

public interface LogsService {
    List<EgoLogMessage> getLogByPage(String q, int page, int rows);
}
