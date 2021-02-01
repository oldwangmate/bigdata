package com.oldwang.service;

import com.oldwang.model.EgoLogMessage;
import org.elasticsearch.index.query.QueryBuilders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery;
import org.springframework.data.elasticsearch.core.query.SearchQuery;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class LogServiceImpl implements LogsService {

    @Autowired
    private ElasticsearchRestTemplate restTemplate;
    @Override
    public List<EgoLogMessage> getLogByPage(String q, int page, int rows) {
        SearchQuery query = new NativeSearchQuery(QueryBuilders.matchQuery("message",q));
        query.setPageable(PageRequest.of(page-1,rows));
        List<EgoLogMessage> egoLogMessages = restTemplate.queryForList(query, EgoLogMessage.class);
        return egoLogMessages;
    }
}
