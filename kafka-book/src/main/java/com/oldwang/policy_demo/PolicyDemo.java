package com.oldwang.policy_demo;

import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.server.policy.CreateTopicPolicy;

import java.util.Map;

/**
 * @author oldwang
 * 主题合法性验证示例
 * 自定义类实现org.apache.kafka.server.policy.CreateTopicPolicy接口然后在broker端的配置文件config/server.properties中配置参数 create.topic.policy.class.name的值为PolicyDemo 最后启动服务
 */
public class PolicyDemo implements CreateTopicPolicy {

    @Override
    public void validate(RequestMetadata requestMetadata) throws PolicyViolationException {
        if(requestMetadata.numPartitions() != null || requestMetadata.replicationFactor() != null){
            if(requestMetadata.numPartitions() < 5){
                throw new PolicyViolationException("topic numPartitions less than 5");
            }
            if (requestMetadata.replicationFactor() < 1){
                throw new PolicyViolationException("topic replicationFactor less than 1");
            }
        }
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
