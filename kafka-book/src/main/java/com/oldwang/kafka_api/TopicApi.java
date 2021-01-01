package com.oldwang.kafka_api;

import kafka.admin.TopicCommand;

/**
 * 使用java程序操作topic
 * @author oldwang
 */
public class TopicApi {

    public static void main(String[] args) {
        createTopic();
        topicList();
        topicDescribe();
        topicDelete();
    }

    /**
     * 删除topic
     */
    private static void topicDelete() {
        String[] options = new String[]{
                "--zookeeper","localhost:2181/kafka",
                "--topic","frist",
                "--delete"
        };
        TopicCommand.main(options);
    }

    /**
     * 查看topic详细信息
     */
    private static void topicDescribe() {
        String[] options = new String[]{
                "--zookeeper","localhost:2181/kafka",
                "--topic","kafka-demo",
                "--describe"
        };
        TopicCommand.main(options);
    }

    /**
     * 查看所有主题
     */
    private static void topicList() {
        String[] options = new String[]{
                "--zookeeper","localhost:2181/kafka",
                "--list"
        };
        TopicCommand.main(options);
    }

    /**
     * 创建topic
     */
    public static void createTopic(){
        String[] options = new String[]{
                "--zookeeper","localhost:2181/kafka",
                "--topic","create-topic-adpi",
                "--create",
                "--partitions","1",
                "--replication-factor","1"
        };

        TopicCommand.main(options);
    }
}
