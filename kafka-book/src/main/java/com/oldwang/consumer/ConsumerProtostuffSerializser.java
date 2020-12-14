package com.oldwang.consumer;

import com.oldwang.custom_serializer.User;
import io.protostuff.ProtobufIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * @author oldwang
 * Protostuff 反序列化
 */
public class ConsumerProtostuffSerializser implements Deserializer<User> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public User deserialize(String topic, byte[] data) {
        if(data == null){
            return null;
        }
        Schema<User> schema = RuntimeSchema.getSchema(User.class);
        User user = new User();
        ProtobufIOUtil.mergeFrom(data,user,schema);
        return user;
    }

    @Override
    public void close() {

    }
}
