package com.oldwang.consumer;

import com.oldwang.custom_serializer.User;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtobufIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * @author oldwang
 * Protostuff 序列化
 */
public class ProducerProtostuffSerializser implements Serializer<User> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, User data) {
        if(data == null){
            return null;
        }
        Schema schema = RuntimeSchema.getSchema(data.getClass());
        LinkedBuffer buffer = LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE);
        byte[] protostuff;
        protostuff = ProtobufIOUtil.toByteArray(data,schema,buffer);
        buffer.clear();
        return protostuff;

    }

    @Override
    public void close() {

    }
}
