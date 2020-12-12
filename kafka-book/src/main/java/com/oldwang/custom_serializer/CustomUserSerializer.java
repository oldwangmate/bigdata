package com.oldwang.custom_serializer;

import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @author oldwang
 * 自定义序列化器
 */
public class CustomUserSerializer implements Serializer<User> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, User data) {
        if(data == null){
            return null;
        }
        byte[] id,name;
        try {
            if(data.getName() != null) {
                name = data.getName().getBytes("UTF-8");
            }else {
                name = new byte[0];
            }
            if(data.getId() >= 0){
                id = (data.getId()+"").getBytes("UTF-8");
            }else {
                id = new byte[0];
            }
            ByteBuffer buffer = ByteBuffer.allocate(4+4+name.length+id.length);
            buffer.putInt(id.length);
            buffer.put(id);
            buffer.putInt(name.length);
            buffer.put(name);
            return buffer.array();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return new byte[0];
    }

    @Override
    public void close() {

    }
}
