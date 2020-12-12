package com.oldwang.custom_serializer;

import org.apache.kafka.common.serialization.Deserializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @author oldwang
 */
public class CustomUserDeserializer implements Deserializer<User> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public User deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        if (data.length < 8){
            throw  new RuntimeException("data length error");
        }
        //wrap可以把字节数组包装成缓冲区ByteBuffer
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int idLen,nameLen;

        //get()从buffer中取出数据，每次取完之后position指向当前取出的元素的下一位，可以理解为按顺序依次读取
        idLen = buffer.getInt();
        byte[] idBytes = new byte[idLen];
        buffer.get(idBytes);

        nameLen = buffer.getInt();
        byte[] nameBytes = new byte[nameLen];
        buffer.get(nameBytes);

        int id = 0;
        String name = null;
        try {
            id = Integer.parseInt(new String(idBytes,"UTF-8"));
            name = new String(nameBytes,"UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return new User(id,name);
    }

        @Override
    public void close() {

    }
}
