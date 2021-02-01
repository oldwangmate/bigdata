package com.oldwang.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.io.Serializable;
import java.util.Date;
import java.util.Objects;

@Document(indexName = "ego-logs-index" ,type = "_doc")
public class EgoLogMessage implements Serializable {

    @Id
    private String id;
    @Field(type = FieldType.Text)
    private String message;
    @Field(type = FieldType.Long)
    private int port;
    @Field(type = FieldType.Text,name = "@version")
    @JsonProperty(value = "@version")
    private String version;
    @Field(type = FieldType.Text)
    private String host;
    @Field(type = FieldType.Date,name = "@timestamp")
    @JsonProperty(value = "@timestamp")
    private Date timestamp;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EgoLogMessage that = (EgoLogMessage) o;
        return port == that.port &&
                Objects.equals(id, that.id) &&
                Objects.equals(message, that.message) &&
                Objects.equals(version, that.version) &&
                Objects.equals(host, that.host) &&
                Objects.equals(timestamp, that.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, message, port, version, host, timestamp);
    }

    @Override
    public String toString() {
        return "EgoLogMessage{" +
                "id='" + id + '\'' +
                ", message='" + message + '\'' +
                ", port=" + port +
                ", version='" + version + '\'' +
                ", host='" + host + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
