package com.oldwang.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.io.Serializable;
import java.util.Objects;

/**
 * 自定义类型商品
 * 自定义的类型和ES中的索引产生关联
 *
 * indexName 索引对应的名称
 * shards 创建索引时设置的主分片数量
 * replicas 创建索引时设置的副本分片数量
 */
@Document(indexName = "ego_item",type = "item",shards = 1,replicas = 0)
public class Item implements Serializable {
    /**
     * @ID 是Springdata核心工程提供的 是所有的Spring Data二级工程通用的
     */
    @Id
    private String id;
    /**
     *     商品名称 需要中文分词 且偶尔需要排序 常用搜索条件之一
     *     @Field 描述实体类型与ES中字段的关系
     *     可以为这个字段自定义mapping
     *     这个自定义映射必须通过代码逻辑设置映射API才能生效
     *     name -- 索引中的字段名称
     *     type -- 索引中字段的类型 默认是fieldType.auto 代表ES自动映射
     *     analyzer - 字段的分词器名称
     *     fielddata - 是否开启正向索引 默认关闭
     *                 默认只为文本类型的字段创建反向索引 提供快速搜索逻辑 设置为true 则会额外创建一个正向索引
     *     index 是否创建反向索引和正向索引，text文本创建反向索引，其他类型创建正向索引
     *           没有索引就不能作为搜索条件
     */
    @Field(name = "title",type = FieldType.Text,analyzer = "ik_max_word",fielddata = true)
    private String title;
    //可以中文分词 常用搜索
    @Field(name = "sell_point",type = FieldType.Text,analyzer = "ik_max_word")
    private String sellPoint;
    @Field(name = "price",type = FieldType.Long)
    private Long price;
    @Field(name = "num",type = FieldType.Integer)
    private  int num;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getSellPoint() {
        return sellPoint;
    }

    public void setSellPoint(String sellPoint) {
        this.sellPoint = sellPoint;
    }

    public Long getPrice() {
        return price;
    }

    public void setPrice(Long price) {
        this.price = price;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Item item = (Item) o;
        return num == item.num &&
                Objects.equals(id, item.id) &&
                Objects.equals(title, item.title) &&
                Objects.equals(sellPoint, item.sellPoint) &&
                Objects.equals(price, item.price);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, title, sellPoint, price, num);
    }

    @Override
    public String toString() {
        return "Item{" +
                "Id='" + id + '\'' +
                ", title='" + title + '\'' +
                ", sellPoint='" + sellPoint + '\'' +
                ", price=" + price +
                ", num=" + num +
                '}';
    }
}
