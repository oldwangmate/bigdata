package com.oldwang.es.test;

import com.oldwang.Application;
import com.oldwang.model.Item;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.SearchResultMapper;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;
import org.springframework.data.elasticsearch.core.aggregation.impl.AggregatedPageImpl;
import org.springframework.data.elasticsearch.core.query.*;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@SpringBootTest(classes = Application.class)
@RunWith(SpringRunner.class)
public class SpringDataEsTest {

    /**
     * ES6.5版本以前 使用客户端一般都是Transport客户端 数据交换客户端 通过端口9300借助协议TCP实现交换数据访问
     * Spring data ElasticSearch 提供的客户端Template对象类型是ElasticSearchTemplate
     * 配置
     * spring:
     *   data:
     *     elasticsearch:
     *       cluster-name: elasticsearch #必须提供的配置 集群名称
     *       cluster-nodes: localhost:9300  #transport客户端端口是9300
     *
     * ES6版本以后 官方推荐使用Rest客户端 通过端口9200 借助HTTP协议 实现数据访问控制
     * Spring data ElasticSearch 提供的客户端Template对象类型是ElasticsearchRestTemplate
     *
     * 配置
     *  spring:
     *   elasticsearch:
     *     rest:
     *       uris: http://localhost:9200
     *
     */



    @Autowired
    private ElasticsearchRestTemplate elasticsearchRestTemplate;


    /**
     * 高亮搜索
     */
    @Test
    public void highlight(){
        HighlightBuilder.Field field = new HighlightBuilder.Field("title");
        field.preTags("<em>");
        field.postTags("</em>");

        SearchQuery searchQuery = new NativeSearchQueryBuilder()
                //排序
                .withSort(SortBuilders.fieldSort("price").order(SortOrder.ASC))
                //分页
                .withPageable(PageRequest.of(0,3))
                //搜索条件
                .withQuery(QueryBuilders.matchQuery("title","ElasticSearch"))
                //设置高亮字段
                .withHighlightFields(field)
                .build();

        AggregatedPage<Item> items = elasticsearchRestTemplate.queryForPage(searchQuery, Item.class, new SearchResultMapper() {
            //处理搜索结果 搜索的完整结果 也就是个集合

            /**
             *
             * @param <T>
             * @param response 搜索的结果 相当于在kibana中执行搜索的结果内容
             * @param clazz 就是返回结果的具体类型
             * @param pageable 分页处理
             * @return
             */
            @Override
            public <T> AggregatedPageImpl mapResults(SearchResponse response, Class<T> clazz, Pageable pageable) {
                //获取总条数
                long totalHits = response.getHits().totalHits;
                System.out.println("totalHits：" + totalHits);

                //获取搜索的结果数据
                SearchHit[] hits = response.getHits().getHits();
                List<Item> list = new ArrayList<>();
                for (SearchHit hit : hits) {
                    //搜索的source源
                    Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                    Item item = new Item();
                    item.setId(sourceAsMap.get("id").toString());
                    item.setSellPoint(sourceAsMap.get("sellPoint").toString());
                    item.setTitle(sourceAsMap.get("title").toString());
                    item.setPrice(Long.parseLong(sourceAsMap.get("price").toString()));
                    item.setNum(Integer.parseInt(sourceAsMap.get("num").toString()));

                    //高亮数据处理 key 字段名 value 是高亮数据
                    Map<String, HighlightField> highlightFields = hit.getHighlightFields();
                    HighlightField title = highlightFields.get("title");
                    if (title == null) {
                        //没有高亮数据
                        item.setTitle(highlightFields.get("title").toString());
                    } else {
                        item.setTitle(title.getFragments()[0].toString());
                    }
                    list.add(item);
                }
                return new AggregatedPageImpl(list, pageable, totalHits);
            }

            //不提供实现
            @Override
            public <T> T mapSearchHit(SearchHit searchHit, Class<T> type) {
                return null;
            }
        });
        List<Item> content = items.getContent();
        for (Item item : content) {
            System.out.println(item);
        }

    }


    /**
     * 分页和排序
     * 所有的Spring data 子工程中的分页排序逻辑和排序逻辑都是相似的方式
     * 根据PageRequest和sort实现分页和排序
     */
    @Test
    public void LimitAndSort(){
        SearchQuery searchQuery = new NativeSearchQuery(
                QueryBuilders.matchAllQuery()

        );
        //设置分页
        searchQuery.setPageable(PageRequest.of(0,2));
        //设置排序
        searchQuery.addSort(Sort.by(Sort.Direction.DESC,"price"));

        //设置分页同时设置排序
//        searchQuery.setPageable(PageRequest.of(0,2,Sort.by(Sort.Direction.DESC,"price")));
        List<Item> items = elasticsearchRestTemplate.queryForList(searchQuery, Item.class);
        items.forEach(System.out::println);
    }

    /**
     * 复合搜索
     */
    @Test
    public void bool(){
        //创建一个bool搜索条件 相当于定义bool : {must:[],should:[],must_not:[]}
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        List<QueryBuilder> must = boolQueryBuilder.must();
        must.add(QueryBuilders.matchQuery("title","手机"));
        must.add(QueryBuilders.rangeQuery("price").lte(1000L).gte(10L));

//        boolQueryBuilder.should();
//        boolQueryBuilder.mustNot();
        SearchQuery searchQuery = new NativeSearchQuery(boolQueryBuilder);
        List<Item> items = elasticsearchRestTemplate.queryForList(searchQuery, Item.class);
        items.forEach(System.out::println);
    }

    /**
     * 范围搜索
     */
    @Test
    public void range(){
        SearchQuery searchQuery = new NativeSearchQuery(
                QueryBuilders.rangeQuery("price")
                .lte(1000L)
                .gte(10L)

        );
        List<Item> items = elasticsearchRestTemplate.queryForList(searchQuery, Item.class);
        items.forEach(System.out::println);
    }

    /**
     * 词组搜索
     */
    @Test
    public void term(){
        SearchQuery searchQuery = new NativeSearchQuery(
                QueryBuilders.termQuery("title","手机")
        );
        List<Item> items = elasticsearchRestTemplate.queryForList(searchQuery, Item.class);
        items.forEach(System.out::println);
    }

    /**
     * 短语搜索
     */
    @Test
    public void matchPhrase(){
        SearchQuery searchQuery = new NativeSearchQuery(
                QueryBuilders.matchPhraseQuery("sellPoint","月亮")
        );
        List<Item> items = elasticsearchRestTemplate.queryForList(searchQuery, Item.class);
        items.forEach(System.out::println);
    }

    /**
     * 条件查询部分数据
     * 标准的关键字搜索
     */
    @Test
    public void match(){
        SearchQuery searchQuery = new NativeSearchQuery(
                QueryBuilders.matchQuery("title","手机")
        );
        List<Item> items = elasticsearchRestTemplate.queryForList(searchQuery, Item.class);
        items.forEach(System.out::println);
    }

    /**
     * 搜索全部数据
     */
    @Test
    public void matchAllData(){
        /**
         * searchQuery - 是Spring data elasticsearch 中定义的一个搜索接口
         * NativeSearchQuery 是searchQuery的实现类
         * 构造的时候 需要提供queryBuilds类型的对象
         * queryBuilds是elasticsearch的Java客户端定义的搜索条件类型
         *
         *  queryBuilds 是queryBuild类型的工具类，可以快速实现queryBuilder类型对象的创建
         *  工具类中提供了大量的静态方法 方法命名和DSL搜索中的条件关键字相关
         *  如 match_all 对应 matchAllQuery()
         *  如 match 对应 matchQuery()
         *  如 range 对应rangeQuery()
         */
        SearchQuery  query = new NativeSearchQuery(
                QueryBuilders.matchAllQuery()
        );
        List<Item> items = elasticsearchRestTemplate.queryForList(query, Item.class);
        items.forEach(System.out::println);
    }


    /**
     * 更新数据
     * 如果是全量替换可以使用index方法实现 只要主键在索引中存在 就是全量替换
     * 如果是部分修改 则可以使用update实现
     */
    @Test
    public void updateDocumentData() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index("ego_item");
        updateRequest.doc(
                XContentFactory.jsonBuilder()
                .startObject()
                .field("title","测试update更新数据")
                .endObject()
        );
        UpdateQuery query = new UpdateQueryBuilder()
                .withUpdateRequest(updateRequest)
                .withClass(Item.class)
                .withId("y3ekW3cByHJp0mARonj2")
                .build();
        elasticsearchRestTemplate.update(query);
    }


    /**
     * 删除数据
     *
     */
    @Test
    public void deleteDocumentData(){
        //根据主键删除数据
        String isDelete = elasticsearchRestTemplate.delete(Item.class, "zXekW3cByHJp0mARonj2");
        System.out.println(isDelete);

        //根据搜索结果删除数据
        DeleteQuery query = new DeleteQuery();
        //query.setIndex("ego_item");  可以设置查询索引
        QueryBuilder queryBuilds = QueryBuilders.matchQuery("title","update");
        query.setQuery(queryBuilds);
        elasticsearchRestTemplate.delete(query,Item.class);
    }

    /**
     * 批量新增数据
     * _bulk操作
     */
    @Test
    public void insertBatchDocumentData(){
        List<IndexQuery> list = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Item item = new Item();
            item.setId(UUID.randomUUID().toString().replaceAll("-","")+"-"+i);
            item.setTitle("ElasticSearch In Action");
            item.setSellPoint("ElasticSearch 系列书籍 一本非常好的学习手册 缺点经常卖断货"+i);
            item.setPrice(99L);
            item.setNum(99);
            list.add(new IndexQueryBuilder().withObject(item).build());
        }
        elasticsearchRestTemplate.bulkIndex(list);
    }

    /**
     * 新增数据到ES
     */
    @Test
    public void InsertDocumentData(){
        //相当于使用PUT请求 实现数据的新增
        Item item = new Item();
        item.setId(UUID.randomUUID().toString().replaceAll("-",""));
        item.setTitle("ElasticSearch In Action");
        item.setSellPoint("ElasticSearch 系列书籍 一本非常好的学习手册 缺点经常卖断货");
        item.setPrice(99L);
        item.setNum(99);
        IndexQuery indexQuery = new IndexQueryBuilder() //创建一个indexQuery构建器
                .withObject(item) //设置需要新增的java对象
                .build();//构建indexQuery类型的对象
        String isAdd = elasticsearchRestTemplate.index(indexQuery);
        System.out.println(isAdd);

    }

    /**
     * 创建索引 并设置映射
     * 需要通过两次访问实现，1.创建索引 2.设置映射
     */
    @Test
    public void createIndexWithElasticsearchRestTemplate(){
        //创建索引根据@Document注解创建
        boolean isCreated = elasticsearchRestTemplate.createIndex(Item.class);
        System.out.println(isCreated);
        //创建映射根据@Field注解创建
        boolean isCreateMapping = elasticsearchRestTemplate.putMapping(Item.class);
        System.out.println(isCreateMapping);
    }

    /**
     * 删除索引
     */
    @Test
    public void deleteIndex(){
        //扫描Item类型上的@Document注解
        boolean idDeleted = elasticsearchRestTemplate.deleteIndex(Item.class);
        System.out.println(idDeleted);
        //直接删除对应的索引名称
        boolean deleteIndex = elasticsearchRestTemplate.deleteIndex("default_index");
        System.out.println(deleteIndex);
    }


//    @Autowired
//    private ElasticsearchTemplate elasticsearchTemplate;
    /**
     * 创建索引不包括映射信息 因为只扫描@Document注解
     */
//    @Test
//    public void createIndexWithElasticsearchTemplate(){
//        //创建索引的时候没有映射
//        boolean isCreated = elasticsearchTemplate.createIndex(Item.class);
//        System.out.println(isCreated);
//    }
}

