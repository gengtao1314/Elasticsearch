package com.gtaotao;

import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Hello world!
 *
 */
public class App 
{
    /*public static void main( String[] args )
    {
        String  ES_CONNECT_IP="192.168.0.238";
        String port = "9200";
        String ES_CLUSTER_NAME = "my-application";
        String ITEM_INDEX = "elasticsearch";
        String ITEM_TYPE = "user";
        try {
            Settings settings = Settings.builder().put("cluster.name", ES_CLUSTER_NAME).build();
            TransportClient client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new TransportAddress(InetAddress.getByName(ES_CONNECT_IP), 9300));

            IndexResponse indexResponse = client.prepareIndex("test", "articles","2")
                    .setSource(jsonBuilder()
                    .startObject()
                    .field("userId","2")
                    .field("userCode","1902181cf5cbd83ff649")
                    .field("userName","张三")
                    .field("workNumber","001")
                    .field("deptName","市场部")
                    .endObject()
                    ).get();
            client.close();
        }catch (Exception e){
            System.out.println(e.getMessage());
        }

        System.out.println( "Hello World!" );
    }*/

    /**
     * 插入数据
     */
    public  static  void  index(){
        try {
            //RestHighLevelClient实例需要低级客户端构建器来构建
            RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("192.168.0.238", 9200, "http")));
            //7.0版本放弃了type
           // IndexRequest indexRequest = new IndexRequest("demo6","demo6");
            IndexRequest indexRequest = new IndexRequest("demo6");
            JSONObject obj=new JSONObject();
            obj.put("title","美国留给伊拉克的是个烂摊子吗66");
            obj.put("type","text");
            indexRequest.source(obj.toJSONString(),XContentType.JSON);
            //添加索引
            client.index(indexRequest,RequestOptions.DEFAULT);
            client.close();

            //http://localhost:9200/demo/demo/_search  浏览器运行查询数据
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 批量插入数据
     */
    public static  void  bacthIndex(){
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
        List<IndexRequest> requests = new ArrayList<>();
        requests.add(generateNewsRequest("中印边防军于拉达克举行会晤 强调维护边境和平", "2018-01-27T08:34:00Z"));
        BulkRequest bulkRequest = new BulkRequest();
        for (IndexRequest indexRequest : requests) {
            bulkRequest.add(indexRequest);
        }
        try {
            client.bulk(bulkRequest,RequestOptions.DEFAULT);
            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static IndexRequest generateNewsRequest(String title,String publishTime){
        IndexRequest indexRequest = new IndexRequest("demo3", "demo3");
        JSONObject obj=new JSONObject();
        obj.put("title",title);
        obj.put("time",publishTime);
        indexRequest.source(obj.toJSONString(),XContentType.JSON);
        return indexRequest;
    }

    /**
     * 查询操作
     * https://blog.csdn.net/paditang/article/details/78802799
     * https://blog.csdn.net/A_Story_Donkey/article/details/79667670
     * https://www.cnblogs.com/wenbronk/p/6432990.html
     */
    public static  void  queryTest(){
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("192.168.0.238", 9200, "http")));
        // 这个sourcebuilder就类似于查询语句中最外层的部分。包括查询分页的起始，
        // 查询语句的核心，查询结果的排序，查询结果截取部分返回等一系列配置
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        try {
            // 结果开始处
            sourceBuilder.from(0);
            // 查询结果终止处
            sourceBuilder.size(2);
            // 查询的等待时间
            sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

            /**
             * 使用QueryBuilder
             * termQuery("key", obj) 完全匹配
             * termsQuery("key", obj1, obj2..)   一次匹配多个值
             * matchQuery("key", Obj) 单个匹配, field不支持通配符, 前缀具高级特性
             * multiMatchQuery("text", "field1", "field2"..);  匹配多个字段, field有通配符忒行
             * matchAllQuery();         匹配所有文件
             */
       //     MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("title", "国人");

            //分词精确查询
            TermQueryBuilder matchQueryBuilder = QueryBuilders.termQuery("title", "伊拉克");


//            // 查询在时间区间范围内的结果  范围查询
//            RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("publishTime");
//            rangeQueryBuilder.gte("2018-01-26T08:00:00Z");
//            rangeQueryBuilder.lte("2018-01-26T20:00:00Z");

            // 等同于bool，将两个查询合并
            BoolQueryBuilder boolBuilder = QueryBuilders.boolQuery();
            boolBuilder.must(matchQueryBuilder);
//            boolBuilder.must(termQueryBuilder);
//            boolBuilder.must(rangeQueryBuilder);
            sourceBuilder.query(boolBuilder);

            // 排序
//            FieldSortBuilder fsb = SortBuilders.fieldSort("date");
//            fsb.order(SortOrder.DESC);
//            sourceBuilder.sort(fsb);


            SearchRequest searchRequest = new SearchRequest("demo6");
            //7.0版本放弃了type
      //      searchRequest.types("demo6");
            searchRequest.source(sourceBuilder);
            SearchResponse response = client.search(searchRequest,RequestOptions.DEFAULT);
            System.out.println(response);
            client.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }




    public static void main(String[] args) {
        queryTest();
    //    index();
    }

}
