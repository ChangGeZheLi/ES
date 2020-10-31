package com.syong.es_api;

import com.alibaba.fastjson.JSON;
import com.syong.es_api.pojo.User;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;

@SpringBootTest
class EsApiApplicationTests {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @Test
    public void test1() throws IOException {
        //创建索引请求
        CreateIndexRequest request = new CreateIndexRequest("kuang_index");
        //客户端执行请求，请求后获得响应
        CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);

        System.out.println(createIndexResponse);
    }


    @Test
    public void testGetIndex() throws IOException {
        //获得索引，并且判断是否存在
        GetIndexRequest request = new GetIndexRequest("kuang_index");
        boolean exists = restHighLevelClient.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    @Test
    public void testDeleteIndex() throws IOException {
        //删除索引
        DeleteIndexRequest request = new DeleteIndexRequest("kuang_index");
        AcknowledgedResponse delete = restHighLevelClient.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println(delete.isAcknowledged());
    }


    //测试添加文档
    @Test
    void testAddDocument() throws IOException {
        //创建对象
        User user = new User("zhangsan", 23);
        //创建请求
        IndexRequest request = new IndexRequest("kuangshen");
        
        //设置规则
        request.id("1");
        request.timeout(TimeValue.timeValueSeconds(3));
        request.timeout("3s");

        //使用阿里的fastjson工具，将数据放入请求，需要先将对象转换为json
        request.source(JSON.toJSONString(user), XContentType.JSON);

        //客户端发送请求，获取响应的结果
        IndexResponse indexResponse = restHighLevelClient.index(request, RequestOptions.DEFAULT);

        System.out.println(indexResponse.toString());
        System.out.println(indexResponse.status());
    }

    //测试文档是否存在
    @Test
    void testExists() throws IOException {
        GetRequest getRequest = new GetRequest("kuangshen", "1");
        //不读取返回的_source的上下文
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");

        boolean exists = restHighLevelClient.exists(getRequest, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    //获得文档内容
    @Test
    void testGetDocument() throws IOException {
        GetRequest request = new GetRequest("kuangshen", "1");
        GetResponse response = restHighLevelClient.get(request, RequestOptions.DEFAULT);

        System.out.println(response.getSourceAsString());
        System.out.println(response);
    }

    //更新
    @Test
    void testUpdateDocument() throws IOException {
        //获取更新请求对象
        UpdateRequest request = new UpdateRequest("kuangshen", "1");

        //更新内容通过user对象转json传递
        User user = new User("lisi", 33);
        request.doc(JSON.toJSONString(user),XContentType.JSON);

        UpdateResponse response = restHighLevelClient.update(request, RequestOptions.DEFAULT);

        System.out.println(response.toString());
    }


    //删除文档信息
    @Test
    void testDelete() throws IOException {
        DeleteRequest request = new DeleteRequest("kuangshen", "1");
        request.timeout("1s");

        DeleteResponse response = restHighLevelClient.delete(request, RequestOptions.DEFAULT);
        System.out.println(response);
    }

    //批量插入请求
    @Test
    void testBulkRequest() throws IOException {
        //创建批量处理对象
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("10s");

        //批量处理数据源
        ArrayList<User> users = new ArrayList<>();
        users.add(new User("zhangsan1",11));
        users.add(new User("zhangsan2",11));
        users.add(new User("zhangsan3",11));
        users.add(new User("zhangsan4",11));
        users.add(new User("zhangsan5",11));
        users.add(new User("zhangsan6",11));
        users.add(new User("zhangsan7",11));

        for (int i = 0; i < users.size(); i++) {
            bulkRequest.add(
                    new IndexRequest("kuang_index")
                    .id(""+(i+1))
                    .source(JSON.toJSONString(users.get(i)),XContentType.JSON)
            );

            BulkResponse responses = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
            System.out.println(responses.status());
        }
    }


    //测试查询
    @Test
    void testSearch() throws IOException {
        //创建查询请求对象
        SearchRequest searchRequest = new SearchRequest("kuang_index");
        //构造查询条件
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //查询条件使用QueryBuilders工具类来实现
        //QueryBuilders.termQuery 精确查询
        //QueryBuilders.matchAllQuery 匹配所有
        //HighlightBuilder 构建高亮显示
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name","zhangsan1");

        searchSourceBuilder.query(termQueryBuilder);
        //设置超时时长
        searchSourceBuilder.timeout(TimeValue.timeValueSeconds(3));

        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(JSON.toJSONString(searchResponse.getHits()));
        System.out.println("=========================================");

        for (SearchHit hit : searchResponse.getHits().getHits()) {
            System.out.println(hit.getSourceAsMap());
        }
    }

}
