package com.syong.esjd.service;

import com.alibaba.fastjson.JSON;
import com.syong.esjd.pojo.Content;
import com.syong.esjd.util.HtmlParseUtil;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.naming.directory.SearchResult;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @Description:
 */
@Service
public class ContentService {

    @Resource
    private RestHighLevelClient restHighLevelClient;

    /**
     * 解析数据放入es索引中
     **/
    public Boolean parseContent(String keyword) throws Exception {
        List<Content> contentList = new HtmlParseUtil().parseJD(keyword);

        //将查询到的数据批量放入es中
        BulkRequest bulkRequest = new BulkRequest();
        //设置超时时间
        bulkRequest.timeout("2m");

        for (int i = contentList.size()-4; i > 10; i--) {

            System.out.println(contentList.get(i));

            bulkRequest.add(
                    new IndexRequest("jd_goods")
                    .source(JSON.toJSONString(contentList.get(i)), XContentType.JSON));
        }

        //执行批量处理请求
        BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);

        return !bulkResponse.hasFailures();
    }

    /**
     * 获取放入es中的数据实现搜索功能,并且实现搜索结果分页显示
     **/
    public List<Map<String, Object>> searchPage(String keyword,int pageNo,int pageSize) throws IOException {
        //页面非1优化
        if (pageNo <= 1){
            pageNo = 1;
        }

        //new一个查询请求，绑定索引
        SearchRequest searchRequest = new SearchRequest("jd_goods");

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //设置超时时间
        searchSourceBuilder.timeout(TimeValue.timeValueSeconds(60));

        //按条件精确查询
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("title", keyword);
        searchSourceBuilder.query(termQueryBuilder);

        //分页
        searchSourceBuilder.from(pageNo);
        searchSourceBuilder.size(pageSize);

        //执行搜索
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        //解析结果
        ArrayList<Map<String, Object>> list = new ArrayList<>();
        //结果都封装在searchResponse.getHits().getHits()中，取出即可
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            list.add(hit.getSourceAsMap());
        }

        return list;
    }

    /**
     * 获取放入es中的数据实现搜索功能,并且实现搜索结果分页显示
     * 实现搜索关键字高亮显示
     **/
    public List<Map<String, Object>> searchPageHighlighted(String keyword,int pageNo,int pageSize) throws IOException {
        //页面非1优化
        if (pageNo <= 1){
            pageNo = 1;
        }

        //new一个查询请求，绑定索引
        SearchRequest searchRequest = new SearchRequest("jd_goods");

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //设置超时时间
        searchSourceBuilder.timeout(TimeValue.timeValueSeconds(60));

        //按条件精确查询
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("title", keyword);
        searchSourceBuilder.query(termQueryBuilder);

        //实现高亮搜索
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        //设置需要高亮的字段
        highlightBuilder.field("title");
        //设置高亮前后缀
        highlightBuilder.preTags("<span style='color:red'>");
        highlightBuilder.postTags("</span>");
        //关闭多个关键字高亮显示
        highlightBuilder.requireFieldMatch(false);
        searchSourceBuilder.highlighter(highlightBuilder);

        //分页
        searchSourceBuilder.from(pageNo);
        searchSourceBuilder.size(pageSize);

        //执行搜索
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        //解析结果
        ArrayList<Map<String, Object>> list = new ArrayList<>();
        //结果都封装在searchResponse.getHits().getHits()中，取出即可
        for (SearchHit hit : searchResponse.getHits().getHits()) {

            //解析高亮字段
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            //得到高亮的title字段
            HighlightField title = highlightFields.get("title");

            //取得原来的结果map
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            //将得到的高亮的title字段，替换到原来的map中的title字段
            if (title != null){
                //取得title
                Text[] fragments = title.fragments();
                System.out.println(Arrays.toString(title.fragments()));
                String newTitle = "";
                for (Text text : fragments) {
                    newTitle += text;
                }
                //将原来的title替换
                sourceAsMap.put("title",newTitle);
            }
            list.add(sourceAsMap);
        }

        return list;
    }


}
