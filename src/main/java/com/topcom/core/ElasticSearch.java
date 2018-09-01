package com.topcom.core;

import com.topcom.util.EsClient;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;

import java.io.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @function: ES集群数据导出导入关键方法
 * @auther: Create by wyf
 * @date: 2018/8/31
 * @version: v1.0
 */
public class ElasticSearch {
    
    /**
     * es数据导出
     *
     * @throws Exception
     */
    public static void outToFile() throws Exception {

        TransportClient client = EsClient.getEsClient(EsClient.esClusterName, EsClient.esHost);
        String indexName = EsClient.indexFromName;
        String typeName = EsClient.indexTypeName;
        String filePath = EsClient.filePath;
        SearchRequestBuilder builder = client.prepareSearch(indexName);

        if (typeName != null) {
            builder.setTypes(typeName);
        }
        builder.setQuery(QueryBuilders.matchAllQuery());
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        if (EsClient.publicTimeStart != null && EsClient.publicTimeEnd != null) {
            RangeQueryBuilder query1 = QueryBuilders.rangeQuery("pubTime").gte(EsClient.publicTimeStart);
            RangeQueryBuilder query2 = QueryBuilders.rangeQuery("pubTime").lte(EsClient.publicTimeEnd);
            boolQueryBuilder.must(query1).must(query2);
            builder.setQuery(boolQueryBuilder);
        }
        if (StringUtils.isNotEmpty(EsClient.keyWords)) {
            QueryStringQueryBuilder query3 = QueryBuilders.queryStringQuery(EsClient.keyWords);
            builder.setQuery(query3);
        }
        builder.setSize(10000);
        builder.setScroll(new TimeValue(6000));
        SearchResponse scrollResp = builder.execute().actionGet();
        try {
            //把导出的结果以JSON的格式写到文件里
            BufferedWriter out = new BufferedWriter(new FileWriter(filePath, true));
            long count = 0;
            while (true) {
                int sum = 0;
                for (SearchHit hit : scrollResp.getHits().getHits()) {
                    String json = hit.getSourceAsString();
                    if (StringUtils.isNotEmpty(json) && !"".equals(json)) {
                        out.write(json);
                        out.write("\r\n");
                        count++;
                        sum++;
                    }
                }
                scrollResp = client.prepareSearchScroll(scrollResp.getScrollId())
                        .setScroll(new TimeValue(6000)).execute().actionGet();
                if (scrollResp.getHits().getHits().length == 0) {
                    break;
                }
                System.out.println("本次写入数据:" + sum);
            }
            System.out.println("总共写入数据:" + count);
            out.close();
            EsClient.close(client);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * es数据导入
     *
     * @throws Exception
     */
    public static void fileToEs() throws Exception {

        TransportClient client = EsClient.getEsClient(EsClient.esClusterName, EsClient.esHost);
        String indexName = EsClient.indexToName;
        String typeName = EsClient.indexTypeName;
        String filePath = EsClient.filePath;

        try {
            //把导出的结果以JSON的格式写到文件里
            BufferedReader br = new BufferedReader(new FileReader(filePath));
            String json = null;
            int count = 0;
            //开启批量插入
            BulkRequestBuilder bulkRequest = client.prepareBulk();
            while ((json = br.readLine()) != null) {
                bulkRequest.add(client.prepareIndex(indexName, typeName).setSource(json));
                //每一千条提交一次
                count++;
                if (count % 1000 == 0) {
                    System.out.println("本次入库数据1000条");
                    BulkResponse bulkResponse = bulkRequest.execute().actionGet();
                    if (bulkResponse.hasFailures()) {
                        System.out.println("message:" + bulkResponse.buildFailureMessage());
                    }
                    //重新创建一个bulk
                    bulkRequest = client.prepareBulk();
                }
            }
            bulkRequest.execute().actionGet();
            System.out.println("总共入库数据：" + count);
            br.close();
            EsClient.close(client);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * es集群间index数据传输
     *
     * @throws Exception
     */
    public static void esToEs() throws Exception {

        String srcClusterName = EsClient.esClusterName;
        String srcIndexName = EsClient.srcIndexName;
        String srcEsHost = EsClient.esHost;

        String tagIndexName = EsClient.tagIndexName;
        String tagClusterName = EsClient.esTagClusterName;
        String tagEsHost = EsClient.esTagHost;
        String tagTypeName = EsClient.indexTypeName;

        TransportClient srcClient = EsClient.getEsClient(srcClusterName, srcEsHost);

        TransportClient tagClient = EsClient.getEsClient(tagClusterName, tagEsHost);

        SearchResponse scrollResp = srcClient.prepareSearch(srcIndexName)
                .setScroll(new TimeValue(1000))
                .setSize(1000)
                .execute().actionGet();

        BulkRequestBuilder bulk;

        ExecutorService executor = Executors.newFixedThreadPool(5);
        while (true) {
            bulk = tagClient.prepareBulk();
            final BulkRequestBuilder bulk_new = bulk;
            System.out.println("查询条数=" + scrollResp.getHits().getHits().length);
            for (SearchHit hit : scrollResp.getHits().getHits()) {
                IndexRequest req = tagClient.prepareIndex().setIndex(tagIndexName)
                        .setType(tagTypeName).setSource(hit.getSource()).request();
                bulk_new.add(req);
            }
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    bulk_new.execute();
                }
            });
            Thread.sleep(1000);
            scrollResp = srcClient.prepareSearchScroll(scrollResp.getScrollId())
                    .setScroll(new TimeValue(1000)).execute().actionGet();
            if (scrollResp.getHits().getHits().length == 0) {
                break;
            }
        }
        //该方法在加入线程队列的线程执行完之前不会执行
        executor.shutdown();
        System.out.println("执行结束");
        EsClient.close(tagClient);
        EsClient.close(srcClient);
    }
}
