package com.topcom.util;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.Properties;

/**
 * @function: es数据库获取连接工具类
 * @auther: Create by wyf
 * @date: 2018/8/31
 * @version: v1.0
 */
public class EsClient {

    public static String esHost = null;
    public static String esClusterName = null;
    public static String indexTypeName = null;
    public static String indexFromName = null;
    public static String indexToName = null;
    public static String filePath = null;
    public static Long publicTimeStart = null;
    public static Long publicTimeEnd = null;
    public static String keyWords = null;
    public static String srcIndexName = null;
    public static String tagIndexName = null;
    public static String esTagHost = null;
    public static String esTagClusterName = null;

    static {
        try {
            Properties properties = new Properties();
            properties.load(new InputStreamReader(EsClient.class.getClassLoader().getResourceAsStream("application.properties"), "UTF-8"));
            esHost = properties.getProperty("elasticsearch.host.ip");
            esClusterName = properties.getProperty("elasticsearch.cluster.name");
            indexTypeName = properties.getProperty("elasticsearch.index.type.name");
            indexFromName = properties.getProperty("elasticsearch.index.from.name");
            filePath = properties.getProperty("elasticsearch.data.out.path");
            publicTimeStart = Long.valueOf(properties.getProperty("elasticsearch.pubTime.start"));
            publicTimeEnd = Long.valueOf(properties.getProperty("elasticsearch.pubTime.end"));
            keyWords = properties.getProperty("elasticsearch.keywords.string");
            indexToName = properties.getProperty("elasticsearch.index.to.name");
            srcIndexName = properties.getProperty("elasticsearch.srcIndex.name");
            tagIndexName = properties.getProperty("elasticsearch.tagIndex.name");
            esTagHost = properties.getProperty("elasticsearch.host.tagIp");
            esTagClusterName = properties.getProperty("elasticsearch.cluster.tagName");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取es连接
     *
     * @param clusterName
     * @param clusterHost
     * @return
     * @throws Exception
     */
    public static TransportClient getEsClient(String clusterName, String clusterHost) throws Exception {
        // 配置信息
        Settings esSetting = Settings.settingsBuilder()
                .put("cluster.name", clusterName)
                .build();

        String[] esHostArr = null;
        TransportClient client = null;
        //配置es集群
        if (StringUtils.isNotEmpty(clusterHost)) {
            esHostArr = clusterHost.split(",", -1);
        }
        if (client == null) {
            synchronized (EsClient.class) {
                client = TransportClient.builder().settings(esSetting).build();
                for (String esHostIp : esHostArr) {
                    //把每一个es的ip加入到client
                    String[] split = esHostIp.split(":", -1);
                    String ip = split[0];
                    Integer port = Integer.valueOf(split[1]);
                    InetSocketTransportAddress inetSocketTransportAddress = new InetSocketTransportAddress(InetAddress.getByName(ip),
                            port);
                    client.addTransportAddress(inetSocketTransportAddress);
                }
            }
        }

        return client;
    }

    /**
     * 关闭es连接
     *
     * @param client
     */
    public static void close(TransportClient client) {

        if (client != null) {
            client.close();
        }
    }

    public static void main(String[] args) throws Exception {
        TransportClient esClient = getEsClient(esClusterName, esHost);
        System.out.println(esClient);
        close(esClient);
    }
}
