package com.topcom.main;

import com.topcom.core.ElasticSearch;

/**
 * @function: 主方法
 * @auther: Create by wyf
 * @date: 2018/9/1
 * @version: v1.0
 */
public class ElasticsearchMain {

    public static void main(String[] args) {

        System.out.println("ES数据操作：1. out  2. in  3. es2es (out数据导出到本地，in数据导入到集群，es2es数据集群传递)");
        if (args.length != 1) {
            System.out.println("输入参数不正确");
            System.exit(1);
        }
        if ("out".equalsIgnoreCase(args[0])) {
            try {
                ElasticSearch.outToFile();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        if ("in".equalsIgnoreCase(args[0])) {
            try {
                ElasticSearch.fileToEs();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        if ("es2es".equalsIgnoreCase(args[0])) {
            try {
                ElasticSearch.esToEs();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
