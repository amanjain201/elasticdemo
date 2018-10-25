/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.aman.elasticdemo.elasticconfig;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.AbstractFactoryBean;
import org.springframework.context.annotation.Configuration;

/**
 *
 * @author amanjain
 */
@Configuration
public class ElasticSearchConfig extends AbstractFactoryBean<Object> {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchConfig.class);

    @Value("${spring.data.elasticsearch.cluster-name}")
    private String clusterName;
    @Value("${spring.data.elasticsearch.cluster-ip}")
    private String clusterIP;
    @Value("${spring.data.elasticsearch.cluster-port}")
    private int clusterPort;
    
    private RestHighLevelClient restHighLevelClient;

    @Override
    public void destroy() {
        try {
            if (restHighLevelClient != null) {
                restHighLevelClient.close();
            }
        } catch (final Exception e) {
            LOG.error("Error closing ElasticSearch client: ", e);
        }
    }

    @Override
    public Class<RestHighLevelClient> getObjectType() {
        return RestHighLevelClient.class;
    }

    @Override
    public boolean isSingleton() {
        return false;
    }

    @Override
    public RestHighLevelClient createInstance() {
        return buildClient();
    }
    
    private RestHighLevelClient buildClient() {
        try {
            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(
                            new HttpHost(clusterIP, clusterPort, "http")));
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        return restHighLevelClient;
    }

}
