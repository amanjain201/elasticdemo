/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.aman.elasticdemo.elasticops;

import java.io.IOException;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 *
 * @author amanjain
 */
@Service
public class ElasticOps {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(ElasticOps.class);

    @Autowired
    private RestHighLevelClient client;

    public String pushDataToElastic(String indexName) {
        XContentBuilder builder = getFormattedContentToPush();
        IndexRequest indexRequest = new IndexRequest(indexName, "doc", "1")
                .source(builder);

        try {
            IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
            logger.info("Generated Id: " + response.getId());
            return response.getId();
        } catch (IOException ex) {
            Logger.getLogger(ElasticOps.class.getName()).log(Level.SEVERE, null, ex);
        }
        return "failed to push";
    }

    public String pushBulkDataToElastic(String indexName) {
        IndexRequest indexRequest;
//        BulkProcessor.Listener listener = getBulkProcessorListener();
        boolean terminated =false;
        try (BulkProcessor bulkProcessor = getBulkProcessor()) {
            
            for (int i = 1; i <= 10000; i++) {
                XContentBuilder builder = getFormattedContentToPush();
                indexRequest = new IndexRequest(indexName, "doc", "" + i)
                        .source(builder);
                bulkProcessor.add(indexRequest);
            }
            try {
                terminated = bulkProcessor.awaitClose(30L, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
                logger.error("Exception " + ex);
            }
        } catch (Exception ex) {
            logger.debug("Exception  in bulk push ", ex);
        }
        if(terminated) {
            return "Bulk Push Successful..";
        }
        return "Bulk Push failed..";
    }
    
    

    
    
    /*---------------------------- Utility functions--------------------------*/
    private XContentBuilder getFormattedContentToPush() {
        XContentBuilder builder = null;
        String randomValue = UUID.randomUUID().toString();
//        logger.info("Random value: " + randomValue);
        try {
            builder = XContentFactory.jsonBuilder();
            builder.startObject();
            builder.field("name", randomValue);
            builder.timeField("postDate", "postDate", new Date().getTime());
            builder.field("message", "trying out Elasticsearch for " + randomValue);
            builder.endObject();
        } catch (IOException ex) {
            Logger.getLogger(ElasticOps.class.getName()).log(Level.SEVERE, null, ex);
        }
        return builder;
    }

    private BulkProcessor.Listener getBulkProcessorListener() {
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                int numberOfActions = request.numberOfActions();
                logger.info("Executing bulk [{}] with {} requests",
                        executionId, numberOfActions);
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                if (response.hasFailures()) {
                    logger.info("Bulk [{}] executed with failures", executionId);
                } else {
                    logger.info("Bulk [{}] completed in {} milliseconds",
                            executionId, response.getTook().getMillis());
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                logger.error("Failed to execute bulk", failure);
            }
        };
        return listener;
    }
    
    private BulkProcessor getBulkProcessor() {
       BulkProcessor.Listener listener = getBulkProcessorListener(); 
       BulkProcessor bulkProcessor = BulkProcessor.builder(
                (bulkRequest, bulkListener) -> client.bulkAsync(bulkRequest, RequestOptions.DEFAULT, bulkListener), listener).setBulkActions(5000).build();
       return bulkProcessor;
    }
}
