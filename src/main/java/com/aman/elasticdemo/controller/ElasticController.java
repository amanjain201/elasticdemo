/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.aman.elasticdemo.controller;

import com.aman.elasticdemo.elasticops.ElasticOps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 *
 * @author amanjain
 */
@RestController
public class ElasticController {
    
    @Autowired
    ElasticOps operations;
    
    @RequestMapping(value = "/push", method = RequestMethod.GET, produces = "text/plain")
    public ResponseEntity pushDataToElastic() {
        String value = operations.pushDataToElastic("posts");
        return new ResponseEntity(value, HttpStatus.OK);
    }
    
    @RequestMapping(value = "/bulkpush", method = RequestMethod.GET, produces = "text/plain")
    public ResponseEntity pushBulkDataToElastic() {
        String value = operations.pushBulkDataToElastic("posts");
        return new ResponseEntity(value, HttpStatus.OK);
    }
}
