/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.sink.elasticsearch.client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.sink.elasticsearch.ElasticSearchEventSerializer;
import org.apache.flume.sink.elasticsearch.IndexNameBuilder;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig.Builder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.codehaus.jettison.json.JSONObject;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder.RequestConfigCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.gson.Gson;

/**
 * Rest ElasticSearch client which is responsible for sending bulks of events to
 * ElasticSearch using ElasticSearch HTTP API. This is configurable, so any
 * config params required should be taken through this.
 */
public class ElasticSearchRestClient implements ElasticSearchClient {

  private static final String INDEX_OPERATION_NAME = "index";
  private static final String INDEX_PARAM = "_index";
  private static final String TYPE_PARAM = "_type";
  private static final String BULK_ENDPOINT = "_bulk";
  private static final Header[] HEADS = {
      new BasicHeader("Content-Type", "application/json;charset=UTF-8")};

  private static final Logger logger = LoggerFactory
      .getLogger(ElasticSearchRestClient.class);

  private final ElasticSearchEventSerializer serializer;
  // private final RoundRobinList<String> serversList;

  private StringBuilder bulkBuilder;
  private RestClient restClient;

  public ElasticSearchRestClient(String[] hostNames,
      ElasticSearchEventSerializer serializer) {
    ArrayList<HttpHost> hosts = Lists.newArrayList();

    for (int i = 0; i < hostNames.length; ++i) {
      HttpHost hh = HttpHost.create(hostNames[i]);
      hosts.add(hh);
    }
    this.serializer = serializer;

    // serversList = new RoundRobinList<String>(Arrays.asList(hostNames));
    restClient = RestClient.builder(hosts.toArray(new HttpHost[hosts.size()]))
        .setRequestConfigCallback(new RequestConfigCallback() {
          @Override
          public Builder customizeRequestConfig(Builder builder) {
            return builder.setConnectTimeout(1000).setSocketTimeout(5000);
          }
        }).build();
    bulkBuilder = new StringBuilder();
  }

  @VisibleForTesting
  public ElasticSearchRestClient(String[] hostNames,
      ElasticSearchEventSerializer serializer, RestClient client) {
    this(hostNames, serializer);
    restClient = client;
  }

  @Override
  public void configure(Context context) {}

  @Override
  public void close() {
    try {
      this.restClient.close();
    } catch (Exception e) {
      logger.error("skip this exception:" + ExceptionUtils.getStackTrace(e));
    }
  }

  @Override
  public void addEvent(Event event, IndexNameBuilder indexNameBuilder,
      String indexType, long ttlMs) throws Exception {
    try {
      JSONObject content = serializer.getContent(event);
      if (content == null || StringUtils.isBlank(content.toString())) {
        logger.info("Skip null content:" + content);
        return;
      }
      logger.debug("content is:" + content);

      Map<String,Map<String,String>> parameters = new HashMap<String,Map<String,String>>();
      Map<String,String> indexParameters = new HashMap<String,String>();
      indexParameters.put(INDEX_PARAM, indexNameBuilder.getIndexName(event));
      indexParameters.put(TYPE_PARAM, indexType);
      parameters.put(INDEX_OPERATION_NAME, indexParameters);

      Gson gson = new Gson();
      synchronized (bulkBuilder) {
        bulkBuilder.append(gson.toJson(parameters));
        bulkBuilder.append("\n");
        bulkBuilder.append(content.toString());
        bulkBuilder.append("\n");
      }
    } catch (Exception e) {
      logger.error("skip this event:" + e.getMessage());
      //      e.printStackTrace();
    }
  }

  @Override
  public void execute() throws Exception {
    int statusCode = 0;
    Response response = null;
    String entity;
    synchronized (bulkBuilder) {
      entity = bulkBuilder  .toString();
      bulkBuilder = new StringBuilder();
    }
    //    logger.info("entity is:" + entity.toString());
    response = restClient.performRequest("POST", BULK_ENDPOINT,
        Collections.<String,String> emptyMap(),
        new StringEntity(entity, ContentType.APPLICATION_JSON), HEADS);
    statusCode = response.getStatusLine().getStatusCode();
    logger.info("Status code from elasticsearch: " + statusCode);
    if (response.getEntity() != null) {
      logger.debug("Status message from elasticsearch: " + response.toString());
    }
  }
}
