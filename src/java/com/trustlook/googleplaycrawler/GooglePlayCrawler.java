/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.trustlook.googleplaycrawler;

import java.util.*;
import java.text.*;

// Commons Logging imports
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.*;
import org.apache.nutch.parse.ParseSegment;
import org.apache.nutch.indexer.solr.SolrDeleteDuplicates;
import org.apache.nutch.indexer.solr.SolrIndexer;
import org.apache.nutch.util.HadoopFSUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;

import org.apache.nutch.fetcher.Fetcher;

public class GooglePlayCrawler extends Configured implements Tool {
  public static final Logger LOG = LoggerFactory.getLogger(GooglePlayCrawler.class);

  /* Perform complete crawling and indexing (to Solr) given a set of root urls and the -solr
     parameter respectively. More information and Usage parameters can be found below. */
  public static void main(String args[]) throws Exception {
    Configuration conf = NutchConfiguration.create();
    int res = ToolRunner.run(conf, new GooglePlayCrawler(), args);
    System.exit(res);
  }
  
  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 1) {
      System.out.println
      ("Usage: Crawl <urlDir> [-dir d] [-depth i] [-numFetchers n]");
      return -1;
    }
    Path rootUrlDir = null;
    Path dir = new Path("nutchdb");
    int threads = getConf().getInt("fetcher.threads.fetch", 10);
    int depth = 2;
    int numFetchers = 200;
    
    for (int i = 0; i < args.length; i++) {
      if ("-dir".equals(args[i])) {
        dir = new Path(args[i+1]);
        i++;
      } else if ("-depth".equals(args[i])) {
        depth = Integer.parseInt(args[i+1]);
        i++;
      } else if ("-numFetchers".equals(args[i])) {
          numFetchers = Integer.parseInt(args[i+1]);
          i++;
      } else if (args[i] != null) {
        rootUrlDir = new Path(args[i]);
      }
    }
    
    JobConf job = new NutchJob(getConf());

    if (LOG.isInfoEnabled()) {
      LOG.info("crawl started in: " + dir);
      LOG.info("rootUrlDir = " + rootUrlDir);
      LOG.info("depth = " + depth);      
      LOG.info("numFetchers =" + numFetchers);
    }
    
    Path crawlDb = new Path("nutchdb");
    Path segments = new Path(dir + "/segments");

    Injector injector = new Injector(getConf());
    Generator generator = new Generator(getConf());
    Fetcher fetcher = new Fetcher(getConf());
    ParseSegment parseSegment = new ParseSegment(getConf());
    CrawlDb crawlDbTool = new CrawlDb(getConf());
      
    // initialize crawlDb
    injector.inject(crawlDb, rootUrlDir);
    int i;
    for (i = 0; i < depth; i++) {             // generate new segment
      Path[] segs = generator.generate(crawlDb, segments, numFetchers, Long.MAX_VALUE, System
          .currentTimeMillis());
      if (segs == null) {
        LOG.info("Stopping at depth=" + i + " - no more URLs to fetch.");
        break;
      }
      fetcher.fetch(segs[0], threads);  // fetch it
      if (!Fetcher.isParsing(job)) {
        parseSegment.parse(segs[0]);    // parse it, if needed
      }
      crawlDbTool.update(crawlDb, segs, true, true); // update crawldb
    }
    if (i == 0) {
      LOG.warn("No URLs to fetch - check your seed list and URL filters.");
    }
    if (LOG.isInfoEnabled()) { LOG.info("crawl finished: " + dir); }
    return 0;
  }


}
