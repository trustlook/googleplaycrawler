package org.apache.nutch.parse.googleplay;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.parse.Parser;
import org.apache.nutch.protocol.Content;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GoogleplayParser implements Parser {
    public static final Logger LOG = LoggerFactory.getLogger("org.apache.nutch.parse.googleplay");
    static Pattern appUrlPattern = Pattern.compile("https://play.google.com/store/apps/details\\?id=[a-zA-Z0-9\\._]+");
    static Pattern titlePattern = Pattern.compile("<title.*?>(.*?)</title>");
    static Pattern appNamePattern= Pattern.compile("<div class=\"document-title\" itemprop=\"name\"> <div.*?>(.*?)</div");
    static Pattern linkPattern = Pattern.compile("href=\"/store/apps/details\\?id=([a-zA-Z0-9\\._]+)");
    static Pattern publisherPattern = Pattern.compile("<meta content=\"/store/apps/developer\\?id=(.*?)\"");
    static Pattern updateTimePattern = Pattern.compile("<div class=\"document-subtitle\">- (.*?)</div>");
    static Pattern categoryPattern = Pattern.compile("<span itemprop=\"genre\">(.*?)</span>");
    static Pattern pricePattern = Pattern.compile("<span class=\"price buy\">  <span>(.*?)<span>(.*?)</span>");
    static Pattern reviewPattern = Pattern.compile("<div class=\"score-container\"(.*?)<meta content=\"(.*?)\" itemprop=\"ratingValue\">(.*?)<meta content=\"(.*)?\" itemprop=\"ratingCount\">");
    static Pattern installPattern = Pattern.compile("<div class=\"content\" itemprop=\"numDownloads\">(.*?)</div>");
    static Pattern versionPattern = Pattern.compile("<div class=\"content\" itemprop=\"softwareVersion\">(.*?)</div>");
    static Pattern ratingPattern = Pattern.compile("<div class=\"content\" itemprop=\"contentRating\">(.*?)</div>");
    static Pattern developerSitePattern = Pattern.compile("<a class=\"dev-link\" href=\"https://www.google.com/url\\?q=(.*?)&");
    static Pattern developerEmailPattern = Pattern.compile("<a class=\"dev-link\" href=\"mailto:(.*?)\"");
    static Pattern descriptionPattern = Pattern.compile("<div class=\"show-more-content text-body\" itemprop=\"description\"> <div class=\"id-app-orig-desc\">(.*?)</div>");

    private Configuration conf;

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public ParseResult getParse(Content content) {
        String thisId = content.getBaseUrl().substring(content.getBaseUrl().indexOf("=")+1);
        byte[] contentInOctets = content.getContent();
        String htmlText = new String(contentInOctets);
        
        Metadata meta = content.getMetadata();
        
        String title = null;
        String appName = null;
        Set<String> ids = new HashSet<String>();
        String publisher = null;
        String updateTime = null;
        String category = null;
        String price = null;
        String reviewScore = null;
        String reviewCount = null;
        String install = null;
        String version = null;
        String rating = null;
        String developerSite = null;
        String developerEmail = null;
        String description = null;
        
        Matcher m = titlePattern.matcher(htmlText);
        if (m.find()) {
            title = m.group(1);
        }
        
        m = linkPattern.matcher(htmlText);
        while (m.find()) {
            if (!m.group(1).equals(thisId)) {
                ids.add(m.group(1));
            }
        }
        List<Outlink> outlinks = new ArrayList<Outlink>();
        for (String id : ids) {
            try {
                outlinks.add(new Outlink("https://play.google.com/store/apps/details?id=" + id, ""));
            } catch (MalformedURLException mue) {
                LOG.warn("Invalid url: '" + id + "', skipping.");
            }
        }
        
        m = appUrlPattern.matcher(content.getBaseUrl());
        if (m.matches()) {  // App page
            m = appNamePattern.matcher(htmlText);
            if (m.find()) {
                appName = m.group(1);
            }
            meta.set("name", appName);
            
            m = publisherPattern.matcher(htmlText);
            if (m.find()) {
                publisher = m.group(1);
            }
            meta.set("publisher", publisher!=null?publisher:"");
            
            m = updateTimePattern.matcher(htmlText);
            if (m.find()) {
                updateTime = m.group(1);
            }
            meta.set("updateTime", updateTime!=null?updateTime:"");
            
            m = categoryPattern.matcher(htmlText);
            if (m.find()) {
                category = m.group(1);
            }
            meta.set("category", category!=null?category:"");
            
            m = pricePattern.matcher(htmlText);
            if (m.find()) {
                price = m.group(2);
            }
            meta.set("price", price!=null?price:"");
            
            m = reviewPattern.matcher(htmlText);
            if (m.find()) {
                reviewScore = m.group(2);
                reviewCount = m.group(4);
            }
            meta.set("reviewScore", reviewScore!=null?reviewScore:"");
            meta.set("reviewCount", reviewCount!=null?reviewCount:"");
            
            m = installPattern.matcher(htmlText);
            if (m.find()) {
                install = m.group(1)!=null?m.group(1):"";
                install = install.trim();
            }
            meta.set("install", install);
            
            m = versionPattern.matcher(htmlText);
            if (m.find()) {
                version = m.group(1)!=null?m.group(1):"";
                version = version.trim();
            }
            meta.set("version", version);
            
            m = ratingPattern.matcher(htmlText);
            if (m.find()) {
                rating = m.group(1)!=null?m.group(1):"";
                rating = rating.trim();
            }
            meta.set("rating", rating);
            
            m = developerSitePattern.matcher(htmlText);
            if (m.find()) {
                developerSite = m.group(1)!=null?m.group(1):"";
                developerSite = developerSite.trim();
            }
            meta.set("developerSite", developerSite);
            
            m = developerEmailPattern.matcher(htmlText);
            if (m.find()) {
                developerEmail = m.group(1)!=null?m.group(1):"";
                developerEmail = developerEmail.trim();
            }
            meta.set("developerEmail", developerEmail);
            
            m = descriptionPattern.matcher(htmlText);
            if (m.find()) {
                description = m.group(1);
            }
            meta.set("description", description!=null?description:"");
        }
                
        ParseData parseData = new ParseData(ParseStatus.STATUS_SUCCESS, title,
                outlinks.toArray(new Outlink[0]), meta);
        ParseResult parseResult = ParseResult.createParseResult(content.getUrl(), 
                new ParseImpl("", parseData));
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
        }
        return parseResult;
    }
}
