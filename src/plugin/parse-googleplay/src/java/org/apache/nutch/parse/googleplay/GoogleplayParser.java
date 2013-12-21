package org.apache.nutch.parse.googleplay;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
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
    Set<String> ids = new HashSet<String>();
    static Pattern titlePattern = Pattern.compile("<title>(.*)?</title>");
    static Pattern pattern = Pattern.compile("href=\"/store/apps/details\\?id=([a-zA-Z0-9\\._]+)");
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
        String thisUrl = content.getBaseUrl().substring(content.getBaseUrl().indexOf("=")+1);
        byte[] contentInOctets = content.getContent();
        String htmlText = new String(contentInOctets);
        String title = null;
        Matcher m = titlePattern.matcher(htmlText);
        if (m.find()) {
            title = m.group(1);
        }
        m = pattern.matcher(htmlText);
        while (m.find()) {
            if (!m.group(1).equals(thisUrl)) {
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
        ParseData parseData = new ParseData(ParseStatus.STATUS_SUCCESS, title,
                outlinks.toArray(new Outlink[0]), content.getMetadata());
        ParseResult parseResult = ParseResult.createParseResult(content.getUrl(), 
                new ParseImpl(htmlText, parseData));
        return parseResult;
    }
}
