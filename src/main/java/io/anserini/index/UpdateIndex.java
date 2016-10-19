/**
 * Twitter Tools
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.anserini.index;

import io.anserini.document.twitter.JsonStatusCorpusReader;
import io.anserini.document.twitter.StatusStream;
import io.anserini.index.twitter.TweetAnalyzer;
import io.anserini.nrts.TweetSearcher;
import io.anserini.rts.TitleExtractor;
import io.anserini.rts.TweetStreamReader;
import io.anserini.rts.TweetStreamReader.StatusField;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import twitter4j.Status;
import twitter4j.json.DataObjectFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.NumericUtils;
import org.apache.tools.bzip2.CBZip2InputStream;

/**
 * Reference implementation for indexing statuses.
 */
public class UpdateIndex {
  private static final Logger LOG = LogManager.getLogger(UpdateIndex.class);
  private static IndexReader reader;
  public static final Analyzer ANALYZER = new TweetAnalyzer();
  public static String corpusFormat = null;
  static int tweetIndexedCount = 0;

  private UpdateIndex() {
  }

  private static final String[] cityName = { "Brooklyn, NY", "Chicago, IL", "Los Angeles, CA", "Philadelphia, PA",
      "Washington, DC", "Houston, TX", "Minneapolis, MN", "Cincinnati, OH", "Portland, ME", "St Louis, MO",
      "Cleveland, OH", "Pittsburgh, PA" };

  private static final String[][] cityNameAlias = { { "Manhattan, NY" }, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {} };

  private static final Double[] longitude = { -74.0063889, -87.6500000, -118.253842, -75.1641667, -77.0366667,
      -95.3630556, -93.2636111, -84.4569444, -70.2558333, -90.197778, -81.669722, -79.976389 };
  private static final Double[] latitude = { 40.7141667, 41.8500000, 34.040667, 39.9522222, 38.8950000, 29.7630556,
      44.9800000, 39.1619444, 43.6613889, 38.627222, 41.482222, 40.439722 };

  public static enum StatusField {
    ID("id"), SCREEN_NAME("screen_name"), EPOCH("epoch"), TEXT("text"), LANG("lang"), IN_REPLY_TO_STATUS_ID(
        "in_reply_to_status_id"), IN_REPLY_TO_USER_ID("in_reply_to_user_id"), FOLLOWERS_COUNT(
            "followers_count"), FRIENDS_COUNT("friends_count"), STATUSES_COUNT("statuses_count"), RETWEETED_STATUS_ID(
                "retweeted_status_id"), RETWEETED_USER_ID("retweeted_user_id"), RETWEET_COUNT(
                    "retweet_count"), LATITUDE("latitude"), LONGITUDE("longitude"), PLACE("place");

    public final String name;

    StatusField(String s) {
      name = s;
    }
  };

  private static final String HELP_OPTION = "h";
  private static final String COLLECTION_OPTION = "collection";
  private static final String INDEX_OPTION = "index";
  private static final String MAX_ID_OPTION = "max_id";
  private static final String DELETES_OPTION = "deletes";
  private static final String OPTIMIZE_OPTION = "optimize";
  private static final String STORE_TERM_VECTORS_OPTION = "store";

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(new Option(HELP_OPTION, "show help"));
    options.addOption(new Option(OPTIMIZE_OPTION, "merge indexes into a single segment"));
    options.addOption(new Option(STORE_TERM_VECTORS_OPTION, "store term vectors"));

    options.addOption(OptionBuilder.withArgName("dir").hasArg().withDescription("index location").create(INDEX_OPTION));
    options.addOption(OptionBuilder.withArgName("file").hasArg().withDescription("file with deleted tweetids")
        .create(DELETES_OPTION));
    options.addOption(OptionBuilder.withArgName("id").hasArg().withDescription("max id").create(MAX_ID_OPTION));

    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }

    if (cmdline.hasOption(HELP_OPTION) || !cmdline.hasOption(INDEX_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(UpdateIndex.class.getName(), options);
      System.exit(-1);
    }

    String indexPath = cmdline.getOptionValue(INDEX_OPTION);

    final FieldType textOptions = new FieldType();
    textOptions.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    textOptions.setStored(true);
    textOptions.setTokenized(true);
    textOptions.setStoreTermVectors(true);

    LOG.info("index: " + indexPath);

    File file = new File("PittsburghUserTimeline");
    if (!file.exists()) {
      System.err.println("Error: " + file + " does not exist!");
      System.exit(-1);
    }

    final StatusStream stream = new JsonStatusCorpusReader(file);

    Status status;
    String s;
    HashMap<Long, String> hm = new HashMap<Long, String>();
    try {
      while ((s = stream.nextRaw()) != null) {
        status = DataObjectFactory.createStatus(s);
        if (status.getText() == null) {
          continue;
        }

        hm.put(status.getUser().getId(),
            hm.get(status.getUser().getId()) + status.getText().replaceAll("[\\r\\n]+", " "));
      }

    } catch (Exception e) {
      e.printStackTrace();
    } finally {

      stream.close();
    }

    ArrayList<String> userIDList = new ArrayList<String>();
    try (BufferedReader br = new BufferedReader(new FileReader(new File("userID")))) {
      String line;
      while ((line = br.readLine()) != null) {
        userIDList.add(line.replaceAll("[\\r\\n]+", ""));

        // process the line.
      }
    }

    try {
      reader = DirectoryReader.open(FSDirectory.open(new File(cmdline.getOptionValue(INDEX_OPTION)).toPath()));
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    final Directory dir = new SimpleFSDirectory(Paths.get(cmdline.getOptionValue(INDEX_OPTION)));
    final IndexWriterConfig config = new IndexWriterConfig(ANALYZER);

    config.setOpenMode(IndexWriterConfig.OpenMode.APPEND);

    final IndexWriter writer = new IndexWriter(dir, config);

    IndexSearcher searcher = new IndexSearcher(reader);
    System.out.println("The total number of docs indexed "
        + searcher.collectionStatistics(TweetStreamReader.StatusField.TEXT.name).docCount());

    for (int city = 0; city < cityName.length; city++) {

      // Pittsburgh's coordinate -79.976389, 40.439722

      Query q_long = NumericRangeQuery.newDoubleRange(TweetStreamReader.StatusField.LONGITUDE.name,
          new Double(longitude[city] - 0.05), new Double(longitude[city] + 0.05), true, true);
      Query q_lat = NumericRangeQuery.newDoubleRange(TweetStreamReader.StatusField.LATITUDE.name,
          new Double(latitude[city] - 0.05), new Double(latitude[city] + 0.05), true, true);

      BooleanQuery bqCityName = new BooleanQuery();

      Term t = new Term("place", cityName[city]);
      TermQuery query = new TermQuery(t);
      bqCityName.add(query, BooleanClause.Occur.SHOULD);
      System.out.println(query.toString());

      for (int i = 0; i < cityNameAlias[city].length; i++) {
        t = new Term("place", cityNameAlias[city][i]);
        query = new TermQuery(t);
        bqCityName.add(query, BooleanClause.Occur.SHOULD);
        System.out.println(query.toString());
      }

      BooleanQuery bq = new BooleanQuery();

      BooleanQuery finalQuery = new BooleanQuery();

      // either a coordinate match
      bq.add(q_long, BooleanClause.Occur.MUST);
      bq.add(q_lat, BooleanClause.Occur.MUST);

      finalQuery.add(bq, BooleanClause.Occur.SHOULD);
      // or a place city name match
      finalQuery.add(bqCityName, BooleanClause.Occur.SHOULD);

      TotalHitCountCollector totalHitCollector = new TotalHitCountCollector();

      searcher.search(finalQuery, totalHitCollector);

      if (totalHitCollector.getTotalHits() > 0) {
        TopScoreDocCollector collector = TopScoreDocCollector.create(Math.max(0, totalHitCollector.getTotalHits()));
        searcher.search(finalQuery, collector);
        ScoreDoc[] hits = collector.topDocs().scoreDocs;

        System.out.println("City " + cityName[city] + " " + collector.getTotalHits() + " hits.");

        HashMap<String, Integer> hasHit = new HashMap<String, Integer>();
        int dupcount = 0;
        for (int i = 0; i < hits.length; ++i) {
          int docId = hits[i].doc;
          Document d;

          d = searcher.doc(docId);

          if (userIDList.contains(d.get(IndexTweets.StatusField.USER_ID.name))&&hm.containsKey(Long.parseLong(d.get(IndexTweets.StatusField.USER_ID.name)))) {
            d.add(new Field("timeline", hm.get(Long.parseLong(d.get(IndexTweets.StatusField.USER_ID.name))),
                textOptions));
            System.out.println("Found a user hit");
            BytesRefBuilder brb = new BytesRefBuilder();
            NumericUtils.longToPrefixCodedBytes(Long.parseLong(d.get(IndexTweets.StatusField.ID.name)), 0, brb);
            Term term = new Term(IndexTweets.StatusField.ID.name, brb.get());
            writer.updateDocument(term, d);
            writer.commit();

          }

        }
      }
    }
    reader.close();

  }
}
