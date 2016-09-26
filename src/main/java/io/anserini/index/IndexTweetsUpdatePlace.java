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
import io.anserini.document.twitter.Status;
import io.anserini.document.twitter.StatusStream;
import io.anserini.index.twitter.TweetAnalyzer;
import io.anserini.rts.TweetStreamReader;
import io.anserini.rts.TweetStreamReader.StatusField;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.NumericUtils;
import org.apache.tools.bzip2.CBZip2InputStream;

/**
 * Reference implementation for indexing statuses.
 */
public class IndexTweetsUpdatePlace {
	private static final Logger LOG = LogManager.getLogger(IndexTweetsUpdatePlace.class);

	public static final Analyzer ANALYZER = new TweetAnalyzer();
	public static String corpusFormat = null;

	private IndexTweetsUpdatePlace() {
	}

	public static enum StatusField {
		ID("id"), SCREEN_NAME("screen_name"), EPOCH("epoch"), TEXT("text"), LANG("lang"), IN_REPLY_TO_STATUS_ID(
				"in_reply_to_status_id"), IN_REPLY_TO_USER_ID("in_reply_to_user_id"), FOLLOWERS_COUNT(
						"followers_count"), FRIENDS_COUNT("friends_count"), STATUSES_COUNT(
								"statuses_count"), RETWEETED_STATUS_ID("retweeted_status_id"), RETWEETED_USER_ID(
										"retweeted_user_id"), RETWEET_COUNT("retweet_count"), LATITUDE(
												"latitude"), LONGITUDE("longitude"), PLACE("place");

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

		options.addOption(OptionBuilder.withArgName("collection").hasArg()
				.withDescription("source collection directory").create(COLLECTION_OPTION));
		options.addOption(
				OptionBuilder.withArgName("dir").hasArg().withDescription("index location").create(INDEX_OPTION));
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

		if (cmdline.hasOption(HELP_OPTION) || !cmdline.hasOption(COLLECTION_OPTION)
				|| !cmdline.hasOption(INDEX_OPTION)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(IndexTweetsUpdatePlace.class.getName(), options);
			System.exit(-1);
		}

		String collectionPath = cmdline.getOptionValue(COLLECTION_OPTION);
		String indexPath = cmdline.getOptionValue(INDEX_OPTION);

		System.out.println(collectionPath + " " + indexPath);

		LOG.info("collection: " + collectionPath);
		LOG.info("index: " + indexPath);

		long startTime = System.currentTimeMillis();
		File file = new File(collectionPath);
		if (!file.exists()) {
			System.err.println("Error: " + file + " does not exist!");
			System.exit(-1);
		}

		final FieldType textOptions = new FieldType();
		textOptions.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
		textOptions.setStored(true);
		textOptions.setTokenized(true);
		if (cmdline.hasOption(STORE_TERM_VECTORS_OPTION)) {
			textOptions.setStoreTermVectors(true);

		}

		final StatusStream stream = new JsonStatusCorpusReader(file);

		final Directory dir = new SimpleFSDirectory(Paths.get(cmdline.getOptionValue(INDEX_OPTION)));
		final IndexWriterConfig config = new IndexWriterConfig(ANALYZER);

		config.setOpenMode(IndexWriterConfig.OpenMode.APPEND);

		final IndexWriter writer = new IndexWriter(dir, config);
		int updateCount = 0;

		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {

				try {
					stream.close();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}

				try {
					writer.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				try {
					dir.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println("Shutting down");

			}
		});
		int cnt = 0;
		Status status;
		try {
			while ((status = stream.next()) != null) {
				
				if (status.getPlace() != null) {

					
					
				    
//					Query q = NumericRangeQuery.newLongRange(TweetStreamReader.StatusField.ID.name, status.getId(),
//							status.getId(), true, true);
//					System.out.print("Deleting docCount="+writer.numDocs());
//					writer.deleteDocuments(q);
//					writer.commit();
//					System.out.print(" Deleted docCount="+writer.numDocs());

					Document doc = new Document();
					doc.add(new LongField(StatusField.ID.name, status.getId(), Field.Store.YES));
					doc.add(new LongField(StatusField.EPOCH.name, status.getEpoch(), Field.Store.YES));
					doc.add(new TextField(StatusField.SCREEN_NAME.name, status.getScreenname(), Store.YES));

					doc.add(new Field(StatusField.TEXT.name, status.getText(), textOptions));

					doc.add(new IntField(StatusField.FRIENDS_COUNT.name, status.getFollowersCount(), Store.YES));
					doc.add(new IntField(StatusField.FOLLOWERS_COUNT.name, status.getFriendsCount(), Store.YES));
					doc.add(new IntField(StatusField.STATUSES_COUNT.name, status.getStatusesCount(), Store.YES));
					doc.add(new DoubleField(StatusField.LONGITUDE.name, status.getLongitude(), Store.YES));
					doc.add(new DoubleField(StatusField.LATITUDE.name, status.getlatitude(), Store.YES));
					doc.add(new TextField(StatusField.PLACE.name, status.getPlace(), Store.YES));
					long inReplyToStatusId = status.getInReplyToStatusId();
					if (inReplyToStatusId > 0) {
						doc.add(new LongField(StatusField.IN_REPLY_TO_STATUS_ID.name, inReplyToStatusId,
								Field.Store.YES));
						doc.add(new LongField(StatusField.IN_REPLY_TO_USER_ID.name, status.getInReplyToUserId(),
								Field.Store.YES));
					}

					String lang = status.getLang();
					if (!lang.equals("unknown")) {
						doc.add(new TextField(StatusField.LANG.name, status.getLang(), Store.YES));
					}

					long retweetStatusId = status.getRetweetedStatusId();
					if (retweetStatusId > 0) {
						doc.add(new LongField(StatusField.RETWEETED_STATUS_ID.name, retweetStatusId, Field.Store.YES));
						doc.add(new LongField(StatusField.RETWEETED_USER_ID.name, status.getRetweetedUserId(),
								Field.Store.YES));
						doc.add(new IntField(StatusField.RETWEET_COUNT.name, status.getRetweetCount(), Store.YES));
						if (status.getRetweetCount() < 0 || status.getRetweetedStatusId() < 0) {
							LOG.warn("Error parsing retweet fields of " + status.getId());
						}
					}

					long id=status.getId();
					BytesRefBuilder brb = new BytesRefBuilder();
				    NumericUtils.longToPrefixCodedBytes(id, 0, brb);
				    Term term = new Term(StatusField.ID.name, brb.get());
				    writer.updateDocument(term,doc);
				    
//					writer.addDocument(doc);
					
					System.out.print(" Updated docCount="+writer.numDocs());
					updateCount += 1;
					
					if (updateCount % 10000 == 0) {
						LOG.info(updateCount + " statuses updated");
						writer.commit();
					}

				}

				
			}

			LOG.info("Total elapsed time: " + (System.currentTimeMillis() - startTime) + "ms");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			writer.close();
			dir.close();
			stream.close();
		}
	}
}
