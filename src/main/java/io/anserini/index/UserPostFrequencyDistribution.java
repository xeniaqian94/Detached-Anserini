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
import io.anserini.rts.TitleExtractor;
import io.anserini.rts.TweetStreamReader;
import io.anserini.rts.TweetStreamReader.StatusField;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import twitter4j.Status;
import twitter4j.json.DataObjectFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringEscapeUtils;
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
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.tools.bzip2.CBZip2InputStream;

/**
 * Reference implementation for indexing statuses.
 */
public class UserPostFrequencyDistribution {
	private static final Logger LOG = LogManager.getLogger(UserPostFrequencyDistribution.class);

	public static final Analyzer ANALYZER = new TweetAnalyzer();
	public static String corpusFormat = null;
	static int userIndexedCount = 0;
	static DirectoryReader reader;
	static IndexSearcher searcher;

	private UserPostFrequencyDistribution() {
	}

	public static enum StatusField {
		ID("id"), USER_ID_STRING("user_id_string"), SCREEN_NAME("screen_name"), EPOCH("epoch"), TEXT("text"), LANG(
				"lang"), IN_REPLY_TO_STATUS_ID("in_reply_to_status_id"), IN_REPLY_TO_USER_ID(
						"in_reply_to_user_id"), FOLLOWERS_COUNT("followers_count"), FRIENDS_COUNT(
								"friends_count"), STATUSES_COUNT("statuses_count"), RETWEETED_STATUS_ID(
										"retweeted_status_id"), RETWEETED_USER_ID("retweeted_user_id"), RETWEET_COUNT(
												"retweet_count"), LATITUDE("latitude"), LONGITUDE("longitude"), PLACE(
														"place"), PREVIOUS_PITTSBURGH_POST(
																"previous_pittsburgh_post"), POST_COUNT_TOTAL(
																		"post_count_total");

		public final String name;

		StatusField(String s) {
			name = s;
		}
	};

	static double pittsburghLongitude = -79.976389d;
	static double pittsburghLatitude = 40.439722d;

	private static final String HELP_OPTION = "h";
	private static final String COLLECTION_OPTION = "collection";
	private static final String INDEX_OPTION = "index";

	private static final String STORE_TERM_VECTORS_OPTION = "store";

	@SuppressWarnings("static-access")
	public static void main(String[] args) throws Exception {
		Options options = new Options();

		options.addOption(new Option(HELP_OPTION, "show help"));

		options.addOption(new Option(STORE_TERM_VECTORS_OPTION, "store term vectors"));

		options.addOption(OptionBuilder.withArgName("collection").hasArg()
				.withDescription("source collection directory").create(COLLECTION_OPTION));
		options.addOption(
				OptionBuilder.withArgName("dir").hasArg().withDescription("index location").create(INDEX_OPTION));

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
			formatter.printHelp(UserPostFrequencyDistribution.class.getName(), options);
			System.exit(-1);
		}

		String collectionPath = cmdline.getOptionValue(COLLECTION_OPTION);
		String indexPath = cmdline.getOptionValue(INDEX_OPTION);

		System.out.println(collectionPath + " " + indexPath);

		final FieldType textOptions = new FieldType();
		textOptions.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
		textOptions.setStored(true);
		textOptions.setTokenized(true);
		textOptions.setStoreTermVectors(true);

		LOG.info("collection: " + collectionPath);
		LOG.info("index: " + indexPath);

		LongOpenHashSet deletes = null;

		long startTime = System.currentTimeMillis();
		File file = new File(collectionPath);
		if (!file.exists()) {
			System.err.println("Error: " + file + " does not exist!");
			System.exit(-1);
		}

		final StatusStream stream = new JsonStatusCorpusReader(file,cmdline.getOptionValue("collection_pattern"));

		final Directory dir = new SimpleFSDirectory(Paths.get(cmdline.getOptionValue(INDEX_OPTION)));
		final IndexWriterConfig config = new IndexWriterConfig(ANALYZER);

		config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);

		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {

				try {

					dir.close();
					stream.close();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				;

				System.out.println("# of users indexed this round: " + userIndexedCount);

				try {
					dir.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println("Shutting down");

			}
		});
		Status status;
		String s;
		boolean readerNotInitialized = true;

		try {
			Properties prop = new Properties();
			OutputStream output = new FileOutputStream(cmdline.getOptionValue("property"));
			while ((s = stream.nextRaw()) != null) {

				if (prop.size() % 1000 == 0) {
					Runtime runtime = Runtime.getRuntime();
					runtime.gc();
					System.out.println("Property size " + prop.size() + "Memory used:  "
							+ ((runtime.totalMemory() - runtime.freeMemory()) / (1024L * 1024L)) + " MB\n");
				}

				status = DataObjectFactory.createStatus(s);
				if (status.getText() == null) {
					continue;
				}

				boolean pittsburghRelated = false;
				try {

					if (Math.abs(status.getGeoLocation().getLongitude() - pittsburghLongitude) < 0.05d
							&& Math.abs(status.getGeoLocation().getLatitude() - pittsburghLatitude) < 0.05d)
						pittsburghRelated = true;
				} catch (Exception e) {

				}
				try {
					if (status.getPlace().getFullName().contains("Pittsburgh, PA"))
						pittsburghRelated = true;
				} catch (Exception e) {

				}
				try {
					if (Math.abs(status.getPlace().getBoundingBoxCoordinates()[0][0].getLongitude()
							- pittsburghLongitude) < 0.05d
							&& Math.abs(status.getPlace().getBoundingBoxCoordinates()[0][0].getLatitude()
									- pittsburghLatitude) < 0.05d)
						pittsburghRelated = true;
				} catch (Exception e) {

				}
				try {
					if (status.getUser().getLocation().contains("Pittsburgh, PA"))
						pittsburghRelated = true;
				} catch (Exception e) {

				}

				try {
					if (status.getText().contains("Pittsburgh"))
						pittsburghRelated = true;
				} catch (Exception e) {

				}
				if (pittsburghRelated) {

					int previousPostCount = 0;

					if (prop.containsKey(String.valueOf(status.getUser().getId()))) {
						previousPostCount = Integer
								.valueOf(prop.getProperty(String.valueOf(status.getUser().getId())).split(" ")[1]);
					}

					prop.setProperty(String.valueOf(status.getUser().getId()),
							String.valueOf(status.getUser().getStatusesCount()) + " " + (1 + previousPostCount));

				}
			}
			prop.store(output, null);
			LOG.info(String.format("Total of %s statuses added", userIndexedCount));
			LOG.info("Total elapsed time: " + (System.currentTimeMillis() - startTime) + "ms");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			dir.close();
			stream.close();
		}
	}
}
