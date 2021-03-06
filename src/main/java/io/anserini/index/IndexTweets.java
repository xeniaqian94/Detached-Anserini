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
import io.anserini.rts.TweetStreamReader.StatusField;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

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
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.tools.bzip2.CBZip2InputStream;

/**
 * Reference implementation for indexing statuses.
 */
public class IndexTweets {
	private static final Logger LOG = LogManager.getLogger(IndexTweets.class);

	public static final Analyzer ANALYZER = new TweetAnalyzer();
	public static String corpusFormat = null;

	private IndexTweets() {
	}

	public static enum StatusField {
		ID("id"), SCREEN_NAME("screen_name"), USER_ID("user_id"), EPOCH("epoch"), TEXT("text"), LANG(
				"lang"), IN_REPLY_TO_STATUS_ID("in_reply_to_status_id"), IN_REPLY_TO_USER_ID(
						"in_reply_to_user_id"), FOLLOWERS_COUNT("followers_count"), FRIENDS_COUNT(
								"friends_count"), STATUSES_COUNT("statuses_count"), RETWEETED_STATUS_ID(
										"retweeted_status_id"), RETWEETED_USER_ID("retweeted_user_id"), RETWEET_COUNT(
												"retweet_count"), LATITUDE("latitude"), LONGITUDE("longitude"), PLACE(
														"place"), USER_LOCATION("user_location"), USER_DESCRIPTION(
																"user_description"), USER_URL("user_url");

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
		options.addOption(OptionBuilder.withArgName("collection_pattern").hasArg()
				.withDescription("source collection directory").create("collection_pattern"));
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
			formatter.printHelp(IndexTweets.class.getName(), options);
			System.exit(-1);
		}

		String collectionPath = cmdline.getOptionValue(COLLECTION_OPTION);
		String indexPath = cmdline.getOptionValue(INDEX_OPTION);

		System.out.println(collectionPath + " " + indexPath);

		final FieldType textOptions = new FieldType();
		textOptions.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
		textOptions.setStored(true);
		textOptions.setTokenized(true);
		if (cmdline.hasOption(STORE_TERM_VECTORS_OPTION)) {
			textOptions.setStoreTermVectors(true);

		}

		LOG.info("collection: " + collectionPath);
		LOG.info("index: " + indexPath);
		LOG.info("collection_pattern " + cmdline.getOptionValue("collection_pattern"));
		LongOpenHashSet deletes = null;
		if (cmdline.hasOption(DELETES_OPTION)) {
			deletes = new LongOpenHashSet();
			File deletesFile = new File(cmdline.getOptionValue(DELETES_OPTION));
			if (!deletesFile.exists()) {
				System.err.println("Error: " + deletesFile + " does not exist!");
				System.exit(-1);
			}
			LOG.info("Reading deletes from " + deletesFile);

			FileInputStream fin = new FileInputStream(deletesFile);
			byte[] ignoreBytes = new byte[2];
			fin.read(ignoreBytes); // "B", "Z" bytes from commandline tools
			BufferedReader br = new BufferedReader(new InputStreamReader(new CBZip2InputStream(fin)));

			String s;
			while ((s = br.readLine()) != null) {
				if (s.contains("\t")) {
					deletes.add(Long.parseLong(s.split("\t")[0]));
				} else {
					deletes.add(Long.parseLong(s));
				}
			}
			br.close();
			fin.close();
			LOG.info("Read " + deletes.size() + " tweetids from deletes file.");
		}

		long maxId = Long.MAX_VALUE;
		if (cmdline.hasOption(MAX_ID_OPTION)) {
			maxId = Long.parseLong(cmdline.getOptionValue(MAX_ID_OPTION));
			LOG.info("index: " + maxId);
		}

		long startTime = System.currentTimeMillis();
		File file = new File(collectionPath);
		if (!file.exists()) {
			System.err.println("Error: " + file + " does not exist!");
			System.exit(-1);
		}

		final StatusStream stream = new JsonStatusCorpusReader(file, cmdline.getOptionValue("collection_pattern"));

		final Directory dir = new SimpleFSDirectory(Paths.get(cmdline.getOptionValue(INDEX_OPTION)));
		final IndexWriterConfig config = new IndexWriterConfig(ANALYZER);

		config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);

		final IndexWriter writer = new IndexWriter(dir, config);

		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {

				try {
					stream.close();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				;

				System.out.println("# of documents indexed this round:" + writer.numDocs());

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
				if (status.getText() == null) {
					continue;
				}

				// Skip deletes tweetids.
				if (deletes != null && deletes.contains(status.getId())) {
					continue;
				}

				if (status.getId() > maxId) {
					continue;
				}

				if (status.getLang().equals("en")
						&& (status.getPlace() != null || (status.getLongitude() != Double.NEGATIVE_INFINITY
								&& status.getlatitude() != Double.NEGATIVE_INFINITY))) {
					cnt++;
					Document doc = new Document();
					doc.add(new LongField(StatusField.ID.name, status.getId(), Field.Store.YES));

					doc.add(new Field(StatusField.TEXT.name, status.getText(), textOptions));
					doc.add(new StringField(StatusField.USER_ID.name, status.getUserid(), Field.Store.YES));
					if (status.getUserDescription() != null)
						doc.add(new Field(StatusField.USER_DESCRIPTION.name, status.getUserDescription(), textOptions));
					if (status.getUserLocation() != null)
						doc.add(new StringField(StatusField.USER_LOCATION.name, status.getUserLocation(),
								Field.Store.YES));
					if (status.getUserURL() != null)
						doc.add(new StringField(StatusField.USER_URL.name, status.getUserURL(), Field.Store.YES));

					doc.add(new IntField(StatusField.STATUSES_COUNT.name, status.getStatusesCount(), Store.YES));

					if (status.getURLEntities() != null) {
						List<String> urls = new ArrayList<String>();

						for (int i = 0; i < status.getURLEntities().length; i++) {

							String url = status.getURLEntities()[i];

							if (url != null) {
								urls.add(url);
								
							}

						}
						if (urls.size() > 0)
							doc.add(new StringField("tweetOutlinkDomain", String.join(" ", urls), Field.Store.YES));

					}

					if (status.getLongitude() != Double.NEGATIVE_INFINITY
							&& status.getlatitude() != Double.NEGATIVE_INFINITY) {
						doc.add(new DoubleField(StatusField.LONGITUDE.name, status.getLongitude(), Store.YES));
						doc.add(new DoubleField(StatusField.LATITUDE.name, status.getlatitude(), Store.YES));
					}
					if (status.getPlace() != null)
						doc.add(new StringField(StatusField.PLACE.name, status.getPlace(), Store.YES));

					writer.addDocument(doc);
					if (cnt % 10000 == 0) {
						LOG.info(cnt + " statuses indexed");
						writer.commit();
					}
				}

			}

			LOG.info(String.format("Total of %s statuses added", cnt));

			if (cmdline.hasOption(OPTIMIZE_OPTION)) {
				LOG.info("Merging segments...");
				writer.forceMerge(1);
				LOG.info("Done!");
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
