package io.anserini.rts;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Calendar;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.SimpleFSDirectory;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import io.anserini.document.twitter.Status;
import io.anserini.index.twitter.TweetAnalyzer;
import io.anserini.nrts.TweetSearcher;
import io.anserini.nrts.TweetStreamIndexer.StatusField;
import twitter4j.RawStreamListener;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

public class TweetStreamReader {
	public static final Logger LOG = LogManager.getLogger(TweetStreamReader.class);
	private static final JsonParser JSON_PARSER = new JsonParser();
	private static final String INDEX_OPTION = "index";
	public static Directory index;
	public static IndexWriter indexWriter;
	public static final Analyzer ANALYZER = new TweetAnalyzer();
	public static boolean flag;
	final static Object lock = new Object();

	public static void main(String[] args) throws TwitterException, IOException {
		System.out.println(args[1]);
		flag = true;

		Options options = new Options();
		options.addOption(INDEX_OPTION, true, "index path");

		CommandLine cmdline = null;
		CommandLineParser parser = new GnuParser();
		try {
			cmdline = parser.parse(options, args);
		} catch (org.apache.commons.cli.ParseException e) {
			System.err.println("Error parsing command line: " + e.getMessage());
			System.exit(-1);
		}

		if (!cmdline.hasOption(INDEX_OPTION)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(TweetSearcher.class.getName(), options);
			System.exit(-1);
		}

		final FieldType textOptions = new FieldType();
		// textOptions.setIndexed(true);
		textOptions.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
		textOptions.setStored(true);
		textOptions.setTokenized(true);

		index = new SimpleFSDirectory(Paths.get(cmdline.getOptionValue(INDEX_OPTION)));
		IndexWriterConfig config = new IndexWriterConfig(ANALYZER);
		indexWriter = new IndexWriter(index, config);

		System.out.println("Initial docs in the index " + indexWriter.numDocs());

		final TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
		final BufferedWriter jsonFout = new BufferedWriter(
				new FileWriter(Calendar.getInstance().getTime().toString().replace(" ", "_")));

		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {

				
				twitterStream.shutdown();
			
					try {
						jsonFout.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					System.out.println("# of documents indexed this round:" + indexWriter.numDocs());
					try {
						indexWriter.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					try {
						index.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					System.out.println("Shutting down");
				

			}
		});
		RawStreamListener listener = new RawStreamListener() {

			int tweetCount = 0;

			@Override
			public void onMessage(String rawJSON) {

				Status status = Status.fromJson(rawJSON);

				if (status == null) {
					try {
						JsonObject obj = (JsonObject) JSON_PARSER.parse(rawJSON);
						if (obj.has("delete")) {
							long id = obj.getAsJsonObject("delete").getAsJsonObject("status").get("id").getAsLong();
							Query q = NumericRangeQuery.newLongRange(StatusField.ID.name, id, id, true, true);
							indexWriter.deleteDocuments(q);
						}
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					return;
				}

				if (status.getText() == null) {
					return;
				}

				Document doc = new Document();
				doc.add(new LongField(StatusField.ID.name, status.getId(), Field.Store.YES));
				doc.add(new LongField(StatusField.EPOCH.name, status.getEpoch(), Field.Store.YES));
				doc.add(new TextField(StatusField.SCREEN_NAME.name, status.getScreenname(), Store.YES));

				doc.add(new Field(StatusField.TEXT.name, status.getText(), textOptions));

				doc.add(new IntField(StatusField.FRIENDS_COUNT.name, status.getFollowersCount(), Store.YES));
				doc.add(new IntField(StatusField.FOLLOWERS_COUNT.name, status.getFriendsCount(), Store.YES));
				doc.add(new IntField(StatusField.STATUSES_COUNT.name, status.getStatusesCount(), Store.YES));

				long inReplyToStatusId = status.getInReplyToStatusId();
				if (inReplyToStatusId > 0) {
					doc.add(new LongField(StatusField.IN_REPLY_TO_STATUS_ID.name, inReplyToStatusId, Field.Store.YES));
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
						System.err.println("Error parsing retweet fields of " + status.getId());
					}
				}

				try {
					indexWriter.addDocument(doc);
					jsonFout.write(rawJSON);
					jsonFout.newLine();
					tweetCount++;
					if (tweetCount % 50 == 0) {
						LOG.info(tweetCount + " statuses indexed");
						if (tweetCount%300==0){
							System.exit(0);
						}

					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			@Override
			public void onException(Exception ex) {
				ex.printStackTrace();
			}
		};
		twitterStream.addListener(listener);
		twitterStream.sample();

	}

}
