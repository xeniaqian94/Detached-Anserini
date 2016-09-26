package io.anserini.rts;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
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
//import io.anserini.nrts.TweetSearcher;
import io.anserini.nrts.TweetStreamIndexer.StatusField;
import io.anserini.util.LatLng;
import twitter4j.FilterQuery;
import twitter4j.RawStreamListener;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

public class TweetStreamReader {
	public static final Logger LOG = LogManager.getLogger(TweetStreamReader.class);
	private static final JsonParser JSON_PARSER = new JsonParser();
	private static final String INDEX_OPTION = "index";
	private static final String RAW_OPTION = "raw";
	public static Directory index;
	public static IndexWriter indexWriter;
	public static final Analyzer ANALYZER = new TweetAnalyzer();
	public static boolean flag;
	final static Object lock = new Object();

	static int tweetCount = 0;
	static int geoTaggedTweetCount = 0;
	static int geoRecognizedTweetCount = 0;

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

	private static Map<String, LatLng> cityCenter = new HashMap<String, LatLng>() {
		{
			put("NYC", new LatLng(40.7141667, -74.0063889));
			put("Chicago", new LatLng(41.8500000, -87.6500000));
			put("Los Angeles", new LatLng(34.040667, -118.253842));
			put("Philadelphia", new LatLng(39.9522222, -75.1641667));
			put("Washington", new LatLng(38.8950000, -77.0366667));
			put("Houston", new LatLng(29.7630556, -95.3630556));
			put("Minneapolis", new LatLng(44.9800000, -93.2636111));
			put("Cincinnati", new LatLng(39.1619444, -84.4569444));
			put("Portland", new LatLng(43.6613889, -70.2558333));
			put("St. Louis", new LatLng(38.627222, -90.197778));
			put("Cleveland", new LatLng(41.482222, -81.669722));
			put("Pittsburgh", new LatLng(40.439722, -79.976389));

		}
	};

	public static void main(String[] args) throws TwitterException, IOException {

		System.out.println(args[1]);
		flag = true;

		Options options = new Options();
		options.addOption(INDEX_OPTION, true, "index path");
		options.addOption(RAW_OPTION, true, "raw path");

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
			formatter.printHelp(TweetStreamReader.class.getName(), options);
			System.exit(-1);
		}
		if (!cmdline.hasOption(RAW_OPTION)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(TweetStreamReader.class.getName(), options);
			System.exit(-1);
		}

		final FieldType textOptions = new FieldType();
		// textOptions.setIndexed(true);
		textOptions.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
		textOptions.setStored(true);
		textOptions.setTokenized(true);
		textOptions.setStoreTermVectors(true);

		index = new SimpleFSDirectory(Paths.get(cmdline.getOptionValue(INDEX_OPTION)));
		IndexWriterConfig config = new IndexWriterConfig(ANALYZER);
		indexWriter = new IndexWriter(index, config);

		System.out.println("Initial docs in the index " + indexWriter.numDocs());

		final TwitterStream twitterStream = new TwitterStreamFactory().getInstance();

		File dir = new File(cmdline.getOptionValue(RAW_OPTION));
		dir.mkdir();
		final BufferedWriter jsonFout = new BufferedWriter(new FileWriter(cmdline.getOptionValue(RAW_OPTION) + "/"
				+ Calendar.getInstance().getTime().toString().replace(" ", "_")));

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
				System.out.println("Indexed tweet count " + tweetCount);
				System.out.println("Geotagged tweet count " + geoTaggedTweetCount);
				System.out.println("Geo recognized tweet count " + geoRecognizedTweetCount);
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
				if (status.getPlace() != null) {
					System.out.println("		" + status.getPlace() + " " + status.getText());

				}
				if ((status.getLongitude() != Double.NEGATIVE_INFINITY
						&& status.getlatitude() != Double.NEGATIVE_INFINITY)) {
					System.out.println(
							"		" + status.getLongitude() + " " + status.getlatitude() + " " + status.getText());
					geoTaggedTweetCount += 1;
					for (Map.Entry<String, LatLng> entry : cityCenter.entrySet()) {
						if (entry.getValue().withinArea(status.getlatitude(), status.getLongitude())) {
							System.out
									.println("Found a hit in city " + entry.getKey() + ", Value = " + entry.getValue());
							geoRecognizedTweetCount += 1;
						}
						break;

					}

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
					// System.out.println("Reply w.r.t " +
					// status.getInReplyToStatusId());
					doc.add(new LongField(StatusField.IN_REPLY_TO_STATUS_ID.name, inReplyToStatusId, Field.Store.YES));
					doc.add(new LongField(StatusField.IN_REPLY_TO_USER_ID.name, status.getInReplyToUserId(),
							Field.Store.YES));
				}

				doc.add(new DoubleField(StatusField.LONGITUDE.name, status.getLongitude(), Store.YES));
				doc.add(new DoubleField(StatusField.LATITUDE.name, status.getlatitude(), Store.YES));
				if(status.getPlace()!=null){
					doc.add(new StringField(StatusField.PLACE.name, status.getPlace(), Store.YES));
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
					if (tweetCount % 2000 == 0) {
						// Log.info(tweetCount + " statuses indexed");
						indexWriter.commit();

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
