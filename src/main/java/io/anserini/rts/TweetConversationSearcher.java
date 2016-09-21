package io.anserini.rts;

import java.io.File;
import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.TermVector;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import io.anserini.nrts.TweetSearcher;
import io.anserini.nrts.TweetStreamIndexer.StatusField;
import twitter4j.TwitterException;

public class TweetConversationSearcher {
	private static IndexReader reader;
	private static final String INDEX_OPTION = "index";
	private static final String CITY_OPTION = "city";
	private static final String LONGITUDE_OPTION = "longitude";
	private static final String LATITUDE_OPTION = "latitude";

	// us,new york,New York,NY,8107916,40.7141667,-74.0063889
	// us,chicago,Chicago,IL,2841952,41.8500000,-87.6500000
	// us,los angeles,Los Angeles,CA, 34.040667, -118.253842
	// us,philadelphia,Philadelphia,PA,1453268,39.9522222,-75.1641667
	// us,washington,Washington,DC,552433,38.8950000,-77.0366667
	// us,houston,Houston,TX,2027712,29.7630556,-95.3630556
	//
	//
	// us,minneapolis,Minneapolis,MN,367773,44.9800000,-93.2636111
	// us,cincinnati,Cincinnati,OH,306382,39.1619444,-84.4569444
	// us,portland,Portland,ME,63142,43.6613889,-70.2558333
	// us, St. Louis, Missouri , 38.627222, -90.197778
	// us. Cleveland, 41.482222, -81.669722
	// pittsburgh 40 -79

	private static final String[] cityName = { "NY", "Chicago", "LA", "Philadelphia", "Washington", "Houston",
			"Minneapolis", "Cincinnati", "Portland", "StLouis", "Cleveland", "Pittsburgh" };
	private static final Double[] longitude = { -74.0063889, -87.6500000, -118.253842, -75.1641667, -77.0366667,
			-95.3630556, -93.2636111, -84.4569444, -70.2558333, -90.197778, -81.669722, -79.976389 };
	private static final Double[] latitude = { 40.7141667, 41.8500000, 34.040667, 39.9522222, 38.8950000, 29.7630556,
			44.9800000, 39.1619444, 43.6613889, 38.627222, 41.482222, 40.439722 };

	public static void main(String[] args) throws IOException, ParseException, TwitterException {
		// TODO Auto-generated method stub

		for (int i = 0; i < cityName.length; i++) {
			System.out.println(cityName[i] + " " + longitude[i] + " " + latitude[i]);

		}

		Options options = new Options();
		options.addOption(INDEX_OPTION, true, "index path");
		// options.addOption(CITY_OPTION, true, "city name");
		// options.addOption(LONGITUDE_OPTION, true, "longitude");
		// options.addOption(LATITUDE_OPTION, true, "latitude");

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
		try {
			reader = DirectoryReader.open(FSDirectory.open(new File(cmdline.getOptionValue(INDEX_OPTION)).toPath()));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		IndexSearcher searcher = new IndexSearcher(reader);
		System.out.println("The total number of docs indexed "
				+ searcher.collectionStatistics(TweetStreamReader.StatusField.TEXT.name).docCount());

		for (int city = 0; city < cityName.length; city++) {
			// Pittsburgh's coordinate -79.976389, 40.439722
			Query q_long = NumericRangeQuery.newDoubleRange(TweetStreamReader.StatusField.LONGITUDE.name,
					new Double(Double.parseDouble(cmdline.getOptionValue(LONGITUDE_OPTION)) - 0.05),
					new Double(Double.parseDouble(cmdline.getOptionValue(LONGITUDE_OPTION)) + 0.05), true, true);
			Query q_lat = NumericRangeQuery.newDoubleRange(TweetStreamReader.StatusField.LATITUDE.name,
					new Double(Double.parseDouble(cmdline.getOptionValue(LATITUDE_OPTION)) - 0.05),
					new Double(Double.parseDouble(cmdline.getOptionValue(LATITUDE_OPTION)) + 0.05), true, true);

			BooleanQuery bq = new BooleanQuery();

			// Query q_inReplyTo =
			// NumericRangeQuery.newLongRange(TweetStreamReader.StatusField.IN_REPLY_TO_STATUS_ID.name,
			// 1l,
			// Long.MAX_VALUE, true, true);

			bq.add(q_long, BooleanClause.Occur.MUST);
			bq.add(q_lat, BooleanClause.Occur.MUST);

			// Query q = new QueryParser(StatusField.TEXT.name,
			// TweetStreamReader.ANALYZER).parse("love");
			TotalHitCountCollector totalHitCollector = new TotalHitCountCollector();

			// First search and scoring part: titleCoordSimilarity(q,d) = Nt/T
			searcher.search(bq, totalHitCollector);

			TopScoreDocCollector collector = TopScoreDocCollector.create(Math.max(0, totalHitCollector.getTotalHits()));
			searcher.search(bq, collector);
			ScoreDoc[] hits = collector.topDocs().scoreDocs;

			System.out.println("Total number of inReplyTo are " + collector.getTotalHits() + " " + hits.length);
			System.out.println("Number of hits for Pittsburgh region " + hits.length);
			// File chainDirectory=new File("chainDirectory");
			// chainDirectory.mkdir();
			// testRetrieveByID.Initialize();\

			// Collection

			for (int i = 0; i < hits.length; ++i) {
				int docId = hits[i].doc;
				Document d;

				d = searcher.doc(docId);

				// System.out.println("The tail status is
				// "+d.get(TweetStreamReader.StatusField.ID.name)+" will be
				// searching for its inReplyTo
				// "+d.get(TweetStreamReader.StatusField.IN_REPLY_TO_STATUS_ID.name));
				// testRetrieveByID.getChain(d.get(TweetStreamReader.StatusField.ID.name));

				// System.out.println(d.get(TweetStreamReader.StatusField.IN_REPLY_TO_STATUS_ID.name)+"
				// "+d.get(TweetStreamReader.StatusField.TEXT.name));

				System.out.println(d.get(TweetStreamReader.StatusField.ID.name) + " "
						+ d.get(TweetStreamReader.StatusField.LONGITUDE.name) + " "
						+ d.get(TweetStreamReader.StatusField.LATITUDE.name) + " "
						+ d.get(TweetStreamReader.StatusField.TEXT.name));

				Terms terms = reader.getTermVector(docId, TweetStreamReader.StatusField.TEXT.name);
				if (terms != null && terms.size() > 0) {
					TermsEnum termsEnum = terms.iterator(); // access the terms
															// for this field
					BytesRef term = null;
					while ((term = termsEnum.next()) != null) {// explore the
																// terms for
																// this field
						DocsEnum docsEnum = termsEnum.docs(null, null); // enumerate
																		// through
																		// documents,
																		// in
																		// this
																		// case
																		// only
																		// one
						int docIdEnum;
						while ((docIdEnum = docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
							System.out.println("	" + term.utf8ToString() + " " + docIdEnum + " " + docsEnum.freq()); // get
																														// the
																														// term
																														// frequency
																														// in
																														// the
																														// document

						}
					}
				}
				System.out.println("Terms to string() " + terms.toString());
				System.out
						.println("t.getStats(): " + terms.getStats().toString() + "\nt.hasFreq(): " + terms.hasFreqs());
			}
		}
		reader.close();

	}

}
