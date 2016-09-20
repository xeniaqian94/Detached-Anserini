package io.anserini.rts;

import java.io.File;
import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.store.FSDirectory;

import io.anserini.nrts.TweetSearcher;
import io.anserini.nrts.TweetStreamIndexer.StatusField;
import twitter4j.TwitterException;

public class TweetNaiveSearcher {
	private static IndexReader reader;
	private static final String INDEX_OPTION = "index";
	private static final String CITY_OPTION="city";
	private static final String LONGITUDE_OPTION = "longitude";
	private static final String LATITUDE_OPTION = "latitude";

	public static void main(String[] args) throws IOException, ParseException, TwitterException {
		// TODO Auto-generated method stub

		Options options = new Options();
		options.addOption(INDEX_OPTION, true, "index path");
		options.addOption(CITY_OPTION,true,"city name");
		options.addOption(LONGITUDE_OPTION,true,"longitude");
		options.addOption(LATITUDE_OPTION,true,"latitude");

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

		// IndexReader newReader = null;
		// try {
		// newReader = DirectoryReader.openIfChanged((DirectoryReader) reader);
		// } catch (IOException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		// if (newReader != null) {
		// try {
		// reader.close();
		// } catch (IOException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		// reader = newReader;
		// }
		IndexSearcher searcher = new IndexSearcher(reader);
		System.out.println("The total number of docs indexed "
				+ searcher.collectionStatistics(TweetStreamReader.StatusField.TEXT.name).docCount());

		// Pittsburgh's coordinate -79.976389, 40.439722
		 Query q_long =
		 NumericRangeQuery.newDoubleRange(TweetStreamReader.StatusField.LONGITUDE.name,
		 new Double(Double.parseDouble(cmdline.getOptionValue(LONGITUDE_OPTION))-0.05), new Double(Double.parseDouble(cmdline.getOptionValue(LONGITUDE_OPTION))+0.05), true, true);
		 Query q_lat =
		 NumericRangeQuery.newDoubleRange(TweetStreamReader.StatusField.LATITUDE.name,
		 new Double(Double.parseDouble(cmdline.getOptionValue(LATITUDE_OPTION))-0.05), new Double(Double.parseDouble(cmdline.getOptionValue(LATITUDE_OPTION))+0.05), true, true);

		// Query q_long =
		// NumericRangeQuery.newDoubleRange(TweetStreamReader.StatusField.LONGITUDE.name,
		// new Double(0), new Double(90), true, true);
		// Query q_lat =
		// NumericRangeQuery.newDoubleRange(TweetStreamReader.StatusField.LATITUDE.name,
		// new Double(0), new Double(90), true, true);

		BooleanQuery bq = new BooleanQuery();

//		Query q_inReplyTo = NumericRangeQuery.newLongRange(TweetStreamReader.StatusField.IN_REPLY_TO_STATUS_ID.name, 1l,
//				Long.MAX_VALUE, true, true);
		
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

		System.out.println("Total number of inReplyTo are "+collector.getTotalHits()+" "+hits.length);
		System.out.println("Number of hits for Pittsburgh region " + hits.length);
//		File chainDirectory=new File("chainDirectory");
//		chainDirectory.mkdir();
//		testRetrieveByID.Initialize();
		
		for (int i = 0; i < hits.length; ++i) {
			int docId = hits[i].doc;
			Document d;

			d = searcher.doc(docId);
//			System.out.println("The tail status is "+d.get(TweetStreamReader.StatusField.ID.name)+" will be searching for its inReplyTo "+d.get(TweetStreamReader.StatusField.IN_REPLY_TO_STATUS_ID.name));
//			testRetrieveByID.getChain(d.get(TweetStreamReader.StatusField.ID.name));
			
//			System.out.println(d.get(TweetStreamReader.StatusField.IN_REPLY_TO_STATUS_ID.name)+" "+d.get(TweetStreamReader.StatusField.TEXT.name));

			System.out.println(d.get(TweetStreamReader.StatusField.ID.name) + " "
					+ d.get(TweetStreamReader.StatusField.LONGITUDE.name) + " "
					+ d.get(TweetStreamReader.StatusField.LATITUDE.name) + " "
					+ d.get(TweetStreamReader.StatusField.TEXT.name));
		}

	}

}
