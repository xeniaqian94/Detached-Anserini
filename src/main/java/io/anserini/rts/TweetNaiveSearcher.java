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
import org.apache.lucene.store.FSDirectory;

import io.anserini.nrts.TweetSearcher;
import io.anserini.nrts.TweetStreamIndexer.StatusField;

public class TweetNaiveSearcher {
	private static IndexReader reader;
	private static final String INDEX_OPTION = "index";

	public static void main(String[] args) throws IOException, ParseException {
		// TODO Auto-generated method stub

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
		try {
			reader = DirectoryReader.open(FSDirectory.open(new File(cmdline.getOptionValue(INDEX_OPTION)).toPath()));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		IndexReader newReader = null;
		try {
			newReader = DirectoryReader.openIfChanged((DirectoryReader) reader);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (newReader != null) {
			try {
				reader.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			reader = newReader;
		}
		IndexSearcher searcher = new IndexSearcher(reader);
		System.out.println("The total number of docs indexed "+searcher.collectionStatistics(TweetStreamReader.StatusField.TEXT.name).docCount());

//		 Pittsburgh's coordinate -79.976389, 40.439722
		Query q_long = NumericRangeQuery.newDoubleRange(TweetStreamReader.StatusField.LONGITUDE.name,
				new Double(-79.926389), new Double(-79.976389), true, true);
		Query q_lat = NumericRangeQuery.newDoubleRange(TweetStreamReader.StatusField.LATITUDE.name,
				new Double(40.389722), new Double(40.489722), true, true);
		
//		Query q_long = NumericRangeQuery.newDoubleRange(TweetStreamReader.StatusField.LONGITUDE.name,
//				new Double(0), new Double(90), true, true);
//		Query q_lat = NumericRangeQuery.newDoubleRange(TweetStreamReader.StatusField.LATITUDE.name,
//				new Double(0), new Double(90), true, true);
		
		BooleanQuery bq = new BooleanQuery();
		bq.add(q_long, BooleanClause.Occur.MUST);
		bq.add(q_lat, BooleanClause.Occur.MUST);

		// Query q = new QueryParser(StatusField.TEXT.name,
		// TweetStreamReader.ANALYZER).parse("love");
		TopScoreDocCollector collector = TopScoreDocCollector.create(15);
		searcher.search(bq, collector);
		ScoreDoc[] hits = collector.topDocs().scoreDocs;

		System.out.println("Number of hits for Pittsburgh region " + hits.length);
		for (int i = 0; i < hits.length; ++i) {
			int docId = hits[i].doc;
			Document d;

			d = searcher.doc(docId);

			System.out.println(d.get(TweetStreamReader.StatusField.EPOCH.name)+" "+d.get(TweetStreamReader.StatusField.LONGITUDE.name) + " "
					+ d.get(TweetStreamReader.StatusField.LATITUDE.name) + " "
					+ d.get(TweetStreamReader.StatusField.TEXT.name));
		}

	}

}
