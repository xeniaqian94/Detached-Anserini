package io.anserini.rts;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

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
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldValueFilter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import io.anserini.nrts.TweetSearcher;
import io.anserini.nrts.TweetStreamIndexer.StatusField;
import twitter4j.TwitterException;

class TweetPlaceNaiveSearcher {
	private static IndexReader reader;
	private static final String INDEX_OPTION = "index";
	private static final String CITY_OPTION = "city";
	private static final String LONGITUDE_OPTION = "longitude";
	private static final String LATITUDE_OPTION = "latitude";

	private static final String[] cityName = { "Brooklyn, NY", "Chicago, IL", "Los Angeles, CA", "Philadelphia, PA",
			"Washington, DC", "Houston, TX", "Minneapolis, MN", "Cincinnati, OH", "Portland, ME", "St Louis, MO",
			"Cleveland, OH", "Pittsburgh, PA" };
	private static final Double[] longitude = { -74.0063889, -87.6500000, -118.253842, -75.1641667, -77.0366667,
			-95.3630556, -93.2636111, -84.4569444, -70.2558333, -90.197778, -81.669722, -79.976389 };
	private static final Double[] latitude = { 40.7141667, 41.8500000, 34.040667, 39.9522222, 38.8950000, 29.7630556,
			44.9800000, 39.1619444, 43.6613889, 38.627222, 41.482222, 40.439722 };

	public static void printMemoryUsage(boolean gc) {

		Runtime runtime = Runtime.getRuntime();

		if (gc)
			runtime.gc();

		System.out.println(
				"Memory used:  " + ((runtime.totalMemory() - runtime.freeMemory()) / (1024L * 1024L)) + " MB\n");
	}

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

		BufferedWriter goldFout = new BufferedWriter(new FileWriter("clusteringDataset/gold_standard"));
		BufferedWriter docVectorsFout = new BufferedWriter(new FileWriter("clusteringDataset/docVectors"));
		BufferedWriter docVectorsBinaryFout = new BufferedWriter(new FileWriter("clusteringDataset/docVectorsBinary"));
		BufferedWriter dictFout = new BufferedWriter(new FileWriter("clusteringDataset/dict"));
		BufferedWriter rawTextFout = new BufferedWriter(new FileWriter("clusteringDataset/rawText"));
		BufferedWriter dfFout = new BufferedWriter(new FileWriter("clusteringDataset/df"));
		Map<String, Integer> dict = new HashMap<String, Integer>(); // term
																	// termID
		Map<Integer, Integer> df = new HashMap<Integer, Integer>();
		int docCount = 0;

		for (int city = 0; city < cityName.length; city++) {
			// Pittsburgh's coordinate -79.976389, 40.439722

			printMemoryUsage(true);

			Query q_long = NumericRangeQuery.newDoubleRange(TweetStreamReader.StatusField.LONGITUDE.name,
					new Double(longitude[city] - 0.05), new Double(longitude[city] + 0.05), true, true);
			Query q_lat = NumericRangeQuery.newDoubleRange(TweetStreamReader.StatusField.LATITUDE.name,
					new Double(latitude[city] - 0.05), new Double(latitude[city] + 0.05), true, true);

			Term t = new Term("place", cityName[city]);
			TermQuery query = new TermQuery(t);

			System.out.println(query.toString());

			BooleanQuery bq = new BooleanQuery();

			BooleanQuery finalQuery = new BooleanQuery();

			//either a coordinate match
			bq.add(q_long, BooleanClause.Occur.MUST);
			bq.add(q_lat, BooleanClause.Occur.MUST);

			finalQuery.add(bq, BooleanClause.Occur.SHOULD);
			//or a place city name match
			finalQuery.add(query, BooleanClause.Occur.SHOULD);

			TotalHitCountCollector totalHitCollector = new TotalHitCountCollector();

			searcher.search(finalQuery, totalHitCollector);
			
			

			if (totalHitCollector.getTotalHits() > 0) {
				TopScoreDocCollector collector = TopScoreDocCollector
						.create(Math.max(0, totalHitCollector.getTotalHits()));
				searcher.search(finalQuery, collector);
				ScoreDoc[] hits = collector.topDocs().scoreDocs;

				System.out.println("City " + cityName[city] + " " + collector.getTotalHits() + " hits.");

				for (int i = 0; i < hits.length; ++i) {
					int docId = hits[i].doc;
					Document d;

					d = searcher.doc(docId);

					
					rawTextFout.write(d.get(TweetStreamReader.StatusField.TEXT.name).replaceAll("[\\r\\n]+", " "));
					rawTextFout.newLine();
					rawTextFout.flush();
					docCount += 1;

					Terms terms = reader.getTermVector(docId, TweetStreamReader.StatusField.TEXT.name);
					if (terms != null && terms.size() > 0) {
						TermsEnum termsEnum = terms.iterator(); // access the
																// terms
																// for this
																// field
						BytesRef term = null;
						while ((term = termsEnum.next()) != null) {// explore
																	// the
																	// terms for
																	// this
																	// field
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
								String thisTerm = term.utf8ToString();
								int termID;
								if (dict.containsKey(thisTerm)) {
									termID = dict.get(thisTerm);
									df.put(termID, df.get(termID) + 1);
								} else {
									termID = dict.size();
									dict.put(thisTerm, termID);
									df.put(termID, 1);
								}

								docVectorsFout.write(termID + ":" + docsEnum.freq() + " ");
								docVectorsBinaryFout.write(termID + ":1 ");

							}

						}
					}
					docVectorsFout.newLine();
					docVectorsFout.flush();
					docVectorsBinaryFout.newLine();
					docVectorsBinaryFout.flush();
					goldFout.write(cityName[city]);
					goldFout.newLine();
					goldFout.flush();
				}
				System.out.println();
			}
		}
		for (String term : dict.keySet()) {
			dictFout.write(term + " " + dict.get(term));
			dictFout.newLine();
		}
		for (int termID : df.keySet()) {
			dfFout.write(termID + ":" + df.get(termID));
			dfFout.newLine();
		}

		goldFout.close();
		docVectorsFout.close();
		dictFout.close();
		rawTextFout.close();
		dfFout.close();
		reader.close();

	}

}
