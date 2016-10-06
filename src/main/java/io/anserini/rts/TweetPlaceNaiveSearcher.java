package io.anserini.rts;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;
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

import io.anserini.index.IndexTweets;
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

	private static final String[][] cityNameAlias = { { "Manhattan, NY" }, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {} };
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

	public static String getDomainName(String url) {
		URI uri;
		try {
			uri = new URI(url);

			String domain = uri.getHost();
			if (domain != null)
				return domain.startsWith("www.") ? domain.substring(4) : domain;

		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
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
		BufferedWriter docVectorsFout = new BufferedWriter(new FileWriter("clusteringDataset/docVectorsTF"));
		BufferedWriter docVectorsBinaryFout = new BufferedWriter(new FileWriter("clusteringDataset/docVectorsBinary"));
		BufferedWriter dictFout = new BufferedWriter(new FileWriter("clusteringDataset/dict"));
		BufferedWriter rawTextFout = new BufferedWriter(new FileWriter("clusteringDataset/rawText"));
		BufferedWriter dfFout = new BufferedWriter(new FileWriter("clusteringDataset/df"));
		BufferedWriter userIDFout = new BufferedWriter(new FileWriter("clusteringDataset/userID"));
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
				TopScoreDocCollector collector = TopScoreDocCollector
						.create(Math.max(0, totalHitCollector.getTotalHits()));
				searcher.search(finalQuery, collector);
				ScoreDoc[] hits = collector.topDocs().scoreDocs;

				System.out.println("City " + cityName[city] + " " + collector.getTotalHits() + " hits.");

				HashMap<String, Integer> hasHit = new HashMap<String, Integer>();
				int dupcount = 0;
				for (int i = 0; i < hits.length; ++i) {
					int docId = hits[i].doc;
					Document d;

					d = searcher.doc(docId);
					if (hasHit.containsKey(d.get(TweetStreamReader.StatusField.ID.name))) {
						System.out.println("Hit once! Duplicate bad");
						dupcount += 1;
					} else
						hasHit.put(TweetStreamReader.StatusField.ID.name, 0);
					// if (d.get((TweetStreamReader.StatusField.PLACE.name)) !=
					// null)
					// System.out.print(" " +
					// d.get((TweetStreamReader.StatusField.ID.name)) + " "
					// + d.get((TweetStreamReader.StatusField.PLACE.name)));

					rawTextFout.write(d.get(TweetStreamReader.StatusField.TEXT.name).replaceAll("[\\r\\n]+", " "));
					rawTextFout.newLine();
					rawTextFout.flush();
					docCount += 1;

					List<String> fields = new ArrayList<String>(Arrays.asList(IndexTweets.StatusField.TEXT.name,
							IndexTweets.StatusField.USER_DESCRIPTION.name));
					for (String field : fields) {
						Terms terms = reader.getTermVector(docId, field);
						if (terms != null && terms.size() > 0) {
							TermsEnum termsEnum = terms.iterator();
							BytesRef term = null;
							while ((term = termsEnum.next()) != null) {
								DocsEnum docsEnum = termsEnum.docs(null, null);
								int docIdEnum;
								while ((docIdEnum = docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
									String thisTerm = field + ":" + term.utf8ToString();
									int termID;
									if (dict.containsKey(thisTerm)) {
										termID = dict.get(thisTerm);

									} else {
										termID = dict.size();
										dict.put(thisTerm, termID);
									}

									docVectorsFout.write(termID + ":" + docsEnum.freq() + " ");
									docVectorsBinaryFout.write(termID + ":1 ");

								}
							}
						}

					}

					fields = new ArrayList<String>(
							Arrays.asList(IndexTweets.StatusField.USER_URL.name, "tweetOutlinkDomain"));
					for (String field : fields) {

						if (d.get(field) != null) {
							for (String url : d.get(field).split(" ")) {
								
								System.out.println(StringUtils.strip(url, "\""));

								String domain = getDomainName(StringUtils.strip(url, "\""));
								if (domain != null) {
									String thisTerm = field + ":" + d.get(field);
									int termID;
									if (dict.containsKey(thisTerm)) {
										termID = dict.get(thisTerm);

									} else {
										termID = dict.size();
										dict.put(thisTerm, termID);
									}
									docVectorsFout.write(termID + ":1 ");
									docVectorsBinaryFout.write(termID + ":1 ");
								}
							}

						}
					}

					fields = new ArrayList<String>(Arrays.asList(IndexTweets.StatusField.USER_LOCATION.name));
					for (String field : fields) {
						if (d.get(field) != null) {
							String thisTerm = field + ":" + d.get(field);
							int termID;
							if (dict.containsKey(thisTerm)) {
								termID = dict.get(thisTerm);

							} else {
								termID = dict.size();
								dict.put(thisTerm, termID);
							}

							docVectorsFout.write(termID + ":1 ");
							docVectorsBinaryFout.write(termID + ":1 ");
						}
					}
					docVectorsFout.newLine();
					docVectorsFout.flush();
					docVectorsBinaryFout.newLine();
					docVectorsBinaryFout.flush();

					// rawFout.write(d.get(TweetStreamReader.StatusField.TEXT.name).replaceAll("[\\t\\n\\r]",
					// " "));
					// rawFout.newLine();
					// rawFout.flush();
				}
			}

			SortedSet<String> terms = new TreeSet<String>(dict.keySet());
			for (String term : terms) {
				dictFout.write(term + " " + dict.get(term));
				dictFout.newLine();
				// dictReverseFout.write(dict.get(term) + " " + term);
				// dictReverseFout.newLine();

			}

			goldFout.close();
			docVectorsFout.close();
			dictFout.close();
			docVectorsBinaryFout.close();
			reader.close();

		}

	}
}
