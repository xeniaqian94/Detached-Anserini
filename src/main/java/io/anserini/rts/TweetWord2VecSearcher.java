package io.anserini.rts;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
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
import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer;
import org.deeplearning4j.models.embeddings.wordvectors.WordVectors;
import org.nd4j.linalg.api.ndarray.INDArray;

import io.anserini.nrts.TweetSearcher;
import io.anserini.nrts.TweetStreamIndexer.StatusField;
import twitter4j.TwitterException;

public class TweetWord2VecSearcher {
	private static IndexReader reader;
	private static final String INDEX_OPTION = "index";
	private static final String CITY_OPTION = "city";
	private static final String LONGITUDE_OPTION = "longitude";
	private static final String LATITUDE_OPTION = "latitude";

	private static final String[] cityName = { "NY", "Chicago", "LA", "Philadelphia", "Washington", "Houston",
			"Minneapolis", "Cincinnati", "Portland", "StLouis", "Cleveland", "Pittsburgh" };
	private static final Double[] longitude = { -74.0063889, -87.6500000, -118.253842, -75.1641667, -77.0366667,
			-95.3630556, -93.2636111, -84.4569444, -70.2558333, -90.197778, -81.669722, -79.976389 };
	private static final Double[] latitude = { 40.7141667, 41.8500000, 34.040667, 39.9522222, 38.8950000, 29.7630556,
			44.9800000, 39.1619444, 43.6613889, 38.627222, 41.482222, 40.439722 };
	private static final String WORD2VEC_OPTION = "word2vec";

	public static void main(String[] args) throws IOException, ParseException, TwitterException {
		// TODO Auto-generated method stub

		for (int i = 0; i < cityName.length; i++) {
			System.out.println(cityName[i] + " " + longitude[i] + " " + latitude[i]);

		}

		Options options = new Options();
		options.addOption(INDEX_OPTION, true, "index path");
		options.addOption(WORD2VEC_OPTION, true, "word2vec model path");

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
		WordVectors wordVectors = WordVectorSerializer
				.loadTxtVectors(new File(cmdline.getOptionValue(WORD2VEC_OPTION)));
		int emptyCount = 0;

		IndexSearcher searcher = new IndexSearcher(reader);
		System.out.println("The total number of docs indexed "
				+ searcher.collectionStatistics(TweetStreamReader.StatusField.TEXT.name).docCount());

		BufferedWriter goldFout = new BufferedWriter(new FileWriter("clusteringDataset/gold_standard"));
		BufferedWriter docVectorsFout = new BufferedWriter(new FileWriter("clusteringDataset/docVectors"));
		int docCount = 0;

		for (int city = 0; city < cityName.length; city++) {
			// Pittsburgh's coordinate -79.976389, 40.439722
			Query q_long = NumericRangeQuery.newDoubleRange(TweetStreamReader.StatusField.LONGITUDE.name,
					new Double(longitude[city] - 0.05), new Double(longitude[city] + 0.05), true, true);
			Query q_lat = NumericRangeQuery.newDoubleRange(TweetStreamReader.StatusField.LATITUDE.name,
					new Double(latitude[city] - 0.05), new Double(latitude[city] + 0.05), true, true);

			BooleanQuery bq = new BooleanQuery();

			bq.add(q_long, BooleanClause.Occur.MUST);
			bq.add(q_lat, BooleanClause.Occur.MUST);

			TotalHitCountCollector totalHitCollector = new TotalHitCountCollector();

			searcher.search(bq, totalHitCollector);

			TopScoreDocCollector collector = TopScoreDocCollector.create(Math.max(0, totalHitCollector.getTotalHits()));
			searcher.search(bq, collector);
			ScoreDoc[] hits = collector.topDocs().scoreDocs;

			System.out.println("City " + cityName[city] + " " + collector.getTotalHits() + " hits.");

			for (int i = 0; i < hits.length; ++i) {
				int docId = hits[i].doc;
				Document d;

				d = searcher.doc(docId);

				// rawTextFout.write(d.get(TweetStreamReader.StatusField.TEXT.name).replaceAll("[\\r\\n]+",
				// " "));
				// rawTextFout.newLine();
				docCount += 1;

				Terms terms = reader.getTermVector(docId, TweetStreamReader.StatusField.TEXT.name);
				List<String> termList = new ArrayList<String>();
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
							String thisTerm = term.utf8ToString();
							for (int j = 0; j < docsEnum.freq(); j++)
								termList.add(thisTerm);

						}
					}
				}

				if (termList.size() > 1) {
					INDArray documentVector = wordVectors.getWordVectorsMean(termList);
					if (null != documentVector) {
						for (int j = 0; j < documentVector.length(); j++)
							docVectorsFout.write((j + 1) + ":" + documentVector.getDouble(j) + " ");
						docVectorsFout.newLine();
						goldFout.write(cityName[city]);
						goldFout.newLine();
					} else
						emptyCount += 1;
				} else if (termList.size() == 1) {
					double[] documentVector = wordVectors.getWordVector(termList.get(0));
					if (null != documentVector) {
						for (int j = 0; j < documentVector.length; j++)
							docVectorsFout.write((j + 1) + ":" + documentVector[j] + " ");
						docVectorsFout.newLine();
						goldFout.write(cityName[city]);
						goldFout.newLine();
					} else
						emptyCount += 1;
				} else {
					System.out.println("Document " + docCount + " does not has any terms");
					emptyCount += 1;

				}

			}
		}
		System.out.println("Sentence2Vec empty for " + emptyCount + " documents");

		goldFout.close();
		docVectorsFout.close();
		reader.close();

	}

}
