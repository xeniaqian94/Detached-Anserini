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

package io.anserini.document.twitter;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import com.google.common.base.Preconditions;

import io.anserini.index.IndexTweets;

/**
 * Abstraction for a corpus of statuses. A corpus is assumed to consist of a
 * number of blocks, each represented by a gzipped file within a root directory.
 * This object will allow to caller to read through all blocks, in sorted
 * lexicographic order of the files.
 */
public class JsonStatusCorpusReader implements StatusStream {
	private static final Logger LOG = LogManager.getLogger(JsonStatusCorpusReader.class);
	private final File[] files;
	private int nextFile = 0;
	private JsonStatusBlockReader currentBlock = null;

	public JsonStatusCorpusReader(File file) throws IOException {
		Preconditions.checkNotNull(file);

		if (!file.isDirectory()) {
			throw new IOException("Expecting " + file + " to be a directory!");
		}

		files = file.listFiles(new FileFilter() {
			public boolean accept(File path) {
				// System.out.println("Currently checking corpus block .gz
				// "+path.toString()+" "+path.toString().contains("2015-12-"));
				return (path.getName().endsWith(".gz") && path.toString().contains("2015-12-")) ? true : false;
			}
		});
		System.out.println("Check recursion: files listed # " + files.length);

		if (files.length == 0) {
			throw new IOException(file + " does not contain any .gz files!");
		}
	}

	/**
	 * Returns the next status, or <code>null</code> if no more statuses.
	 */
	public Status next() throws IOException {
		if (currentBlock == null) {
			currentBlock = new JsonStatusBlockReader(files[nextFile]);
			LOG.info("Switched to file " + files[nextFile].getName());
			nextFile++;
		}

		Status status = null;
		while (true) {
			status = currentBlock.next();
			if (status != null) {
				return status;
			}

			if (nextFile >= files.length) {
				// We're out of files to read. Must be the end of the corpus.
				return null;
			}

			currentBlock.close();
			// Move to next file.
			currentBlock = new JsonStatusBlockReader(files[nextFile]);
			LOG.info("Switched to file " + files[nextFile].getName());
			nextFile++;
		}
	}

	public void close() throws IOException {
		currentBlock.close();
	}

	public static class Args {
		@Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
		public String input;
	}

	public static void main(String[] argv) throws IOException {
		Args args = new Args();
		CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

		try {
			parser.parseArgument(argv);
		} catch (CmdLineException e) {
			System.err.println(e.getMessage());
			parser.printUsage(System.err);
			System.exit(-1);
		}

		long minId = Long.MAX_VALUE, maxId = Long.MIN_VALUE, cnt = 0;
		JsonStatusCorpusReader tweets = new JsonStatusCorpusReader(new File(args.input));
		Status tweet = null;
		while ((tweet = tweets.next()) != null) {
			cnt++;
			long id = tweet.getId();

			System.out.println("id: " + id);

			if (id < minId)
				minId = id;
			if (id > maxId)
				maxId = id;

			if (cnt % 100000 == 0) {
				System.out.println("Read " + cnt + " tweets");
			}
		}
		tweets.close();

		System.out.println("Read " + cnt + " in total.");
		System.out.println("MaxId = " + maxId);
		System.out.println("MinId = " + minId);
	}
}
