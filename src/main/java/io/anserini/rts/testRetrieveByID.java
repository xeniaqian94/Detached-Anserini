package io.anserini.rts;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import twitter4j.GeoQuery;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.TwitterObjectFactory;
import twitter4j.User;
import twitter4j.auth.AccessToken;
import twitter4j.json.DataObjectFactory;

public class testRetrieveByID {

	final static Twitter twitter = new TwitterFactory().getInstance();

	static class chainElement {
		Long statusID;
		Status status;

		chainElement(Long statusID, Status status) {
			this.statusID = statusID;
			this.status = status;
		}
		
		public String toString(){
			return statusID.toString();
		}
	}

	public static void Initialize() throws TwitterException, IOException {
		BufferedReader br = new BufferedReader(new FileReader("rawJSON/Mon_Sep_19_121305"));

		String jsonStatus = br.readLine(); // Content read from a file
		br.close();
		Status status = TwitterObjectFactory.createStatus(jsonStatus); // your
																		// status
		User u2 = twitter.showUser(status.getUser().getScreenName()); // you use
																		// the
																		// twitter
																		// object
																		// once
		String jsonUser = TwitterObjectFactory.getRawJSON(status.getUser()); // now
																				// you
																				// won't
																				// get
																				// the
																				// error
		// System.out.print(jsonUser); //your happy json
	}

	public static List<chainElement> getChain(String lastStatusID) throws IOException {

		File thisChain = new File("chainDirectory/" + lastStatusID);
		Long newStatusID = Long.parseLong(lastStatusID);
		BufferedWriter chainFout = new BufferedWriter(new FileWriter(thisChain));
		List<chainElement> chainList = new ArrayList<chainElement>();
		boolean chainStart = false;
		int count = 0;
		while (!chainStart && newStatusID != -1) {
			try {
//				System.out.println("Searching status " + newStatusID);
				Status status = twitter.showStatus(newStatusID);
//				System.out.println("Status got");
				if (status == null) {
					chainStart = true;//
					System.out.println("How come this ID is invalid " + newStatusID);
					// don't know if needed - T4J docs are very bad
				} else {
					String statusJson = DataObjectFactory.getRawJSON(status);
					chainList.add(0, new chainElement(newStatusID, status));
					chainFout.write(statusJson);
					chainFout.newLine();
					count++;
					newStatusID = status.getInReplyToStatusId();
					// System.out.println("@" + status.getUser().getScreenName()
					// + " - " + status.getText() + "\n"
					// + status.getInReplyToUserId());
				}
			} catch (TwitterException e) {

				chainStart = true;
			}
		}
		System.out.println(lastStatusID + ": This conversation chain lasts for " + count + "turns:");
		System.out.println(chainList.toString());
		chainFout.close();

		return chainList;

	}

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		final Twitter twitter = new TwitterFactory().getInstance();
//		getChain("777962135787823106");
		 try {
		 Status status =
		 twitter.showStatus(Long.parseLong("777962148370653184"));
		
		 if (status == null) { //
		 // don't know if needed - T4J docs are very bad
		 } else {
		 String statusJson = DataObjectFactory.getRawJSON(status);
		 System.out.println(statusJson);
		 System.out.println("\n\n"+status.getInReplyToStatusId());
		 System.out.println("@" + status.getUser().getScreenName() + " - " +
		 status.getText() + "\n"
		 + status.getInReplyToUserId());
		 }
		 } catch (TwitterException e) {
		 System.err.print("Failed to search tweets: " + e.getMessage());
		 // e.printStackTrace();
		 // DON'T KNOW IF THIS IS THROWN WHEN ID IS INVALID
		 }

	}

}
