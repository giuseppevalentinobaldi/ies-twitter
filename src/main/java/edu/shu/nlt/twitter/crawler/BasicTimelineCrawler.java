package edu.shu.nlt.twitter.crawler;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;


import winterwell.jtwitter.OAuthSignpostClient;
import winterwell.jtwitter.Twitter;
import winterwell.jtwitter.TwitterException.TwitLongerException;
import edu.shu.nlt.twitter.crawler.data.Status;
import edu.shu.nlt.twitter.crawler.data.Timeline;
import edu.shu.nlt.twitter.crawler.data.User;
import edu.shu.nlt.twitter.crawler.data.UserProfile;
import edu.shu.nlt.twitter.crawler.repository.DiskCache;
import edu.shu.nlt.twitter.crawler.repository.PersistentCache;

/**
 * Crawls in the current thread.
 * 
 * @author shu
 * 
 */
public class BasicTimelineCrawler implements Runnable {

	static final int NUM_OF_UPDATES_TO_RETRIEVE = 20;

	private static List<Status> adaptStatusList(List<winterwell.jtwitter.Status> statusList) {
		List<Status> adaptedStatus = new ArrayList<Status>(statusList.size());

		for (winterwell.jtwitter.Status status : statusList) {
			adaptedStatus.add(Status.getInstance(status));
		}

		return adaptedStatus;
	}

	/**
	 * Ensures that user friend data is loaded for user
	 * 
	 * @param user
	 * @return
	 */
	public static Timeline ensureTimeline(PersistentCache repository, Twitter twitter, User user,
			boolean appendTimelines) {

		Timeline cachedTimeline = Timeline.getInstance(repository, user.getScreenName());

		if (cachedTimeline != null && !(appendTimelines && needsUpdate(cachedTimeline, NUM_OF_UPDATES_TO_RETRIEVE))) {

			System.out.println("Cached timeline " + cachedTimeline.getStatusList().size() + ": " + user.getScreenName()
					+ "\t" + repository.getLastUpdated(Timeline.getCacheKey(user.getScreenName())));
			return cachedTimeline;
		}

		Timeline timeline;
		try {
			checkRateLimitStatus();
			List<winterwell.jtwitter.Status> statusList = twitter.getUserTimeline(user.getScreenName());
			timeline = Timeline.getInstance(user.getScreenName(), adaptStatusList(statusList), new Date());

		} catch (Exception ex) {
			if (ex.getMessage().contains("401 Unauthorized") || ex.getMessage().contains("404 Error")) {
				System.out.println("401/404 ignored" + user.getScreenName());

				timeline = Timeline.getInstance(user.getScreenName(), null, new Date());
			} else {

				System.out.println("Error / Rate limit reached, sleeping for pre-set time." + new Date() + " "
						+ ex.getMessage());
				try {
					Thread.sleep(1000 * 60 * Util.ThrottlerWaitTimeMinutes);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}

				return ensureTimeline(repository, twitter, user, appendTimelines);
			}
		}

		if (cachedTimeline != null) {
			timeline.append(cachedTimeline);
			System.out.println("Updated timeline +"
					+ (timeline.getStatusList().size() - cachedTimeline.getStatusList().size()) + ": "
					+ user.getScreenName());
		} else {
			System.out.println("New timeline " + timeline.getStatusList().size() + ": " + user.getScreenName());
		}
		// update cache
		repository.put(timeline);
		return timeline;

	}

	public static void main(String[] args) {

		OAuthSignpostClient oauthClient = new OAuthSignpostClient("N2LZiDdNAqY1qtgJ8EPRoAdx9",
				"ayLGG7YtnVykMbkfNZ3XyYZRo1FDCC4sIO8VBSJELBOoM6lYHU", "oob");
		// Open the authorisation page in the user's browser. On a desktop, we
		// can do that like this:
		oauthClient.authorizeDesktop();
		// get the pin
		String v = oauthClient.askUser("Please enter the verification PIN from Twitter");
		oauthClient.setAuthorizationCode(v);
		// Store the authorisation token details for future use
		String[] accessToken = oauthClient.getAccessToken();

		Twitter twitter = new Twitter("giuseppe14291", oauthClient);

		DiskCache cache = DiskCache.getInstance();

		BasicTimelineCrawler crawler = new BasicTimelineCrawler(twitter, cache, "giuseppe14291");
		crawler.run();
	}

	public static boolean needsUpdate(Timeline timeline, int numOfUpdatesToRetrieve) {
		double daysSinceLastUpdate = (double) (System.currentTimeMillis() - timeline.getLastUpdated().getTime())
				/ (double) (3600 * 24 * 1000);

		double updateFrequency = timeline.getUpdateFrequency();

		// return true if estimated number of new updates exceeds the number of
		// updates we can retrieve per call
		return (daysSinceLastUpdate * updateFrequency) > numOfUpdatesToRetrieve;

	}

	private PersistentCache repository;

	private String rootUser;

	private static Twitter twitter;

	public BasicTimelineCrawler(Twitter twitter, PersistentCache repository, String rootUser) {
		super();
		this.twitter = twitter;
		this.repository = repository;
		this.rootUser = rootUser;
	}

	/**
	 * Depth first algorithm for crawling the timeline.
	 * 
	 * Assume that all relevant social network data is already cached
	 * 
	 * @param rootUser
	 */
	private synchronized void crawlBreadthFirst(User rootUser) {
		LinkedList<User> queue = new LinkedList<User>();
		queue.add(rootUser);

		while (!queue.isEmpty()) {
			User user = queue.remove();
			// UserProfile userProfile = UserProfile.getInstance(repository,
			// userScreenName);
			UserProfile userProfile = BasicFriendGraphCrawler.ensureUserProfile(repository, twitter, user);

			// Only perform timeline loading and tree expansion for cached user
			// profiles
			if (userProfile != null) {
				ensureTimeline(repository, twitter, userProfile.getUser(), true);

				for (User friend : userProfile.getFriends()) {
					queue.add(friend);
				}
			}
		}
	}

	private static void checkRateLimitStatus() {
		try {
			int rateLimitStatus = twitter.getRateLimitStatus();

			if (rateLimitStatus <= 0) {
				int remainingTime = 900000;
				System.out.println("Twitter request rate limit reached. Waiting " + remainingTime / 60
						+ " minutes to request again.");
				try {
					Thread.sleep(remainingTime * 1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		} catch (TwitLongerException te) {
			System.err.println(te.getMessage());
			if (te.getMessage().equals("503")) {
				try {
					Thread.sleep(120 * 1000);// wait 2 minutes
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		} catch (Exception e) {
			System.err.println(e.getMessage());

		}
	}

	public void run() {
		UserProfile cachedValue = UserProfile.getInstance(repository, rootUser);
		User rootUserData = (cachedValue != null) ? cachedValue.getUser()
				: User.getInstance(twitter.show(769181646176284672L));
		crawlBreadthFirst(rootUserData);

	}
}
