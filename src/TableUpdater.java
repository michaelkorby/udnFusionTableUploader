import java.awt.image.BufferedImage;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.imageio.ImageIO;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import javax.swing.text.StyledEditorKit.ForegroundAction;

import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;
import org.jasypt.properties.EncryptableProperties;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver.Builder;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.DataStoreFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.fusiontables.Fusiontables;
import com.google.api.services.fusiontables.FusiontablesScopes;
import com.google.api.services.fusiontables.model.Sqlresponse;
import com.google.api.services.fusiontables.model.Table;
import com.google.api.services.fusiontables.model.TableList;
import com.googlecode.htmlcompressor.compressor.HtmlCompressor;

public class TableUpdater {


	/**
	 * Specific pointer to the client_secrets file we use for authorization.
	 * This file is obtained from google apps. Log in to google apps for joe@urinal.net, click into the UDN application under OAuth and there'll be a button to download client_secretes.json
	 */
	private static final String CLIENT_SECRETS_LOCATION = "C:\\users\\mkorby\\Google Drive\\Code\\urinal\\client_secrets.json";

	/**
	 * Properties file
	 */
	private static final String TABLE_UPDATER_PROPERTIES = "C:\\users\\mkorby\\Google Drive\\Code\\urinal\\urinal.properties";

	//The local server receiver URL (host and port below) have to match exactly the one that's configured as part of this account on the Google API site. The account is referenced in client_secrets.json
	private static final int LOCAL_SERVER_RECEIVER_PORT = 8080;
	private static final String LOCAL_SERVER_RECEIVER_HOST = "localhost";
	private static final SimpleDateFormat GOOGLE_DATE_FORMAT = new SimpleDateFormat("MM/dd/yy hh:mm a");
	private static final Pattern URINAL_PATTERN = Pattern.compile("\\s*<urinal name=\"(.*)\" url=\"(.*)\"/>");
	private static final Pattern MARKER_PATTERN = Pattern.compile("\\s*<marker lat=\"(.*)\" lng=\"(.*)\" location=\"(.*)\">");
	private static final Pattern LINK_PATTERN = Pattern.compile("<a.*>(.*)</a>");
	private static final Pattern URL_PATTERN = Pattern.compile(".*href\\s*=\\s*\"(.*)\" target.*");
	private static final String UDN_US_LOCATIONS_XML_URL = "http://urinal.net/usLocations.xml";
	private static final int ATTEMPT_COUNT = 10;
	private static final String LOCATION_TABLE_NAME = "Location table";
	private static final HtmlCompressor HTML_COMPRESSOR = new HtmlCompressor();
	private static final String RATE_LIMIT_EXCEEDED = "Rate Limit Exceeded";
	private static final String BLANK_SUFFIX = "_blank\">";
	private static final int MAX_URINALS_PER_BUBBLE_DUE_TO_QUERY_SIZE_CONSTRAINT = 14;
	private static final int MAX_IMAGES_PER_BUBBLE_DUE_TO_QUERY_SIZE_CONSTRAINT = 5;
	private static final String TOO_LARGE_TO_PROCESS = "is too large to process";

	/**
	 * Global instance of the {@link DataStoreFactory}. The best practice is to make it a single
	 * globally shared instance across your application.
	 */
	private static FileDataStoreFactory dataStoreFactory;

	/** Global instance of the HTTP transport. */
	private static HttpTransport httpTransport;

	/** Directory to store user credentials. */
	private static final java.io.File DATA_STORE_DIR = new java.io.File(System.getProperty("user.home"), ".store/UDN_table_updater");

	/**
	 * Be sure to specify the name of your application. If the application name is {@code null} or
	 * blank, the application will log a warning. Suggested format is "MyCompany-ProductName/1.0".
	 */
	private static final String APPLICATION_NAME = "UrinalDotNet/1.1";


	private static final String USERNAME = "mkorby";
	private static final String MY_EMAIL_ADDRESS = "mkorby@gmail.com";
	static Properties mailServerProperties;
	static Session getMailSession;
	static MimeMessage generateMailMessage;

	static {
		HTML_COMPRESSOR.setRemoveIntertagSpaces(true);
		HTML_COMPRESSOR.setRemoveQuotes(true);
		HTML_COMPRESSOR.setRemoveLinkAttributes(true);
		HTML_COMPRESSOR.setSimpleBooleanAttributes(true);
	}

	private final Map<String, String> _cityCoordinateMap = new HashMap<String, String>();
	/**
	 * Global instance of the JSON factory.
	 */
	private static final JsonFactory JSON_FACTORY = new JacksonFactory();
	private static Fusiontables fusiontables;

	/**
	 * Authorizes the installed application to access user's protected data.
	 */
	private static Credential authorize() throws Exception {
		// load client secrets
		final GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(new FileInputStream(CLIENT_SECRETS_LOCATION)));
		if (clientSecrets.getDetails().getClientId().startsWith("Enter") || clientSecrets.getDetails().getClientSecret().startsWith("Enter ")) {
			throw new RuntimeException("Enter Client ID and Secret from https://code.google.com/apis/console/?api=fusiontables "
					+ "into fusiontables-cmdline-sample/src/main/resources/client_secrets.json");
		}
		// set up authorization code flow
		GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(
				httpTransport, JSON_FACTORY, clientSecrets,
				Collections.singleton(FusiontablesScopes.FUSIONTABLES)).setDataStoreFactory(
						dataStoreFactory).build();

		// authorize
		Builder builder = new LocalServerReceiver.Builder();
		builder.setHost(LOCAL_SERVER_RECEIVER_HOST);
		builder.setPort(LOCAL_SERVER_RECEIVER_PORT);
		LocalServerReceiver localServerReceiver = builder.build();
		return new AuthorizationCodeInstalledApp(flow, localServerReceiver).authorize("user");
	}


	public TableUpdater() throws Exception {
		httpTransport = GoogleNetHttpTransport.newTrustedTransport();
		dataStoreFactory = new FileDataStoreFactory(DATA_STORE_DIR);

		// authorization
		final Credential credential = authorize();
		// set up global FusionTables instance
		fusiontables = new Fusiontables.Builder(httpTransport, JSON_FACTORY, credential).setApplicationName(APPLICATION_NAME).build();

		final String locationTableId = getTableIdByName(LOCATION_TABLE_NAME);

		final String now = GOOGLE_DATE_FORMAT.format(new Date());

		final Map<String, ArrayList<String>> allLocations = getAllLocations(UDN_US_LOCATIONS_XML_URL);

		final ArrayList<String> cities = new ArrayList<String>();
		cities.addAll(allLocations.keySet());
		Collections.sort(cities);

		final ArrayList<String> allQueries = new ArrayList<String>();

		System.out.println("Building queries for " + cities.size() + " cities");
		int i = 0;

		for (final String city : cities) {
			//Update the log on our progress
			if (++i % 10 == 0) {
				System.out.println("City #" + i + ": " + city);
			}


			final ArrayList<String> locations = allLocations.get(city);
			Collections.sort(locations, new LinkComparator());
			final StringBuilder updateQuery = new StringBuilder();
			final StringBuilder htmlBuilder = new StringBuilder();
			final int numberOfUrinals = locations.size();
			updateQuery.append("insert into ").append(locationTableId)
			.append(" (Urinals, Number, City, Location, UpdateTime, Marker) values ('");
			int locationCount = 0;

			// Currently, the Google API only allows a query of 1500 characters
			// max. This is a problem for larger cities.
			// We max out at 14 locations per bubble, that seems to work. Also,
			// show images for bubbles with 5 or fewer urinals only to save on
			// space.
			for (String location : locations) {
				locationCount++;
				if (locationCount > MAX_URINALS_PER_BUBBLE_DUE_TO_QUERY_SIZE_CONSTRAINT) {
					htmlBuilder.append("and ").append((numberOfUrinals - locationCount) + 1).append(" more...");
					break;
				}

				// No room to show images if we have more than 5 urinals here.
				// Max length of query is currently 1500 chars
				final String image = locations.size() <= MAX_IMAGES_PER_BUBBLE_DUE_TO_QUERY_SIZE_CONSTRAINT
						? getFirstImage(location) : null;
						if (image != null) {
							location = location.replace(BLANK_SUFFIX, BLANK_SUFFIX + image + "<br>");
						}
						htmlBuilder.append(location).append("<p>");
			}
			final String compressedHtml = HTML_COMPRESSOR.compress(htmlBuilder.toString() + "'");

			updateQuery.append(compressedHtml).append(",").append(numberOfUrinals);
			final String escapedCity = escape(city);
			updateQuery.append(",'").append(escapedCity).append("',");
			final String latLng = _cityCoordinateMap.get(city);
			updateQuery.append("'").append(latLng).append("',");
			updateQuery.append("'").append(now).append("',");
			String markerName = "red_stars";
			if (numberOfUrinals <= 10) {
				markerName = numberOfUrinals + "_blue";
			}
			updateQuery.append("'").append(markerName).append("')");
			allQueries.add(updateQuery.toString());
		}

		Collections.sort(allQueries, new Comparator<String>() {
			@Override
			public int compare(final String o1, final String o2) {
				return o2.length() - o1.length();
			}
		});

		// Delete all rows in the table first. We can't delete only the old rows
		// because that results in too many requests for google
		runUpdateMultipleAttempts("delete from " + locationTableId);

		// Run the insert queries one by one
		for (final String query : allQueries) {
			runUpdateMultipleAttempts(query);
		}
	}

	/**
	 * List tables for the authenticated user.
	 */
	private String getTableIdByName(final String tableName) throws IOException {
		// Fetch the table list
		final Fusiontables.Table.List listTables = fusiontables.table().list();
		// Tell the list operation how many results to return, otherwise we just
		// get 25 results back, which isn't enough.
		listTables.setMaxResults(100000l);
		final TableList tablelist = listTables.execute();

		if ((tablelist.getItems() == null) || tablelist.getItems().isEmpty()) {
			throw new RuntimeException("No tables found at all.");
		}

		for (final Table table : tablelist.getItems()) {
			if (tableName.equals(table.getName())) {
				return table.getTableId();
			}
		}
		throw new RuntimeException("Cannot find table " + tableName);
	}

	private void runUpdateMultipleAttempts(final String query) {
		System.out.println(query.length() + " - query = " + query);
		for (int i = 1; i <= ATTEMPT_COUNT; i++) {
			try {
				final String response = run(query);
				System.out.println(response);
				break;
			} catch (final Exception ie) {
				if (i == ATTEMPT_COUNT) {
					// That's it, giving up
					throw new RuntimeException(ie);
				}
				if (ie.getMessage().contains(RATE_LIMIT_EXCEEDED)) {
					// This is a too-many-requests issue. Sleep for a minute and
					// try again
					System.err.println("Rate limit exceeded. Sleeping for a minute and retrying");
					try {
						Thread.sleep(1000 * 60);
					} catch (final InterruptedException e) {
						// Doesn't matter
					}
				} else if (ie.getMessage().contains(TOO_LARGE_TO_PROCESS)) {
					// This individual query is over 1500 chars long. Retrying
					// won't really help here.
					System.err.println("QUERY IS TOO LARGE at " + query.length() + " characters. (1500 chars max)");
					ie.printStackTrace();
					return;
				} else {
					System.err.println("Caught Exception. Will retry");
					ie.printStackTrace();
				}
			}
		}
	}

	/**
	 * Get the first image at this location and return a tiny-ized <img> tag for
	 * it
	 *
	 * @param location
	 * @return
	 */
	private String getFirstImage(final String location) {
		try {
			final Matcher locationMatcher = URL_PATTERN.matcher(location);
			if (locationMatcher.matches()) {
				String url = locationMatcher.group(1);
				if (!url.endsWith("/")) {
					url += "/";
				}
				final String stringContent = getWebContents(url);
				final String[] pieces = stringContent.split("\"");
				for (final String piece : pieces) {
					if (endsWith(piece, ".jpg", true)) {
						// This is what we want

						// Get the image itself to see if it's vertical or
						// horizontal
						final String imageUrl = url + piece;
						final BufferedImage image = ImageIO.read(new URL(imageUrl));
						if (image != null) {
							final double widthToHeightRatio = (1.0d * image.getWidth()) / image.getHeight();
							final int width = (int) (100 * widthToHeightRatio);

							return "<img src=\"" + imageUrl + "\" height=100 width=" + width + " border=0>";
						}
					}
				}
			}
		} catch (final Exception ie) {
			ie.printStackTrace();
			// Oh, well
		}

		return null;
	}

	private String run(final String query) throws IOException {

		final Fusiontables.Query.Sql sql = fusiontables.query().sql(query);

		try {
			final Sqlresponse response = sql.execute();
			return response.toString();
		} catch (final IllegalArgumentException e) {
			// For google-api-services-fusiontables-v1-rev1-1.7.2-beta this
			// exception will always
			// been thrown.
			// Please see issue 545: JSON response could not be deserialized to
			// Sqlresponse.class
			// http://code.google.com/p/google-api-java-client/issues/detail?id=545
		}
		return query;
	}

	private Map<String, ArrayList<String>> getAllLocations(final String url) throws MalformedURLException, IOException {
		final Map<String, ArrayList<String>> cityMap = new HashMap<String, ArrayList<String>>();

		final String stringContent = getWebContents(url);
		final LineNumberReader reader = new LineNumberReader(new StringReader(stringContent));
		String line = "";
		String latLng = null;
		String link = null;
		String city = null;
		while (true) {
			line = reader.readLine();
			if (line == null) {
				break;
			}

			final Matcher markerMatcher = MARKER_PATTERN.matcher(line);
			final Matcher urinalMatcher = URINAL_PATTERN.matcher(line);

			if (markerMatcher.matches()) {
				// This is a marker
				final String lat = markerMatcher.group(1);
				final String lng = markerMatcher.group(2);
				latLng = lat + "," + lng;
				city = markerMatcher.group(3);
				_cityCoordinateMap.put(city, latLng);

			} else if (urinalMatcher.matches()) {
				final String name = urinalMatcher.group(1);
				final String uri = urinalMatcher.group(2);
				link = "<a href=\"http://urinal.net" + uri + "\" target=\"_blank\">" + escape(name) + "</a>";

				ArrayList<String> locationsForCity = cityMap.get(city);
				if (locationsForCity == null) {
					locationsForCity = new ArrayList<String>();
					cityMap.put(city, locationsForCity);
				}
				locationsForCity.add(link);
			}
		}

		return cityMap;

	}

	private String getWebContents(final String url) throws IOException {
		final WebFile file = new WebFile(url);
		final Object content = file.getContent();
		if (content instanceof String) {
			return stripNulls((String) content);
		}
		final String stringContent = new String((byte[]) content);

		// For some reason, some times we read a ton of null characters inside a
		// line, thousands of them. Strip them out right away.
		return stripNulls(stringContent);
	}

	private String stripNulls(final String soureString) {
		return soureString.replaceAll("\u0000", "");
	}

	private String escape(final String escapeMe) {
		return escapeMe.replaceAll("'", "");
	}

	/**
	 * Check if a String ends with a specified suffix (optionally case
	 * insensitive).
	 *
	 * @see java.lang.String#endsWith(String)
	 * @param str
	 *            the String to check, may be null
	 * @param suffix
	 *            the suffix to find, may be null
	 * @param ignoreCase
	 *            indicates whether the compare should ignore case (case
	 *            insensitive) or not.
	 * @return <code>true</code> if the String starts with the prefix or both
	 *         <code>null</code>
	 */
	private boolean endsWith(final String str, final String suffix, final boolean ignoreCase) {
		if ((str == null) || (suffix == null)) {
			return ((str == null) && (suffix == null));
		}
		if (suffix.length() > str.length()) {
			return false;
		}
		final int strOffset = str.length() - suffix.length();
		return str.regionMatches(ignoreCase, strOffset, suffix, 0, suffix.length());
	}

	public static void generateAndSendEmail(final String password, final String message)
			throws AddressException, MessagingException {
		mailServerProperties = System.getProperties();
		mailServerProperties.put("mail.smtp.port", "587"); // TLS Port
		mailServerProperties.put("mail.smtp.auth", "true"); // Enable
		// Authentication
		mailServerProperties.put("mail.smtp.starttls.enable", "true"); // Enable
		// StartTLS
		getMailSession = Session.getDefaultInstance(mailServerProperties, null);
		generateMailMessage = new MimeMessage(getMailSession);
		generateMailMessage.addRecipient(Message.RecipientType.TO, new InternetAddress(MY_EMAIL_ADDRESS));
		generateMailMessage.setSubject("UDN Map Updater Failed");
		final String emailBody = "An error occured while running the UDN Fusion Table Updater on " + new Date() + ":<br>"
				+ message;
		generateMailMessage.setContent(emailBody, "text/html");
		final Transport transport = getMailSession.getTransport("smtp");
		// Enter your correct gmail UserID and Password
		transport.connect("smtp.gmail.com", USERNAME, password);
		transport.sendMessage(generateMailMessage, generateMailMessage.getAllRecipients());
		transport.close();
	}

	/**
	 * This method pulls back the password that we need to log in to gmail to send mail
	 * 
	 * In order to re-encrypt the password, you'll need the Jasypt command line
	 * tools More info here: http://www.jasypt.org/cli.html
	 * 
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private static String getPasswordFromEncryptedFile(final String paramsKey)
			throws FileNotFoundException, IOException {
		/*
		 * First, create (or ask some other component for) the adequate
		 * encryptor for decrypting the values in our .properties file.
		 */
		final StandardPBEStringEncryptor encryptor = new StandardPBEStringEncryptor();
		encryptor.setPassword("encryptionToken");

		/*
		 * Create our EncryptableProperties object and load it the usual way.
		 */

		final Properties props = new EncryptableProperties(encryptor);
		props.load(new FileInputStream(TABLE_UPDATER_PROPERTIES));

		/*
		 * To get a non-encrypted value, we just get it with getProperty...
		 */

		final String password = props.getProperty(paramsKey);
		return password;
	}

	/**
	 * Compare links according to their actual text.
	 *
	 * @author mkorby
	 */
	public class LinkComparator implements Comparator<String> {

		@Override
		public int compare(final String o1, final String o2) {
			if (o1 == null) {
				return 1;
			} else if (o2 == null) {
				return -1;
			}
			final Matcher pattern1 = LINK_PATTERN.matcher(o1);
			final Matcher pattern2 = LINK_PATTERN.matcher(o2);
			if (pattern1.matches() && pattern2.matches()) {
				try {
					final String linkText1 = pattern1.group(1).trim();
					final String linkText2 = pattern2.group(1).trim();
					return linkText1.compareTo(linkText2);
				} catch (final IndexOutOfBoundsException ie) {
					// This shouldn't happen. Can't find the matching group.
					// Fall through to no match case.
				}
			}
			return o1.compareTo(o2);
		}

	}

	/**
	 * Get a web file.
	 */
	public final class WebFile {
		// Saved response.
		private java.util.Map<String, java.util.List<String>> responseHeader = null;
		private java.net.URL responseURL = null;
		private int responseCode = -1;
		private String MIMEtype = null;
		private String charset = null;
		private Object content = null;

		/**
		 * Open a web file.
		 */
		public WebFile(final String urlString) throws java.net.MalformedURLException, java.io.IOException {
			// Open a URL connection.
			final java.net.URL url = new java.net.URL(urlString);
			final java.net.URLConnection uconn = url.openConnection();
			if (!(uconn instanceof java.net.HttpURLConnection)) {
				throw new java.lang.IllegalArgumentException("URL protocol must be HTTP.");
			}
			final java.net.HttpURLConnection conn = (java.net.HttpURLConnection) uconn;

			// Set up a request.
			conn.setConnectTimeout(10000); // 10 sec
			conn.setReadTimeout(10000); // 10 sec
			conn.setInstanceFollowRedirects(true);
			conn.setRequestProperty("User-agent", "spider");

			// Send the request.
			conn.connect();

			// Get the response.
			responseHeader = conn.getHeaderFields();
			responseCode = conn.getResponseCode();
			responseURL = conn.getURL();
			final int length = conn.getContentLength();
			final String type = conn.getContentType();
			if (type != null) {
				final String[] parts = type.split(";");
				MIMEtype = parts[0].trim();
				for (int i = 1; (i < parts.length) && (charset == null); i++) {
					final String t = parts[i].trim();
					final int index = t.toLowerCase().indexOf("charset=");
					if (index != -1) {
						charset = t.substring(index + 8);
					}
				}
			}

			// Get the content.
			final java.io.InputStream stream = conn.getErrorStream();
			if (stream != null) {
				content = readStream(length, stream);
			} else if (((content = conn.getContent()) != null) && (content instanceof java.io.InputStream)) {
				content = readStream(length, (java.io.InputStream) content);
			}
			conn.disconnect();
		}

		/**
		 * Read stream bytes and transcode.
		 */
		private Object readStream(final int length, final java.io.InputStream stream) throws java.io.IOException {
			final int buflen = Math.max(1024, Math.max(length, stream.available()));
			byte[] buf = new byte[buflen];
			byte[] bytes = null;

			for (int nRead = stream.read(buf); nRead != -1; nRead = stream.read(buf)) {
				if (bytes == null) {
					bytes = buf;
					buf = new byte[buflen];
					continue;
				}
				final byte[] newBytes = new byte[bytes.length + nRead];
				System.arraycopy(bytes, 0, newBytes, 0, bytes.length);
				System.arraycopy(buf, 0, newBytes, bytes.length, nRead);
				bytes = newBytes;
			}

			if (charset == null) {
				return bytes;
			}
			try {
				return new String(bytes, charset);
			} catch (final java.io.UnsupportedEncodingException e) {
			}
			return bytes;
		}

		/**
		 * Get the content.
		 */
		public Object getContent() {
			return content;
		}

		/**
		 * Get the response code.
		 */
		public int getResponseCode() {
			return responseCode;
		}

		/**
		 * Get the response header.
		 */
		public java.util.Map<String, java.util.List<String>> getHeaderFields() {
			return responseHeader;
		}

		/**
		 * Get the URL of the received page.
		 */
		public java.net.URL getURL() {
			return responseURL;
		}

		/**
		 * Get the MIME type.
		 */
		public String getMIMEType() {
			return MIMEtype;
		}
	}

	public static void main(final String... args) {
		String mailPassword = null;

		try {
			mailPassword = getPasswordFromEncryptedFile("mailer.password");
			new TableUpdater();
		} catch (final Throwable ie) {
			// Let's send an email about this
			if (mailPassword != null) {
				try {
					generateAndSendEmail(mailPassword, ie.toString());
				} catch (final MessagingException e1) {
					e1.printStackTrace();
				}
			}
			ie.printStackTrace();
		}
	}
}
