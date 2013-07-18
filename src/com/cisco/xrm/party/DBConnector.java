package com.cisco.xrm.party;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSet;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class DBConnector {

	public static final int POST_MATCH = 0;
	public static final int PRE_MATCH = 1;
	public static final int FULL_MATCH = 2;
	public static final int DEFAULT_SORT = 0;
	public static final int ASC = 1;
	public static final int DESC = 2;

	private Connection con, con2;
	private Statement st, st2;
	private ResultSet rs, rs2;
	private JSONArray partyArr = new JSONArray();

	public DBConnector() {
		try {
			Class.forName("com.mysql.jdbc.Driver");

			/*
			 * con = DriverManager.getConnection(
			 * "jbdc:mysql://api-wizards.cisco.com:3306/apiwizards", "root",
			 * "Cisco123");
			 */
			con = DriverManager.getConnection(
					"jdbc:mysql://atom3.cisco.com:3306/reddb", "redteam",
					"redteam");
			st = con.createStatement();
			con2 = DriverManager.getConnection(
					"jdbc:mysql://tjthomas-ws:3306/party_db", "tjthomas",
					"party");
			st2 = con2.createStatement();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
	
	public String copyDB() throws JSONException {
		long startTime = System.nanoTime();
		try {
			String query = "SELECT * FROM PureServlet";
			rs = st.executeQuery(query);
			query = "TRUNCATE parties";
			st2.executeUpdate(query);
			query = "INSERT INTO parties VALUES(?, ?)";
			PreparedStatement pstmt = null;
			while (rs.next()) {
				pstmt = con2.prepareStatement(query);
				String party_id = rs.getString("PARTY_ID");
				String party_name = rs.getString("PARTY_NAME");
				pstmt.setString(1, party_id);
				pstmt.setString(2, party_name);
				pstmt.executeUpdate();
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		long endTime = System.nanoTime();
		long totalTime = endTime - startTime;
		double seconds = (double) totalTime / 1000000000.0;
		System.out.println("Time Elapsed " + totalTime + " nanoseconds/"
				+ seconds + " seconds");
		return "Success";
	}

	public String getData() throws JSONException {
		long startTime = System.nanoTime();
		try {
			String query = "SELECT * FROM PureServlet";
			rs = st.executeQuery(query);
			System.out.println("Records from Database");
			while (rs.next()) {
				JSONObject p = new Party();
				String party_id = rs.getString("PARTY_ID");
				String party_name = rs.getString("PARTY_NAME");
				p.put(party_id, party_name);
				partyArr.put(p);
				System.out.println("PARTY_ID: " + party_id + ", PARTY_NAME: "
						+ party_name);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		long endTime = System.nanoTime();
		long totalTime = endTime - startTime;
		double seconds = (double) totalTime / 1000000000.0;
		System.out.println("Time Elapsed " + totalTime + " nanoseconds/"
				+ seconds + " seconds");
		return partyArr.toString(3);
	}

	public String getDataById(String party_ID) throws JSONException {
		long startTime = System.nanoTime();
		try {
			String query = "SELECT * FROM PureServlet WHERE PARTY_ID = "
					+ party_ID;
			rs = st.executeQuery(query);
			System.out.println("Records from Database");
			while (rs.next()) {
				JSONObject p = new Party();
				String party_id = rs.getString("PARTY_ID");
				String party_name = rs.getString("PARTY_NAME");
				p.put(party_id, party_name);
				partyArr.put(p);
				System.out.println("PARTY_ID: " + party_id + ", PARTY_NAME: "
						+ party_name);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		long endTime = System.nanoTime();
		long totalTime = endTime - startTime;
		double seconds = (double) totalTime / 1000000000.0;
		System.out.println("Time Elapsed " + totalTime + " nanoseconds/"
				+ seconds + " seconds");
		return partyArr.toString(3);
	}

	public String getDataByLimit(String limit) throws JSONException {
		long startTime = System.nanoTime();
		try {
			String query = "SELECT * FROM PureServlet LIMIT " + limit;
			rs = st.executeQuery(query);
			System.out.println("Records from Database");
			while (rs.next()) {
				JSONObject p = new Party();
				String party_id = rs.getString("PARTY_ID");
				String party_name = rs.getString("PARTY_NAME");
				p.put(party_id, party_name);
				partyArr.put(p);
				System.out.println("PARTY_ID: " + party_id + ", PARTY_NAME: "
						+ party_name);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		long endTime = System.nanoTime();
		long totalTime = endTime - startTime;
		double seconds = (double) totalTime / 1000000000.0;
		System.out.println("Time Elapsed " + totalTime + " nanoseconds/"
				+ seconds + " seconds");
		return partyArr.toString(3);
	}

	public String getDataByName(String party_name) throws JSONException {
		long startTime = System.nanoTime();
		try {
			String query = "SELECT * FROM PureServlet";
			rs = st.executeQuery(query);
			System.out.println("Records from Database");
			while (rs.next()) {
				String p_name = rs.getString("PARTY_NAME");
				if (p_name.equals(party_name)) {
					JSONObject p = new Party();
					String party_id = rs.getString("PARTY_ID");
					p.put(party_id, party_name);
					partyArr.put(p);
					System.out.println("PARTY_ID: " + party_id
							+ ", PARTY_NAME: " + party_name);
					long endTime = System.nanoTime();
					long totalTime = endTime - startTime;
					double seconds = (double) totalTime / 1000000000.0;
					System.out.println("Time Elapsed " + totalTime
							+ " nanoseconds/" + seconds + " seconds");
					return partyArr.toString(3);
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		long endTime = System.nanoTime();
		long totalTime = endTime - startTime;
		double seconds = (double) totalTime / 1000000000.0;
		System.out.println("Time Elapsed " + totalTime + " nanoseconds/"
				+ seconds + " seconds");
		return partyArr.toString(3);
	}

	public String getDataByLike(String pattern, int match_type) throws JSONException {
		long startTime = System.nanoTime();
		String query = "SELECT * FROM PureServlet WHERE PARTY_NAME LIKE ?";
		int count = 0;
		String match = (match_type == POST_MATCH ? pattern + "%"
				: match_type == PRE_MATCH ? "%" + pattern : "%" + pattern + "%");
		PreparedStatement pstmt = null;
		try {
			pstmt = con.prepareStatement(query);
			pstmt.setString(1, match);
			rs = pstmt.executeQuery();
			System.out.println("Records from Database");
			while (rs.next()) {
					JSONObject p = new Party();
					String party_name = rs.getString("PARTY_NAME");
					String party_id = rs.getString("PARTY_ID");
					p.put(party_id, party_name);
					partyArr.put(p);
					System.out.println("PARTY_ID: " + party_id
							+ ", PARTY_NAME: " + party_name);
					count++;
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		long endTime = System.nanoTime();
		long totalTime = endTime - startTime;
		double seconds = (double) totalTime / 1000000000.0;
		System.out.println(count + "\n" + "Time Elapsed " + totalTime + " nanoseconds/"
				+ seconds + " seconds");
		return partyArr.toString(3);
	}

	public String getDataByRegExp(String pattern) throws JSONException {
		long startTime = System.nanoTime();
		String query = "SELECT * FROM PureServlet WHERE PARTY_NAME REGEXP ?";
		int count = 0;
		PreparedStatement pstmt = null;
		try {
			pstmt = con.prepareStatement(query);
			pstmt.setString(1, pattern);
			rs = pstmt.executeQuery();
			System.out.println("Records from Database");
			while (rs.next()) {
					JSONObject p = new Party();
					String party_name = rs.getString("PARTY_NAME");
					String party_id = rs.getString("PARTY_ID");
					p.put(party_id, party_name);
					partyArr.put(p);
					System.out.println("PARTY_ID: " + party_id
							+ ", PARTY_NAME: " + party_name);
					count++;
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		long endTime = System.nanoTime();
		long totalTime = endTime - startTime;
		double seconds = (double) totalTime / 1000000000.0;
		System.out.println(count + "\n" + "Time Elapsed " + totalTime + " nanoseconds/"
				+ seconds + " seconds");
		return partyArr.toString(3);
	}

	public String getDataByOrderBy(int sort_type, String col) throws JSONException {
		long startTime = System.nanoTime();
		String sort = (sort_type == DEFAULT_SORT ? "" : sort_type == ASC ? "ASC" : "DESC");
		String query = "SELECT * FROM PureServlet ORDER BY " + col + " " + sort;
		int count = 0;
		PreparedStatement pstmt = null;
		try {
			pstmt = con.prepareStatement(query);
			rs = pstmt.executeQuery();
			System.out.println("Records from Database");
			while (rs.next()) {
					JSONObject p = new Party();
					String party_name = rs.getString("PARTY_NAME");
					String party_id = rs.getString("PARTY_ID");
					p.put(party_id, party_name);
					partyArr.put(p);
					System.out.println("PARTY_ID: " + party_id
							+ ", PARTY_NAME: " + party_name);
					count++;
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		long endTime = System.nanoTime();
		long totalTime = endTime - startTime;
		double seconds = (double) totalTime / 1000000000.0;
		System.out.println(count + "\n" + "Time Elapsed " + totalTime + " nanoseconds/"
				+ seconds + " seconds");
		return partyArr.toString(3);
	}
}
