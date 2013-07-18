package com.cisco.xrm.party;

import java.io.File;
import java.io.IOException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

//import redis.clients.jedis.Jedis;

//import com.google.gson.Gson;
//import com.google.gson.GsonBuilder;

import jxl.Sheet;
import jxl.Workbook;
import jxl.read.biff.BiffException;

@Path("/party")
public class PartyCollection {
	private JSONArray partyArr = new JSONArray();
	DBConnector dbc = new DBConnector();
	private String inputFile = "C:/DFT/LDE/xRM_Party_Demo/src/com/cisco/xrm/party/sample/Sample.xls";

	@GET
	@Produces({ MediaType.TEXT_HTML, MediaType.APPLICATION_JSON,
			MediaType.TEXT_PLAIN })
	public String dbParty() throws JSONException {
		return dbc.getData();
	}

	@GET
	@Path("/ids/{party_ID}")
	@Produces({ MediaType.TEXT_HTML, MediaType.APPLICATION_JSON,
			MediaType.TEXT_PLAIN })
	public String dbPartyById(@PathParam("party_ID") final String party_ID)
			throws JSONException {
		return dbc.getDataById(party_ID);
	}

	@GET
	@Path("/limit/{limit}")
	@Produces({ MediaType.TEXT_HTML, MediaType.APPLICATION_JSON,
			MediaType.TEXT_PLAIN })
	public String dbPartyByLimit(@PathParam("limit") final String limit)
			throws JSONException {
		return dbc.getDataByLimit(limit);
	}

	@GET
	@Path("/names/{party_name}")
	@Produces({ MediaType.TEXT_HTML, MediaType.APPLICATION_JSON,
			MediaType.TEXT_PLAIN })
	public String dbPartyByName(@PathParam("party_name") final String party_name)
			throws JSONException {
		return dbc.getDataByName(party_name);
	}

	@GET
	@Path("/copy")
	@Produces({ MediaType.TEXT_HTML, MediaType.APPLICATION_JSON,
			MediaType.TEXT_PLAIN })
	public String copyDB()
			throws JSONException {
		return dbc.copyDB();
	}

	/**
	 * 
	 * @param pattern
	 * @param match
	 *            0 - Post; 1 - Pre; 2 - Full
	 * @return Data retrieved from database
	 * @throws JSONException
	 */
	@GET
	@Path("/names/like/{match}/{pattern}")
	@Produces({ MediaType.TEXT_HTML, MediaType.APPLICATION_JSON,
			MediaType.TEXT_PLAIN })
	public String dbPartyByLike(@PathParam("pattern") final String pattern,
			@PathParam("match") final int match) throws JSONException {
		return dbc.getDataByLike(pattern, match);
	}

	@GET
	@Path("/names/regexp/{pattern}")
	@Produces({ MediaType.TEXT_HTML, MediaType.APPLICATION_JSON,
			MediaType.TEXT_PLAIN })
	public String dbPartyByRegExp(@PathParam("pattern") final String pattern)
			throws JSONException {
		return dbc.getDataByRegExp(pattern);
	}

	/**
	 * 
	 * @param sort
	 * 			0 - Default; 1 - ASC; 2 - DESC
	 * @param col column name
	 * @return Data retrieved from database
	 * @throws JSONException
	 */
	@GET
	@Path("/names/orderby/{sort}/{col}")
	@Produces({ MediaType.TEXT_HTML, MediaType.APPLICATION_JSON,
			MediaType.TEXT_PLAIN })
	public String dbPartyByOrderBy(@PathParam("sort") final int sort,
			@PathParam("col") final String col) throws JSONException {
		return dbc.getDataByOrderBy(sort, col);
	}

	@GET
	@Path("/excel")
	@Produces({ MediaType.TEXT_HTML, MediaType.APPLICATION_JSON,
			MediaType.TEXT_PLAIN })
	public String excelRead() {
		long startTime = System.nanoTime();
		// Jedis jedis = new Jedis("localhost", 8080);
		// jedis.connect();

		File inputWkbk = new File(inputFile);
		Workbook w = null;
		try {
			w = Workbook.getWorkbook(inputWkbk);
		} catch (BiffException | IOException e) {
			e.printStackTrace();
		}
		Sheet sheet = w.getSheet(0);
		for (int i = 1; i < sheet.getRows(); i++) {
			JSONObject p = new Party();
			try {
				for (int j = 0; j < sheet.getColumns(); j++) {
					// jedis.set(sheet.getCell(j, 0).getContents(), sheet
					// .getCell(j, i).getContents());
					if (j == 5) {
						// System.out.println(jedis.get(sheet.getCell(j,
						// 0).getContents()));
					}
					p.put(sheet.getCell(j, 0).getContents(), sheet
							.getCell(j, i).getContents());
				}
				partyArr.put(p);
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}
		System.out.println(partyArr.length());

		String json = "";
		/*
		 * Gson gson = new GsonBuilder().setPrettyPrinting().create(); try { for
		 * (int i = 0; i < partyArr.length(); i++) { json +=
		 * partyArr.getJSONObject(i).toString(3); } } catch (JSONException e) {
		 * // TODO Auto-generated catch block e.printStackTrace(); } //json +=
		 * "]"; json = json.replace("\\", "");
		 */

		try {
			json = partyArr.toString(3);
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		long endTime = System.nanoTime();
		long totalTime = endTime - startTime;
		double seconds = (double) totalTime / 1000000000.0;
		System.out.println("Time Elapsed " + totalTime + " nanoseconds/"
				+ seconds + " seconds");

		return json;
	}

	/*
	 * @GET
	 * 
	 * @Produces(MediaType.TEXT_HTML) public JSONObject getPartyObject(int
	 * index) throws JSONException { return partyArr.getJSONObject(index); }
	 * 
	 * @GET
	 * 
	 * @Produces(MediaType.TEXT_HTML) public String
	 * getPartyName(@PathParam("index") final int index) throws JSONException {
	 * return partyArr.getJSONObject(index).get("PARTY_NAME").toString(); }
	 */
}
