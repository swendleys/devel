package souk.maventeset;

import java.util.ArrayList;
import java.math.RoundingMode;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.math.IntMath;
import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.Cursor;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.util.JSON;
import com.google.common.collect.Lists;
import com.mongodb.AggregationOutput;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

public class ThreadTest1 extends Thread {

	AggregationOutput output;
	MongoClient mongoClient = new MongoClient(new ServerAddress("localhost", 27017));
	DB db = mongoClient.getDB("project");
	DBCollection coll = db.getCollection("positions");
	double dX;
	double dY;
	double SomN;
	double SomE;
	double Latitude;
	double Longitude;
	double x2;
	double y2;
	ArrayList<String> UnitIds = new ArrayList<String>();
	ArrayList<Double> topSpeed = new ArrayList<Double>();
	DBCollection collUnits = db.getCollection("collUnits");
	DBCollection collAan = db.getCollection("collAan");
	ArrayList<String> firstHalf = new ArrayList<>();
	ArrayList<String> secondHalf = new ArrayList<>();
	ArrayList<String> thirdHalf = new ArrayList<>();
	ArrayList<String> fourthHalf = new ArrayList<>();
	
	public void convert() {

		dX = (double) ((x2 - 155000) * Math.pow(10, -5));
		dY = (double) ((y2 - 463000) * Math.pow(10, -5));

		SomN = (3235.65389 * dY) + (-32.58297 * Math.pow(dX, 2)) + (-0.2475 * Math.pow(dY, 2))
				+ (-0.84978 * Math.pow(dX, 2) * dY) + (-0.0655 * Math.pow(dY, 3))
				+ (-0.01709 * Math.pow(dX, 2) * Math.pow(dY, 2)) + (-0.00738 * dX) + (0.0053 * Math.pow(dX, 4))
				+ (-0.00039 * Math.pow(dX, 2) * Math.pow(dY, 3)) + (0.00033 * Math.pow(dX, 4) * dY)
				+ (-0.00012 * dX * dY);
		SomE = (5260.52916 * dX) + (105.94684 * dX * dY) + (2.45656 * dX * Math.pow(dY, 2))
				+ (-0.81885 * Math.pow(dX, 3)) + (0.05594 * dX * Math.pow(dY, 3)) + (-0.05607 * Math.pow(dX, 3) * dY)
				+ (0.01199 * dY) + (-0.00256 * Math.pow(dX, 3) * Math.pow(dY, 2)) + (0.00128 * dX * Math.pow(dY, 4))
				+ (0.00022 * Math.pow(dY, 2)) + (-0.00022 * Math.pow(dX, 2)) + (0.00026 * Math.pow(dX, 4));

		Latitude = 52.15517 + (SomN / 3600);
		Longitude = 5.387206 + (SomE / 3600);
	}

	public static double distance(double lat1, double lon1, double lat2, double lon2, String unit) {

		double theta = lon1 - lon2; // of omgekeerd
		double dist = Math.sin(deg2rad(lat1)) * Math.sin(deg2rad(lat2))
				+ Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.cos(deg2rad(theta));
		dist = Math.acos(dist);
		dist = rad2deg(dist);
		dist = dist * 60 * 1.1515;
		if (unit == "K") {
			dist = dist * 1.609344;
		} else if (unit == "N") {
			dist = dist * 0.8684;
		}
		return (dist);
	}

	private static double deg2rad(double deg) {
		return (deg * Math.PI / 180.0);
	}

	private static double rad2deg(double rad) {
		return (rad * 180 / Math.PI);
	}
	
	public void HalfUnitId() {
		DBObject group = new BasicDBObject("$group",
				new BasicDBObject("_id", "$UnitId").append("count", new BasicDBObject("$sum", 1)));

		output = coll.aggregate(group);
		for (DBObject result : output.results()) {
			UnitIds.add(result.get("_id").toString());
			int partitionSize = IntMath.divide(UnitIds.size(), 2, RoundingMode.UP);

			List<String> sub = UnitIds.subList(0, partitionSize);
			List<String> one = new ArrayList<String>(sub);
			//firstHalf = (ArrayList<String>) one;
			
			int partitionSize1 = IntMath.divide(one.size(), 2, RoundingMode.UP);
			List<String> sub1 = one.subList(0, partitionSize1);
			List<String> first = new ArrayList<String>(sub1);
			firstHalf = (ArrayList<String>) first;
			//System.out.println(firstHalf);
			
			List<String> sub2 = one.subList(partitionSize1, sub.size() );
			List<String> second = new ArrayList<String>(sub2);
			secondHalf = (ArrayList<String>) second;
			//System.out.println(secondHalf);
			

			List<String> secondSub = UnitIds.subList(UnitIds.size() - partitionSize + 1, UnitIds.size());
			List<String> two = new ArrayList<String>(secondSub);
			//secondHalf = (ArrayList<String>) two;
			
			
			int partitionSize2 = IntMath.divide(two.size(), 2, RoundingMode.UP);
			List<String> sub3 = two.subList(0, partitionSize2);
			List<String> third = new ArrayList<String>(sub3);
			thirdHalf = (ArrayList<String>) third;
			//System.out.println(thirdHalf);
			
			List<String> sub4 = two.subList(partitionSize2,secondSub.size() );
			List<String> fourth = new ArrayList<String>(sub4);
			fourthHalf = (ArrayList<String>) fourth;
			//System.out.println(fourthHalf);
			
			//collUnits.drop();
		}

	}
	
	Thread thread2 = new Thread() {
		public void run() {
			//collUnits.drop();
			//System.out.println("Time start:" + System.currentTimeMillis());
			long start = System.currentTimeMillis();
			DBCollection coll1 = db.getCollection("positions");
			ArrayList<String> arraylist = new ArrayList<String>();
			Set<String> hs = new HashSet<>();
			
			for (int h = 0; h<secondHalf.size();h++) {
				//System.out.println(firstHalf.get(h));
				BasicDBObject whereQuery = new BasicDBObject();
				whereQuery.put("UnitId", secondHalf.get(h));
				DBCursor cursor = coll1.find(whereQuery);
				
				while (cursor.hasNext()) {
					DateTimeFormatter formatter1 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
					String string1 = cursor.next().get("DateTime").toString();
					LocalDate localdate11 = LocalDate.parse(string1, formatter1);
					String lstring11 = localdate11.toString();
					arraylist.add(lstring11.toString());
				}
				hs.addAll(arraylist);
				arraylist.clear();
				arraylist.addAll(hs);
				Collections.sort(arraylist);
				//System.out.println("Dagen zijn in de ArrayList");	
				double geredenTotaal = 0;
				for (int m = 0; m < arraylist.size(); m++) {
					BasicDBObject andQuery = new BasicDBObject();
					List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
					obj.add(new BasicDBObject("UnitId", secondHalf.get(h)));
					obj.add(new BasicDBObject("DateTime", java.util.regex.Pattern.compile(arraylist.get(m))));
					andQuery.put("$and", obj);
					DBCursor cursor1 = coll1.find(andQuery);
					int i = 0;
					double gereden = 0;
					double l1 = 0;
					double l2 = 0;
					double l3 = 0;
					double l4 = 0;
					while (cursor1.hasNext()) {

						BasicDBObject obj1 = (BasicDBObject) cursor1.next();
						ArrayList<String> x = new ArrayList<String>();
						ArrayList<String> y = new ArrayList<String>();
						x.add(obj1.getString("Rdx"));
						y.add(obj1.getString("Rdy"));
						String y1 = y.get(0);
						String x1 = x.get(0);

						x2 = Double.parseDouble(x1);
						y2 = Double.parseDouble(y1);
						convert();
						i++;
						if (i == 3) {
							i = 1;
						}
						if (i == 1) {
							l1 = Latitude;
							l2 = Longitude;
						} else if (i == 2) {
							l3 = Latitude;
							l4 = Longitude;
						}
						if (distance(l1, l2, l3, l4, "K") < 100) {
							gereden = gereden + distance(l1, l2, l3, l4, "K");
							geredenTotaal = geredenTotaal + distance(l1, l2, l3, l4, "K");

						}

					}

					// gson - export naar mongodb
					String json = "{\"UnitId\": " + "\"" + secondHalf.get(h) + "\"" + ",\"Dag\": " + "\"" + arraylist.get(m) + "\""
							+ ", \"KM\": " + "\"" + Math.round(gereden) + "\"}";

					DBObject dbObject = (DBObject) JSON.parse(json);
					collUnits.insert(dbObject);

					 System.out.println("{\"UnitId\": " + "\"" + secondHalf.get(h)
					 + "\"" + ",\"Dag\": " + "\"" + arraylist.get(m) + "\""
					 + ", \"KM\": " + "\"" + Math.round(gereden) + "\"}");

				}
				System.out.println(
						"UnitId: " + secondHalf.get(h) + ", Totaal KM vanaf " + arraylist.get(0) + ": " + Math.round(geredenTotaal));
				System.out.println(
						"UnitId: " + secondHalf.get(h) + ", Gemiddeld KM per dag: " + Math.round(geredenTotaal / arraylist.size()));

			}
			System.out.println("End" + System.currentTimeMillis());
			long end = System.currentTimeMillis();
			long diff = end - start;
			System.out.println("Diff: " + diff);
			}
	};
}
