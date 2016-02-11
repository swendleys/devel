package souk.maventeset;

import java.util.ArrayList;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.util.JSON;

public class CityGis {

	AggregationOutput output;
	MongoClient mongoClient = new MongoClient(new ServerAddress("localhost", 27017));
	DB db = mongoClient.getDB("project");
	DBCollection coll = db.getCollection("positions");
	ArrayList<String> unit = new ArrayList<String>();
	DBCollection cgquality = db.getCollection("cgquality");
	ArrayList<String> quality = new ArrayList<String>();
	DBCollection cgqualityTotal = db.getCollection("cgqualityTotal");

	public void getData() {
		
		cgquality.drop();
		DBObject group = new BasicDBObject("$group",
				new BasicDBObject("_id", "$UnitId").append("count", new BasicDBObject("$sum", 1)));
		DBObject sort = new BasicDBObject("$sort", new BasicDBObject("count", -1));
		output = coll.aggregate(group, sort);

		for (DBObject result : output.results()) {
			// System.out.println(result);
			unit.add(result.get("_id").toString());
		}

		for (int i = 0; i < unit.size(); i++) {

			DBObject match = new BasicDBObject("$match", new BasicDBObject("UnitId", unit.get(i)));
			DBObject group1 = new BasicDBObject("$group",
					new BasicDBObject("_id", "$Quality").append("count", new BasicDBObject("$sum", 1)));
			DBObject sort1 = new BasicDBObject("$sort", new BasicDBObject("count", -1));
			output = coll.aggregate(match, group1, sort1);

			for (DBObject result : output.results()) {
				String json = "{\"UnitId\": " + "\"" + unit.get(i) + "\"" + ",\"Quality\": " + "\"" + result.get("_id") + "\""
						+ ", \"Aantal\": " + result.get("count") + "}";
				
				DBObject dbObject = (DBObject) JSON.parse(json);
				cgquality.insert(dbObject);
			}

		}

	}
	public void getTotalData() {
		cgqualityTotal.drop();
		
		DBObject group = new BasicDBObject("$group",
				new BasicDBObject("_id", "$Quality").append("count", new BasicDBObject("$sum", 1)));
		DBObject sort = new BasicDBObject("$sort", new BasicDBObject("count", -1));
		output = coll.aggregate(group, sort);

		for (DBObject result : output.results()) {
			String json = "{\"Quality\": " + "\"" + result.get("_id") + "\"" + ",\"Aantal\": " + "" + result.get("count")
			+ "}";
			System.out.println(json);
			DBObject dbObject = (DBObject) JSON.parse(json);
			cgqualityTotal.insert(dbObject);
		}

		

	}


}
