package edu.rit.ibd.a6;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.DeleteResult;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import javax.print.Doc;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GenerateLOne {

	public static void main(String[] args) throws Exception {
		final String mongoDBURL = args[0];
		final String mongoDBName = args[1];
		final String mongoColTrans = args[2];
		final String mongoColL1 = args[3];
		final int minSup = Integer.valueOf(args[4]);
		
		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);
		
		MongoCollection<Document> transactions = db.getCollection(mongoColTrans);
		MongoCollection<Document> l1 = db.getCollection(mongoColL1);
		transactions.createIndex(new Document("items",1));

		Map<Integer, Integer> result = new HashMap<>(extractIIDs(transactions));

		Map<Integer, Integer> resultforCount = new HashMap<>(increaseCount(result, transactions));
		result.clear();

		Map<Integer, Integer> resultforRemoval = new HashMap<>(removeFromMap(resultforCount, minSup));
		resultforCount.clear();

		populateL1(resultforRemoval, l1);
		resultforRemoval.clear();
		transactions.dropIndex(new Document("items",1));

		
		client.close();
	}
	
	private static MongoClient getClient(String mongoDBURL) {
		MongoClient client = null;
		if (mongoDBURL.equals("None"))
			client = new MongoClient();
		else
			client = new MongoClient(new MongoClientURI(mongoDBURL));
		return client;
	}
	public static Map<Integer, Integer> extractIIDs(MongoCollection<Document> transactions){

		FindIterable<Document> iterable = transactions.find().batchSize(200);
		Map<Integer, Integer> result = new HashMap<>();
		for (Document d : iterable){
			List<Integer> items = new ArrayList<>(d.getList("items",Integer.class));
			for (Integer integer : items)
				result.put(integer,0);
		}
		return result;
	}


	public static void populateL1(Map<Integer, Integer> result, MongoCollection<Document> l1){

		for (Map.Entry<Integer, Integer> entry : result.entrySet()){
			Document dForL1 = new Document();
			dForL1.append("count",entry.getValue()).append("items",new Document("pos_0",entry.getKey()));
			l1.insertOne(dForL1);
		}
	}
	public static Map<Integer, Integer> increaseCount(Map<Integer, Integer> result, MongoCollection<Document> transactions){
		FindIterable<Document> iterable = transactions.find().batchSize(200);
		for (Document document : iterable){
			List<Integer> items = new ArrayList<>(document.getList("items",Integer.class));
			for (Integer itr : items){
				result.put(itr,result.get(itr) + 1);
			}
		}
		return result;
	}

	public static Map<Integer, Integer> removeFromMap(Map<Integer, Integer> result, int minSup){

		Map<Integer, Integer> copy = new HashMap<>(result);
		for (Map.Entry<Integer, Integer> entry : copy.entrySet()){
			if (entry.getValue() < minSup){
				result.remove(entry.getKey());
			}

		}

		return result;
	}

}
