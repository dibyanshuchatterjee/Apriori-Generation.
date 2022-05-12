package edu.rit.ibd.a6;

import com.mongodb.client.FindIterable;
import com.mongodb.client.model.Filters;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import java.io.File;
import java.util.*;

public class GenerateLOneOpt {


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
		
		// TODO Your code here!
		
		/*
		 * 
		 * Extract single items from the transactions. Only single items that are present in at least minSup transactions should survive.
		 * 
		 * Keep track of the transactions associated to each item using an array field named 'transactions'. Also, use _ids such that
		 * 	they reflect the lexicographical order in which documents are processed.
		 * 
		 */
		transactions.createIndex(new Document("items",1));
		Map<Integer, Integer> itemMap = new HashMap<>(extractItem(transactions, minSup));
		Map<Integer, List<Integer>> tIDMap = new HashMap<>(getTransactions(itemMap, transactions));

		populateL1(itemMap,tIDMap,l1);
		transactions.dropIndex(new Document("items",1));

		// TODO End of your code!
		
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

	public static Map<Integer, Integer> extractItem(MongoCollection<Document> transactions, int minSup){

		Map<Integer, Integer> map = new HashMap<>();
		FindIterable<Document> itr = transactions.find().batchSize(500);
		for (Document trDoc : itr){
			List<Integer> itemList = new ArrayList<>(trDoc.getList("items", Integer.class));
			for (Integer item : itemList){
				if (!map.containsKey(item)){
					int count = checkCount(itr,item);
					if (count >= minSup)
						map.put(item,count);
				}
			}

		}

		return map;
	}

	public static int checkCount(FindIterable<Document> itr, int item){
		int count = 0;
		for (Document d : itr){
			List<Integer> toCheck = new ArrayList<>(d.getList("items",Integer.class));
			if (toCheck.contains(item))
				count++;
		}



		return count;
	}

	public static Map<Integer,List<Integer>> getTransactions(Map<Integer, Integer> itemMap,MongoCollection<Document> transactions){
		FindIterable<Document> TDoc = transactions.find().batchSize(500);
		Map<Integer,List<Integer>> tIDMap = new HashMap<>();
		for (Integer item : itemMap.keySet()){
			List<Integer> list = new ArrayList<>();
			for (Document d : TDoc){
				List<Integer> toCheck = new ArrayList<>(d.getList("items",Integer.class));
				if (toCheck.contains(item)){
					list.add(d.getInteger("_id"));
				}
			}
			tIDMap.put(item,list);
		}

		return tIDMap;
	}

	public static void populateL1(Map<Integer, Integer> itemMap,Map<Integer, List<Integer>> tIDMap,MongoCollection<Document> l1){
		List<Integer> temp = new ArrayList<>(itemMap.keySet());
		Collections.sort(temp);
		int _id = 0;
		for (Integer i : temp){
			Document d = new Document();
			Document arrDoc = new Document();
			d.append("_id",_id).append("count",itemMap.get(i)).append("items",  new Document("pos_0",i));
			l1.insertOne(d);
			for (Integer j : tIDMap.get(i))
			l1.updateOne(Filters.eq("_id",_id),arrDoc.append(
					"$push", new Document("transactions",j)
			));
			_id++;

		}

	}

}
