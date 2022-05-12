package edu.rit.ibd.a6;

import java.util.*;

import com.mongodb.client.FindIterable;
import com.mongodb.client.model.Filters;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class AprioriGenOpt {

	public static void main(String[] args) throws Exception {
		final String mongoDBURL = args[0];
		final String mongoDBName = args[1];
		final String mongoColLKMinusOne = args[2];
		final String mongoColLK = args[3];
		final int minSup = Integer.valueOf(args[4]);
		
		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);
		
		MongoCollection<Document> lKMinusOne = db.getCollection(mongoColLKMinusOne);
		MongoCollection<Document> lk = db.getCollection(mongoColLK);
		
		// TODO Your code here!
		
		/*
		 * 
		 * The documents include the transactions that contain them, so if a new document is added to CK, we can directly compute its transactions by performing the intersection. Having the actual
		 * 	transactions entails that we also know its number, so we can discard those that do not meet the minimum support. Items can be processed in ascending order.
		 * 
		 */
		lKMinusOne.createIndex(new Document("items", 1));
		int kMinus1 = findK(lKMinusOne);
		joiningStep(kMinus1, lKMinusOne, lk, minSup);
		lKMinusOne.dropIndex(new Document("items", 1));
		
		// You must figure out the value of k - 1.
		
		// You can implement this "by hand" using Java, an aggregation query, or a mix.
		
		// Remember that there is a single join step. The prune step is not used anymore.
		
		// Make sure the _ids of the documents are according to the lexicographical order of the items. You can start joining documents
		//	whose _ids are strictly greater than the current document. Also, the first time a pair of documents do not join, we can safely stop.
		
		// Both documents contain the arrays of transactions lexicographically sorted. The new document will have the intersecion of both sets
		//	of transactions.
		
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
	public static int findK(MongoCollection<Document> lKMinusOne) {

		Document doc = lKMinusOne.find().first();
		Document items = (Document) doc.get("items");
		return items.size();

	}

	public static void joiningStep(int kMinus1, MongoCollection<Document> lKMinusOne, MongoCollection<Document> ck, int minSup) {

		FindIterable<Document> p = lKMinusOne.find().batchSize(500);
		FindIterable<Document> q = lKMinusOne.find().batchSize(500);
		int _id = 0;
		for (Document Ppointer : p) {
			Document Pitems = (Document) Ppointer.get("items");
			for (Document Qpointer : q) {
				Document Qitems = (Document) Qpointer.get("items");
				List<Integer> listOfQ = new ArrayList<>();
				List<Integer> listOfP = new ArrayList<>();
				Set<Integer> pTransactions = new HashSet<>(Ppointer.getList("transactions",Integer.class));
				Set<Integer> qTransactions = new HashSet<>(Ppointer.getList("transactions",Integer.class));


				Set<Integer> set = new HashSet<>();
				for (int i = 0; i <= kMinus1; i++) {
					String pos = "pos_" + i;
					if (Pitems.containsKey(pos) && Qitems.containsKey(pos)) {
						listOfP.add(Pitems.getInteger(pos));
						listOfQ.add(Qitems.getInteger(pos));
					}


				}
				List<Integer> copyP = new ArrayList<>(listOfP);
				List<Integer> copyQ = new ArrayList<>(listOfQ);
				if (checkEligibility(copyP, copyQ)) {
					set.addAll(listOfP);
					set.addAll(listOfQ);

					List<Integer> finalSet = new ArrayList<>(set);
					Collections.sort(finalSet);
					pTransactions.retainAll(qTransactions);
					addLK(finalSet, ck, pTransactions, _id, minSup);
					finalSet.clear();
					_id++;
				}

				copyP.clear();
				copyQ.clear();

			}
		}
	}
	public static boolean checkEligibility(List<Integer> listOfP, List<Integer> listOfQ) {

		int pItem = listOfP.get(listOfP.size() - 1);
		int qItem = listOfQ.get(listOfQ.size() - 1);
		listOfP.remove(listOfP.size() - 1);
		listOfQ.remove(listOfQ.size() - 1);
		return listOfP.equals(listOfQ) && pItem < qItem;
	}
	public static void addLK(List<Integer> set, MongoCollection<Document> ck, Set<Integer> transactions, int _id, int minSup) {

		if (transactions.size() >= minSup){
			Document dMain = new Document();
			int i = 0;
			Document d = new Document();
			for (Integer ints : set) {
				String pos = "pos_" + i;
				i++;
				d.append(pos, ints);
			}
			dMain.append("_id",_id).append("count", 0).append("items", d);
			ck.insertOne(dMain);
			Document arrDoc = new Document();
			List<Integer> tempList = new ArrayList<>(transactions);
			Collections.sort(tempList);
			transactions.clear();
			for (Integer j :tempList)
				ck.updateOne(Filters.eq("_id",_id),arrDoc.append(
						"$push", new Document("transactions",j)
				));
			tempList.clear();
		}


	}

}
