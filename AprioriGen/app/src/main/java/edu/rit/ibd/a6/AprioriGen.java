package edu.rit.ibd.a6;

import com.google.common.collect.Sets;
import com.mongodb.*;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Field;
import com.mongodb.client.model.Filters;
import org.bson.Document;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.conversions.Bson;

import java.util.*;
import java.awt.*;
import java.util.List;

import static com.mongodb.client.model.Projections.*;


public class AprioriGen {

    public static void main(String[] args) throws Exception {
        final String mongoDBURL = args[0];
        final String mongoDBName = args[1];
        final String mongoColLKMinusOne = args[2];
        final String mongoColCK = args[3];

        MongoClient client = getClient(mongoDBURL);
        MongoDatabase db = client.getDatabase(mongoDBName);

        MongoCollection<Document> lKMinusOne = db.getCollection(mongoColLKMinusOne);
        MongoCollection<Document> ck = db.getCollection(mongoColCK);

        lKMinusOne.createIndex(new Document("items", 1));

        int kMinus1 = findK(lKMinusOne);

        joiningStep(kMinus1, lKMinusOne, ck);
        ck.createIndex(new Document("items", 1));
        if (kMinus1 > 1)
            prune(ck, lKMinusOne, kMinus1);
        lKMinusOne.dropIndex(new Document("items", 1));
        ck.dropIndex(new Document("items", 1));

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

    public static void joiningStep(int kMinus1, MongoCollection<Document> lKMinusOne, MongoCollection<Document> ck) {

        FindIterable<Document> p = lKMinusOne.find().batchSize(200);
        FindIterable<Document> q = lKMinusOne.find().batchSize(200);
        for (Document Ppointer : p) {
            Document Pitems = (Document) Ppointer.get("items");
            for (Document Qpointer : q) {
                Document Qitems = (Document) Qpointer.get("items");
                List<Integer> listOfQ = new ArrayList<>();
                List<Integer> listOfP = new ArrayList<>();

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
                    addCK(finalSet, ck);
                    finalSet.clear();
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

    public static void addCK(List<Integer> set, MongoCollection<Document> ck) {

        Document dMain = new Document();
        int i = 0;
        Document d = new Document();
        for (Integer ints : set) {
            String pos = "pos_" + i;
            i++;
            d.append(pos, ints);
        }
        dMain.append("count", 0).append("items", d);
        ck.insertOne(dMain);

    }

    public static void prune(MongoCollection<Document> ck, MongoCollection<Document> lKMinusOne, int kMinus1) {
        FindIterable<Document> iterable = ck.find().batchSize(200);
        for (Document ckDoc : iterable) {
            Set<Integer> CkposSet = new HashSet<>();
            Document items = (Document) ckDoc.get("items");
            for (int i = 0; i <= kMinus1; i++) {
                String pos = "pos_" + i;
                CkposSet.add(items.getInteger(pos));
            }
            boolean flag = false;
            for (Set<Integer> comb : Sets.combinations(CkposSet, kMinus1)) {
                FindIterable<Document> lkDocs = lKMinusOne.find().batchSize(200);
                for (Document lkDoc : lkDocs) {
                    Set<Integer> lkposSet = new HashSet<>();
                    Document Lkitems = (Document) lkDoc.get("items");
                    for (int i = 0; i <= kMinus1; i++) {
                        String pos = "pos_" + i;
                        lkposSet.add(Lkitems.getInteger(pos));
                    }
                    if (lkposSet.containsAll(comb)) {
                        flag = true;
                        break;
                    } else flag = false;
                }
                if (!flag) {
                    ck.deleteOne(ckDoc);
                    break;
                }
            }

        }

    }
}
