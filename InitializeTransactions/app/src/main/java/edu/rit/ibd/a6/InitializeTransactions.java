package edu.rit.ibd.a6;

import java.sql.*;
import java.util.Arrays;
import java.util.List;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.PushOptions;
import com.mongodb.client.model.Updates;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import javax.print.Doc;

import static java.util.Arrays.asList;

public class InitializeTransactions {

	public static void main(String[] args) throws Exception {
		final String jdbcURL = args[0];
		final String jdbcUser = args[1];
		final String jdbcPwd = args[2];
		final String sqlQuery = args[3];
		final String mongoDBURL = args[4];
		final String mongoDBName = args[5];
		final String mongoCol = args[6];
		
		Connection con = DriverManager.getConnection(jdbcURL, jdbcUser, jdbcPwd);
		
		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);
		
		MongoCollection<Document> transactions = db.getCollection(mongoCol);
		
		// TODO Your code here!!!

		addTIDandUpdateIID(con, sqlQuery, transactions);

		
		// TODO End of your code!
		
		client.close();
		con.close();
	}

	private static MongoClient getClient(String mongoDBURL) {
		MongoClient client = null;
		if (mongoDBURL.equals("None"))
			client = new MongoClient();
		else
			client = new MongoClient(new MongoClientURI(mongoDBURL));
		return client;
	}


	public static void addTIDandUpdateIID(Connection con, String query,MongoCollection<Document> transactions) throws SQLException {
		PreparedStatement st = con.prepareStatement(query);
		st.setFetchSize(/* Batch size */ 500);
		ResultSet rs = st.executeQuery();

		while (rs.next()){

			Document d = new Document();
			Document Darr = new Document();
			Integer tid = rs.getInt("tid");
			Document TDoc = transactions.find(Document.parse("{_id:"+tid+"}")).first();

			if (TDoc == null){
				d.append("_id",tid);
				transactions.insertOne(d);
			}

			Integer iid = rs.getInt("iid");
			transactions.updateMany(Filters.eq("_id",tid),
					Darr.append(
							"$push", new Document("items",iid)
					)
					);



		}
		rs.close();
		st.close();

	}


}
