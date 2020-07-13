package com.loader;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.policy.Priority;
import com.aerospike.client.policy.ScanPolicy;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

// # Under development
public class DataLoaderApp implements ScanCallback {

	// Scylla Connection
	static String[] contactPoints = { "172.17.0.2", "172.17.0.3", "172.17.0.4" };
	static String keyspace = "catalog";
	static String table = "superheroes";
	static PoolingOptions poolingOptions = new PoolingOptions();
	static Cluster cluster = Cluster.builder().addContactPoints(contactPoints).withPoolingOptions(poolingOptions)
			.build();
	static Session session = cluster.connect();

	public static void main(String[] args) {

		try {
			DataLoaderApp app = new DataLoaderApp();
			app.runLoader();
		} catch (Exception e) {
			e.printStackTrace();
			session.close();
			cluster.close();
		}
		System.out.println("Ended !!!");
	}

	public void runLoader() throws AerospikeException {
		AerospikeClient client = new AerospikeClient("172.17.0.5", 3000);

		try {
			ScanPolicy scanPolicy = new ScanPolicy();
			// Queries nodes in parallel if set true or will query one by one.
			scanPolicy.concurrentNodes = true;
			scanPolicy.includeBinData = true; // true will return the bin data as well
			scanPolicy.priority = Priority.LOW;

			// # the scan returns records to the user-defined call back methods.
			// # In this case it is returning to scanCallback method.
			client.scanAll(scanPolicy, "test", "myset", this);
		} finally {
			client.close();
		}

	}

	@Override
	public void scanCallback(Key key, Record record) throws AerospikeException {

		// Code to handle empty or null records to be added here. 
		System.out.println(
				record.getLong("id") + " " + record.getString("first_name") + " " + record.getString("last_name"));

		PreparedStatement preparedInsert = session.prepare("INSERT INTO " + keyspace + "." + table
				+ " (id,first_name,last_name) " + "VALUES (:id, :first_name, :last_name)");

		BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED).add(preparedInsert
				.bind((int)record.getLong("id"), record.getString("first_name"), record.getString("last_name")));
		session.execute(batch);
	}
}
