package com.datastax.bulkloader;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.CompositeType.Builder;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.SSTableSimpleUnsortedWriter;

import com.datastax.sampledata.Transaction;

/**
 * To run this code, you need to have your cluster 'cassandra.yaml' and
 * 'log4j-tools.properties' in the 'src/main/resources' directory.
 * 
 */
public class BulkLoadTransactions {

	private DateFormat dateFormat = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss.SSS");
	private DateFormat monthYearFormat = new SimpleDateFormat("yyyyMM");

	private SSTableSimpleUnsortedWriter testWriter;
	private CompositeType compositeColumn;
	private File filePath;

	public BulkLoadTransactions() throws IOException {

		String keyspace = "datastax_bulkload_demo";
		String tableName = "transactions";
			
		createDirectories(keyspace, tableName);
		
		/*
		 * Create a composite column with the types for each part
		 * 
		 * Each column will have the types in order of the table creation
		 * command In this case we have an timestamp then a varchar(UTF8 is the
		 * underlying type) Use the type mapping here:
		 * http://www.datastax.com/docs/1.2/cql_cli/cql_data_types
		 */
		List<AbstractType<?>> compositeColumnValues = new ArrayList<AbstractType<?>>();
		compositeColumnValues.add(TimestampType.instance);
		compositeColumnValues.add(UTF8Type.instance);		
		compositeColumn = CompositeType.getInstance(compositeColumnValues);

		testWriter = new SSTableSimpleUnsortedWriter(filePath,
				new Murmur3Partitioner(), keyspace, tableName,
				compositeColumn, null, 128);
	}
	
	public File getFilePath(){
		return this.filePath;
	}

	private void createDirectories(String keyspace, String tableName) {
		File directory = new File(keyspace);
		if (!directory.exists())
			directory.mkdir();
		
		filePath = new File(directory, tableName);
		if (!filePath.exists())
			filePath.mkdir();
	}

	/**
	 * 
	 create table transactions ( accid text, txtntime timestamp, txtnid uuid,
	 * amount double, type text, reason text, PRIMARY KEY((accid), txtntime) );
	 */
	public void loadTransactions(List<Transaction> transactions)
			throws IOException {
		long timestamp = System.currentTimeMillis();

		Builder builder;

		for (Transaction transaction : transactions) {
			
			String yearMonth = monthYearFormat
					.format(transaction.getTxtnDate());

			String rowKey =transaction.getAcountId() + "-" + yearMonth;			
			testWriter.newRow(ByteBuffer.wrap(rowKey.getBytes()));

			// First column is a cql3 row marker
			builder = compositeColumn.builder();
			builder.add(bytes(transaction.getTxtnDate().getTime()));
			builder.add(bytes(""));
			testWriter.addColumn(builder.build(), bytes(""), timestamp);
			
			builder = compositeColumn.builder();
			builder.add(bytes(transaction.getTxtnDate().getTime()));
			builder.add(bytes("amount"));
			testWriter.addColumn(builder.build(), bytes(transaction.getAmount()), timestamp);

			builder = compositeColumn.builder();
			builder.add(bytes(transaction.getTxtnDate().getTime()));
			builder.add(bytes("reason"));
			testWriter.addColumn(builder.build(), bytes(transaction.getReason()), timestamp);

			builder = compositeColumn.builder();
			builder.add(bytes(transaction.getTxtnDate().getTime()));
			builder.add(bytes("txtnid"));
			testWriter.addColumn(builder.build(), bytes(UUID.fromString(transaction.getTxtnId())), timestamp);

			builder = compositeColumn.builder();
			builder.add(bytes(transaction.getTxtnDate().getTime()));
			builder.add(bytes("type"));
			testWriter.addColumn(builder.build(), bytes(transaction.getType()), timestamp);
		}
	}

	public void finish() throws IOException {
		testWriter.close();
	}
}