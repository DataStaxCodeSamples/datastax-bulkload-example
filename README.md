Reference Data example
========================================================


## Running the demo 

To run this code, you need to have your cluster 'cassandra.yaml' and 'log4j-tools.properties' in the 'src/main/resources' directory.

You will need a java runtime (preferably 7) along with maven 3 to run this demo. Start DSE 4.0.X or a cassandra 2.0.X instance on your local machine. This demo just runs as a standalone process on the localhost.

This demo uses quite a lot of memory so it is worth setting the MAVEN_OPTS to run maven with more memory

    export MAVEN_OPTS=-Xmx512M


## Schema Setup
Note : This will drop the keyspace "datastax_bulkload_demo" and create a new one. All existing data will be lost. 

To specify contact points use the contactPoints command line parameter e.g. '-DcontactPoints=192.168.25.100,192.168.25.101'
The contact points can take mulitple points in the IP,IP,IP (no spaces).

To create the a single node cluster with replication factor of 1 for standard localhost setup, run the following

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.demo.SchemaSetup"

To run the bulk loader, this defaults to 1 million rows.

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.bulkloader.Main" 
    
To run it other settings for no of rows, jmx host and port

	mvn clean compile exec:java -Dexec.mainClass="com.datastax.bulkloader.Main"  -DnoOfRows=5000000 -Djmxhost=cassandra1 -Djmxport=7191    
	
To remove the tables and the schema, run the following.

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.demo.SchemaTeardown"
	
