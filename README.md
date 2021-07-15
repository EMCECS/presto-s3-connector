# S3 Presto connector

Presto is a distributed SQL query engine for big data. Presto uses connectors to query storage from different storage sources. This repository contains the code for a connector (the S3 Presto connector) to query storage from many S3 compatibile object stores. In many ways, the S3 connector is similar to the hive connector, but is specific to S3 storage, and does not require configuration and access to a Hive metastore.

Amazon S3 or Amazon Simple Storage Service is a service offered by Amazon Web Services (AWS) that provides object storage through a web service interface. There are numerous vendors that supply S3 alternatives to AWS.  The products for these vendors comply with the S3 protocol.  Objects written to S3 can typically store any type of object, which allows for uses like storage for Internet applications, backup and recovery, disaster recovery, data archives, data lakes for analytics, and hybrid cloud storage.

See the [User Manual](https://prestodb.github.io/docs/current/) for Presto deployment instructions and end user documentation.

## Types of S3 Servers Evaluated

The S3 Presto connector has been evaluated with the following S3 compatible object storage servers

- Dell Technologies Elastic Cloud Storage [ECS](https://www.delltechnologies.com/en-us/storage/ecs/index.htm)
- Scality Cloudserver [Scality](https://www.scality.com/)
- Minio Object Storage [MinIO](https://min.io/)

## Requirements

To build and run the S3 Presto connector, you must meet the following requirements:

* Linux 
* To build: Java 11+ 64-bit. Both Oracle JDK and OpenJDK are supported (we build using Java 11 JDK but with Java 8 compatibility)
* To run: Java 8 Update 151 or higher (8u151+), 64-bit. Both Oracle JDK and OpenJDK are supported. 
* Gradle 6.5.1+ (for building)
* Python 2.7+ (for running with the launcher script)
* S3 Compatible Storage server mentioned above
* Pravega Schema Registry version 0.2.0 or higher is recommended

## Building S3 Presto connector

S3 Presto connector is a standard Gradle project. Simply run the following command from the project root directory:

    # [root@lrmk226 ~]# ./gradlew clean build

On the first build, Gradle will download all the dependencies from various locations of the internet and cache them in the local repository (`~/.gradle / caches `), which can take a considerable amount of time.  Subsequent builds will be faster.  

S3 Presto connector has a set of unit tests that can take a few minutes to run. You can run the tests using this command:

    # [root@lrmk226 ~]# ./gradlew test

S3 Presto connector has a more comprehensive set of integrations tests that are longer to run than the unit tests. You can run the integration tests using this command:

    # [root@lrmk226 ~]# ./gradlew test -Pintegration

The --info argument can be passed on the command line for more information during the integration test run.

## Installing Presto

If you haven't already done so, install the Presto server and default connectors on one or more Linux hosts. Instructions for downloading, installing and configuring Presto can be found here: https://prestodb.io/docs/current/installation/deployment.html.

When using the tar.gz Presto bundle downloaded from Maven Central, the Presto installation can be installed in any location. Determine a location with sufficient available storage space. Using the wget command, download the gzip'd tar file from Maven using the link defined in the PrestoDB deployment section, but similar to the steps below:
    
    # [root@lrmk226 ~]# pwd
    /root
    # [root@lrmk226 ~]# wget https://repo1.maven.org/maven2/com/facebook/presto/presto-server/0.248/presto-server-0.248.tar.gz
    # [root@lrmk226 ~]# tar xvzf presto-server-0.248.tar.gz
    # [root@lrmk226 ~]# export PRESTO_HOME=/root/presto-server-0.248
    
Make a directory for the Presto configuration files

    [root@lrmk226 ~]# mkdir $PRESTO_HOME/etc
    
Now follow the directions to create the necessary configuration files for configuring Presto found in the PrestoDB documentation.

Note that if you are also running with Java 11, you may have to add the following to your etc/jvm.config
-Djdk.attach.allowAttachSelf=true

## Installing and Configuring S3 Connector

The plugin file that gets created during the presto-connector build process is: ./build/distributions/s3-presto-connector--<VERSION>.tar.gz.  This file can be untar'd in the $PRESTO_ROOT/plugin directory of a valid Presto installation. Like all Presto connectors, the S3 Presto connector uses a properties files to point to the storage provider (e.g. S3 server IP and port).  Create a properties file similar to below, but replace the # characters with the appropriate IP address of the S3 storage server and the Pravega Schema Registry server of your configuration.

    [root@lrmk226 ~]# cd $PRESTO_HOME/plugin
    [root@lrmk226 ~]# ls *.gz
    s3-presto-connector-0.1.0.tar.gz
    [root@lrmk226 ~]# tar xvfz s3-presto-connector-0.1.0.tar.gz
    [root@lrmk226 ~]# cat $PRESTO_HOME/etc/catalog/s3.properties
    s3.s3SchemaFileLocationDir=etc/s3
    s3.s3Port=9020
    s3.s3UserKey=<S3USER>
    s3.s3UserSecretKey=<S3SECRETKEY>
    s3.s3Nodes=##.###.###.###,##.###.###.###

    s3.schemaRegistryServerIP=##.###.###.###
    s3.schemaRegistryPort=9092
    s3.schemaRegistryNamespace=s3-schemas

    s3.maxConnections=500
    s3.s3SocketTimeout=5000
    s3.s3ConnectionTimeout=5000
    s3.s3ClientExecutionTimeout=5000

More options for the S3 secret key will be available in a future release.

If you have deployed Presto on more than one host (coordinator and one or more workers), you must download/copy the S3 connector gzip tar file to each node, and create the configuration properties file on all hosts.

## Running Presto Server

As mentioned in the PrestoDB documentation, use the launcher tool to start the Presto server on each node.

## Running Presto in your IDE

After building Presto for the first time, you can load the project into your IDE and run the server in your IDE. We recommend using [IntelliJ IDEA](http://www.jetbrains.com/idea/). Because S3 Presto connector is a standard Gradle project, you can import it into your IDE. In IntelliJ, choose Import Project from the Quick Start box and point it to the root of the source tree.  IntelliJ will identify the *.gradle files and prompt you to confirm.

After opening the project in IntelliJ, double check that the Java SDK is properly configured for the project:

* Open the File menu and select Project Structure
* In the SDKs section, ensure that a Java 11+ JDK is selected (create one if none exist)
* In the Project section, ensure the Project language level is set to 8.0 as Presto makes use of several Java 8 language features

Use the following options to create a run configuration that runs the Presto server using the Pravega Presto connector:

* Main Class: 'com.facebook.presto.server.PrestoServer'
* VM Options: '-ea -XX:+UseG1GC -XX:G1HeapRegionSize=32M -XX:+UseGCOverheadLimit -XX:+ExplicitGCInvokesConcurrent -Xmx2G -Dconfig=etc/config.properties -Dcom.sun.xml.bind.v2.bytecode.ClassTailor.noOptimize=true -Dlog.levels-file=etc/log.properties'
* Working directory: '$MODULE_DIR$'
* Use classpath of module: 'presto-s3-connector.main'
* Add a 'Before Launch' task - Add a gradle task to run the 'jar' task for the 'presto-s3-connector' Gradle project.

Please note that some versions of Intellij do not display VM Options by default.  For this, enable them with 'Modify options'

Modify the s3.properties file in etc/catalog as previously described to point to a running S3 storage server, and a running Schema Registry.

## Schema Definitions

Optionally, you may manually create schema definitions using a JSON file. The 'CREATE TABLE' and 'CREAE TABLE AS' Presto commands are also available to create ad-hoc tables.  The JSON configuration files are read at server startup, and should be located in etc/s3 directory.  

In the JSON schema example below, "testdb" is the Presto schema in the S3 catalog, addressTable is the name of the table.  In sources, testbucket is the name of the bucket, and testdb/addressTable is the S3 prefix location of the object data for the table.  The objectDataFormat, hasHeaderRow and recordDelimiter settings are self explanitory.

    {
      "schemas": [
        {
          "schemaTableName": {
            "schema_name": "testdb",
            "table_name": "addressTable"
          },
          "s3Table": {
            "name": "addressTable",
            "columns": [
              {
                "name": "Name",
                "type": "VARCHAR"
              },
              {
                "name": "Address",
                "type": "VARCHAR"
              }
            ],
            "sources": {"testbucket": ["testdb/addressTable"] },
            "objectDataFormat": "csv",
            "hasHeaderRow": "false",
            "recordDelimiter": "\n"
          }
        }
      ]
    }


## Tests
The pravega presto connector has 2 types of tests
* unit tests
  * all unit tests run during developer builds
* integration tests
    * by default only run on CI server
    * use '-Pintegration' flag to run:  ./gradlew test -Pintegration
    

