# PROUD:PaRallel OUtlier Detection for streams using Hazelcast Jet

This library is a port of the [PROUD](https://github.com/tatoliop/PROUD-PaRallel-OUtlier-Detection-for-streams)
platform for distance based outlier detection in streaming
environments in [Hazelcast Jet](https://jet-start.sh/).

## :speech_balloon: About

The PROUD platform was developed in Scala for [Apache Flink](https://flink.apache.org/)
by [Theodore Toliopoulos](https://github.com/tatoliop) as an
implementation of algorithms developed for the academic paper
"PROUD: PaRallel OUtlier Detection for Streams"[1] by Theodoros
Toliopoulos, Christos Bellas, Anastasios Gounaris, and Apostolos Papadopoulos.

PROUD offers a variety of algorithms for distance based outlier detection
that support queries in both single-query[2] (**S**) and multi-query[3] parameter
spaces. For the multi-query outlier detection query case there are two
distinct types. The first one is the multi-app level spatial parameters
that are the minimum number of neighbours K in a specific spatial range
R (**RK**).The second one is an extension that supports multiple window sizes W
and window slide sizes S (**RKWS**).

Moreover, PROUD also provides two different ways of partitioning incoming
data, namely grid-based and tree-based technique. The first one is only 
used for the euclidead space while the second one can be used for any 
metric space.

This version of PROUD was developed as part of a dissertation project in Java
and Hazelcast Jet by [Athanasios Kefalas](https://github.com/athankefalas). This
library is provided *AS IS* and it is **NOT** recommended that it is used
in a production setting or added in the process of any critical downstream systems.

## :compass: Contents

1. **[Installation](#floppy_disk-installation)**
2. **[General](#bulb-general)**
3. **[Configuration](#toolbox-configuration)**
   1. **[Configuration Builder](#configuration-builder)**
      1. *[Algorithm Selection](#1-algorithm-selection)*
      2. *[Outlier Query Type Selection](#2-outlier-query-type-selection)*
      3. *[Outlier Query Definition](#3-outlier-query-definition)*
      4. *[Dataset and Input Configuration](#4-dataset-and-input-configuration)*
      5. *[Partitioning Method Configuration](#5-partitioning-method-configuration)*
          1. [Replication Partitioning](#replication-partitioning)
          2. [Tree Partitioning](#tree-partitioning)
          3. [Grid Partitioning](#grid-partitioning)
          4. [User Defined Partitioning](#user-defined-partitioning)
      6. *[Output Configuration](#6-output-configuration)*
      7. *[Building the configuration](#7-building-the-configuration)*
   2. **[Command Line Arguments Configuration](#command-line-arguments-configuration)**
      1. *[Command Line Options](#command-line-options)*
      2. *[Environment Variables](#environment-variables)*
      3. *[Creating the Configuration](#creating-the-configuration)*
      4. *[Caveats](#caveats)*
4. **[PROUD Pipeline API](#dart-proud-pipeline-api)**
   1. **[Proud Source](#proud-source)**
      1. *[Auto Source](#auto-source)*
      2. *[File Source](#file-source)*
      3. *[Kafka Source](#kafka-source)*
      4. *[User Defined Sources](#user-defined-sources)*
   2. **[Partition Data](#partition-data)**
   3. **[Detect Outliers](#detect-outliers)**
   4. **[Proud Sink](#proud-sink)**
      1. *[Auto Sink](#auto-sink)*
      2. *[Logger Sink](#logger-sink)*
      3. *[InfluxDB Sink](#influxdb-sink)*
      4. *[User Defined Sinks](#user-defined-sinks)*
   5. **[Pipeline Downgrade](#pipeline-downgrade)**
   6. **[Jet Pipeline Upgrade](#jet-pipeline-upgrade)**
5. **[Extension Points](#jigsaw-extension-points)**
   1. **[User Defined Partitioning](#user-defined-partitioning-extension-points)**
   2. **[User Defined Outlier Detection](#user-defined-outlier-detection-extension-points)**
6. **[Execution](#joystick-execution)**
7. **[References](#link-references)**

## :floppy_disk: Installation

The library can be easily installed as a dependency using either gradle or maven.

To install PROUD using maven, add the following `dependency` declaration to dependencies section in your pom.xml file:
```xml
<dependency>
  <groupId>edu.auth</groupId>
  <artifactId>jet-proud</artifactId>
  <version>1.0</version>
</dependency>
```

Alternatively to install PROUD using gradle use the following:

```groovy
compile group: 'edu.auth', name: 'jet-proud', version: 1.0
```

**OR**

```groovy
compile "edu.auth:jet-proud:1.0"
```

## :bulb: General

The PROUD library is designed to mimic the general pipeline model used by Hazelcast Jet while retaining compatibility with
the native Jet pipeline API. A new PROUD pipeline can be created from scratch or by upgrading an existing native Jet `Pipeline`
whilst a PROUD Pipeline can be downgraded to a native Jet `Pipeline` at any point.

## :toolbox: Configuration

Before the PROUD pipeline can be used the PROUD library must be correctly configured. In order to
configure the library a client can use either a code based configuration or a command line argument based one.

### Configuration Builder

Code based configuration is possible by using a builder object. A new instance of the configuration builder can be created
with the following:

```java
import edu.auth.jetproud.proud.context.Proud;

Proud proud = Proud.builder()
```

From this basic builder instance the client is guided through configuring the context of the PROUD task. The builder 
compartmentalizes the configuration options to significantly minimize the chance of on invalid configuration which would result in 
a runtime error during the creation of the PROUD Pipeline. Some of the options available in the builder include the algorithm,
the outlier query type, the outlier query, input selection, partitioning method selection and output selection.

The guided configuration of the builder leverages the type system of Java to provide a safer way to configure PROUD. However,
when using user defined algorithms or partitioning methods the correctness of the configuration is reliant on the client as the
exact specifications of the user defined component are unknown. Additionally, the proper combination of Algorithm - Outlier Query Type
is also not checked by the builder at compile time in version 1.0 and the client must select the correct combination. For a list of supported
Algorithm - Outlier Query Type combination view the next section.

#### **1. Algorithm Selection**

This option sets the algorithm that will be used for detecting outliers in the stream and can be set by the following:

```java
import edu.auth.jetproud.proud.context.Proud;
import edu.auth.jetproud.application.parameters.data.ProudAlgorithmOption;

Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.Naive)
```

The available algorithms along with their compatible outlier query types are:

* Naive - **S**
* Advanced - **S**
* AdvancedExtended - **S**
* Slicing - **S**
* PMCod - **S**
* PMCodNet - **S**
* AMCod - **RK**
* Sop - **RK, RKWS**
* PSod - **RK, RKWS**
* PMCSky - **RK, RKWS**

While using a user defined algorithm is possible by selecting the option
 * UserDefined - **Unknown**

#### 2. Outlier Query Type Selection

This option selects the type of outlier detection query to be used by PROUD. There are three available outlier query types:

1. Single Space Query (**S**)
2. Multi Space Query (**RK**)
3. Multi Space Query with Multiple Windowing Parameters (**RKWS**)

The required query type can be set right after selecting the algorithm, like so:

```java
import edu.auth.jetproud.proud.context.Proud;
import edu.auth.jetproud.application.parameters.data.ProudAlgorithmOption;

// Single Space Outlier Query Type
Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.Naive)
                .inSingleSpace()

// Multi Space Outlier Query Type
Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.AMCod)
                .inMultiQuerySpace()
                
// Multi Space Query with Multiple Windowing Parameters Outlier Query Type
Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.Sop)
                .inMultiQueryMultiWindowSpace()

```

#### 3. Outlier Query Definition

After the required outlier query type is selected, the parameters for the query must be configured. The builder automatically provides a function
named `querying(...)` that can receive the appropriate parameters based on the query type. All query types can accept one or more of four parameters:
K - number of neighbours, R - distance range, W - window size in milliseconds and S - window slide size in milliseconds. Examples of the 
definition of query parameters for each query type can be seen below.

```java
import edu.auth.jetproud.proud.context.Proud;
import edu.auth.jetproud.application.parameters.data.ProudAlgorithmOption;

// Single Space Outlier Query Type
Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.Naive)
                .inSingleSpace()
                .querying(50, 0.45, 10000, 500) // querying(int kNeighbours, double range, int window, int slide)

// Multi Space Outlier Query Type
Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.AMCod)
                .inMultiQuerySpace()
                .querying(List.of(50), List.of(0.45), 10000, 500) // querying(List<Integer> kNeighboursDimensions, List<Double> rangeDimensions, int window, int slide)
                
// Multi Space Query with Multiple Windowing Parameters Outlier Query Type
Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.Sop)
                .inMultiQueryMultiWindowSpace()
                .querying(List.of(50), List.of(0.45), List.of(10000), List.of(500)) // querying(List<Integer> kNeighboursDimensions, List<Double> rangeDimensions, List<Integer> windowDimensions, List<Integer> slideDimensions

```

#### 4. Dataset and Input Configuration

The next step includes the configuration of the input dataset and the source to read the dataset items from. PROUD supports by default a
file input datasource, and a kafka input datasource which can be later automatically resolved to a Hazelcast Jet `Source`.

```java
import edu.auth.jetproud.proud.context.Proud;
import edu.auth.jetproud.application.parameters.data.ProudAlgorithmOption;

// Dataset - File Datasource
Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.Naive)
                .inSingleSpace()
                .querying(50, 0.45, 10000, 500)
                .forDatasetNamed("$DATASET_NAME")
                .locatedIn("$ABSOLUTE_PATH_TO_DATASET_FILE")

// Dataset - Kafka Datasource
Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.Naive)
                .inSingleSpace()
                .querying(50, 0.45, 10000, 500)
                .forDatasetNamed("$DATASET_NAME")
                .fromKafka()
                  .inTopic("$KAFKA_TOPIC")


```

#### 5. Partitioning Method Configuration

The configuration of the partitioning method to be used by PROUD can also be configured via the builder instance. The available partitioning methods
and the algorithms they are supported by, are the following:

* Replication - **Naive, Advanced**
* Value Based with a Tree - **All algorithms except from Naive & Advanced**
* Value Based with a Grid - **All algorithms except from Naive & Advanced**
* User Defined - **Unknown**

The options for each partitioning method can be seen in the following sections.

##### Replication Partitioning

Replication Partitioning replicates the stream items in all Jet partitions, while flagging the items
that truly belong in the partition with `0` and replicated items with `1`.

```java
import edu.auth.jetproud.proud.context.Proud;
import edu.auth.jetproud.application.parameters.data.ProudAlgorithmOption;

Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.Naive)
                .inSingleSpace()
                .querying(50, 0.45, 10000, 500)
                .forDatasetNamed("$DATASET_NAME")
                .fromKafka()
                  .inTopic("$KAFKA_TOPIC")
                .replicationPartitioned()


```

##### Tree Partitioning

Value based partitioning with Tree, uses a vantage point tree that queries the spatial coordinates of an item
and assign it to a primary partition, as well as replicates it to a number of neighbouring partitions. An item
that belongs to a specific partition is flagged with a `0` while replicated copies of the item in neighbouring
partitions are flagged with `1`. The vantage point tree needs to be initialized
with a sample of the dataset and must be available as a file.

Tree based partitioning can be configured by the following:

```java
import edu.auth.jetproud.proud.context.Proud;
import edu.auth.jetproud.application.parameters.data.ProudAlgorithmOption;

// Tree Partitioning
Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.PMCod)
                .inSingleSpace()
                .querying(50, 0.45, 10000, 500)
                .forDatasetNamed("$DATASET_NAME")
                .fromKafka()
                  .inTopic("$KAFKA_TOPIC")
                .treePartitionedUsing("$ABSOLUTE_PATH_TO_TREE_INIT_FILE")
        
// Tree Partitioning with a custom initial node count
Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.PMCod)
                .inSingleSpace()
                .querying(50, 0.45, 10000, 500)
                .forDatasetNamed("$DATASET_NAME")
                    .fromKafka()
                .inTopic("$KAFKA_TOPIC")
                .treePartitionedUsing("$ABSOLUTE_PATH_TO_TREE_INIT_FILE", 1205)


```

##### Grid Partitioning

Value based partitioning with Grid, uses a grid cell structure and the spatial coordinates of an item
in order to assign it to a primary partition, as well as replicate it to a number of neighbouring partitions. An item
that belongs to a specific partition is flagged with a `0` while replicated copies of the item in neighbouring
partitions are flagged with `1`. The grid based partitioning, requires a user defined function that
partitions the items by taking advantage of foreknowledge of the general spatial boundaries of the dataset. The `GridPartitioning.GridPartitioner` is an interface that
encapsulates this functionality, and it is a required parameter when using grid base partitioning.

For the Stocks and TAO datasets a grid partitioner is implemented by default and an instance can
be created by invoking `DefaultGridPartitioners.forDatasetNamed("STK")`. While the configuration of 
grid partitioning can be seen below. 

```java
import edu.auth.jetproud.proud.context.Proud;
import edu.auth.jetproud.application.parameters.data.ProudAlgorithmOption;

Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.PMCod)
                .inSingleSpace()
                .querying(50, 0.45, 10000, 500)
                .forDatasetNamed("$DATASET_NAME")
                .fromKafka()
                  .inTopic("$KAFKA_TOPIC")
                .gridPartitionedUsing(new GridPartitioning.GridPartitioner(){...})


```

##### User Defined Partitioning

PROUD allows for the definition and use of a user defined partitioning method when using the PROUD Pipeline. The feature
however is opt-in and must be explicitly configured.

```java
import edu.auth.jetproud.proud.context.Proud;
import edu.auth.jetproud.application.parameters.data.ProudAlgorithmOption;

Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.Naive)
                .inSingleSpace()
                .querying(50, 0.45, 10000, 500)
                .forDatasetNamed("$DATASET_NAME")
                .fromKafka()
                  .inTopic("$KAFKA_TOPIC")
                .userDefinedPartitioning()


```

#### 6. Output Configuration

By default, PROUD supports two types of output: influxDB and logger. The logger output
simply prints the items in the console while the influxDB output saves results in a connected
influxDB. Both types of output can be auto-resolved to a Hazelcast Sink.

The configuration of the output of PROUD can be seen below.

```java
import edu.auth.jetproud.proud.context.Proud;
import edu.auth.jetproud.application.parameters.data.ProudAlgorithmOption;

// Logger output
Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.Naive)
                .inSingleSpace()
                .querying(50, 0.45, 10000, 500)
                .forDatasetNamed("$DATASET_NAME")
                .fromKafka()
                  .inTopic("$KAFKA_TOPIC")
                .replicationPartitioned()
                .printingOutliers()

// InfluxDB output
Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.Naive)
                .inSingleSpace()
                .querying(50, 0.45, 10000, 500)
                .forDatasetNamed("$DATASET_NAME")
                .fromKafka()
                    .inTopic("$KAFKA_TOPIC")
                .replicationPartitioned()
                .writingOutliersToInfluxDB()
                    .inDatabase("$DB_NAME")
                        .locatedAt("$DB_HOST")
                        .authenticatedWith("$USERNAME","$PASSWORD")


```
#### 7. Building the configuration

After the configuration is created using the builder the `build()` method must be invoked to
finalize the configuration and perform a validation check. Additionally, in order to
enable debug mode in PROUD call the `enablingDebug()` method on the builder before finalizing
the configuration. The finalized configuration is an instance that implements `ProudContext` and
is used internally by PROUD to extract the configured options. A complete example of the configuration process can be seen below.

```java
import edu.auth.jetproud.proud.context.Proud;
import edu.auth.jetproud.application.parameters.data.ProudAlgorithmOption;

Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.Naive)
                .inSingleSpace()
                .querying(50, 0.45, 10000, 500)
                .forDatasetNamed("$DATASET_NAME")
                .fromKafka()
                    .inTopic("$KAFKA_TOPIC")
                .replicationPartitioned()
                .writingOutliersToInfluxDB()
                    .inDatabase("$DB_NAME")
                        .locatedAt("$DB_HOST")
                        .authenticatedWith("$USERNAME","$PASSWORD")
                .enablingDebug() // Optional
                .build();


```


### Command Line Arguments Configuration

An alternative to using the builder object to configure PROUD is to use command line arguments as wells as
environment variables to extract the configuration options. This method of configuring PROUD is
compatible with the Flink version of PROUD and uses the same options and variables.

#### Command Line Options

Command | Options
------- | --------
**space** | Possible value are: "*single*", "*rk*", "*rkws*".
**algorithm** | Possible values for *single-query space* are: "*naive*", "*advanced*", "*advanced_extended*", "*slicing*", "*pmcod*" and "*pmcod_net*". <BR> Possible values for multi-query space are "*amcod*", "*sop*", "*psod*" and "*pmcsky*".
**k** | Possible values include one or more integers - minimum kNeighbours. <BR> *50[;80;100]*
**R** | Possible values include one or more floating point numbers - distance ranges. <BR> *0.45[;0.15;0.97]*
**W** | Possible values include one or more integers - window sizes. <BR> *1000[;2000;3000]*
**S** | Possible values include one or more integers - slide sizes. <BR> *250[;500;1000]*
**dataset**| Possible values include a String - the name of the dataset.
**partitioning**| Partitioning option. <BR>"*replication*" partitioning is mandatory for "naive" and "advanced" algorithms whilst "*grid*" and "*tree*" partitioning is available for every other algorithm. <BR> "*grid*" technique needs pre-recorded data on the dataset's distribution. <BR> "*tree*" technique needs a file containing data points from the dataset in order to initialize the VP-tree
**tree_init** (Optional)| Represents the number of data points to be read for initialization by the "*tree*" partitioning technique.<BR> *Default value is 10000*


#### Environment Variables

Variable | Options
---------|--------
JOB_INPUT| The absolute path of the location of the dataset files.
KAFKA_BROKERS|The Kafka Broker to be used as the input
KAFKA_TOPIC|The Kafka Topic
INFLUXDB_DB|The name of the InfluxDB database
INFLUXDB_HOST|The host of the InfluxDB database
INFLUXDB_USER| The username needed for authenticating with the InfluxDB database
INFLUXDB_PASSWORD|The password needed for authenticating with the InfluxDB database

#### Creating the Configuration

The code below can used to create the required `ProudContext` instance by reading the configuration from the command line
arguments and environment variables discussed above. 

```java
import edu.auth.jetproud.proud.context.Proud;

public static void main(String[] args) throws ProudArgumentException {
        Proud proud = Proud.builder(args)
                           .enablingDebug()
                           .build();
}


```

#### Caveats

The command line configuration of PROUD is not recommended as it may only work with the two default datasets (Stocks, TAO)
especially when using grid partitioning.
Additionally, no user defined functionalities can be added with this style of configuration. 
The use of the builder object is the preferred way to configure PROUD, and
the command line option is used for compatibility purposes only.

## :dart: PROUD Pipeline API

After PROUD is configured the outlier detection process can be executed by invoking the
`ProudPipeline` and providing the created configuration by means of
a `ProudContext` instance. 

The complete general flow can be viewed below.

```java
// Configuration
Proud proud = Proud.builder()
        .forAlgorithm(ProudAlgorithmOption.Naive)
        .inSingleSpace()
        .querying(50, 0.45, 10000, 500)
        .forDatasetNamed("$DATASET_NAME")
        .fromKafka()
            .inTopic("$KAFKA_TOPIC")
        .replicationPartitioned()
        .writingOutliersToInfluxDB()
            .inDatabase("$DB_NAME")
                .locatedAt("$DB_HOST")
                .authenticatedWith("$USERNAME","$PASSWORD")
        .enablingDebug() // Optional
        .build();

// Pipeline
ProudPipeline pipeline = ProudPipeline.create(proud);

pipeline.readData()
        .partition()
        .detectOutliers()
        .aggregateAndWriteData();

// Execute PROUD Pipeline
Job job = ProudExecutor.executeJob(pipeline);
```

### Proud Source

The stream items are read from an instance that implements `ProudSource`
and can be ultimately resolved to a native Jet `Source`. Instances of `ProudSource`
that are implemented by default in PROUD can read stream items from a file,
or a kafka topic. 

#### Auto Source

As was mentioned previously the source can be automatically resolved from
the configuration. To create the appropriate source automatically use the
code below:

```java

// Configuration
Proud proud = /* Configuration Creation */ ;

// Create an appropriate source based 
//  on the specific configuration. 
ProudSource.auto(proud);

```

#### File Source

A file source reads stream items from a file located on the local filesystem.
All such sources implemented by default in PROUD are instances of `ProudFileSource` which
implements the `ProudSource` interface. By default, the home directory used in these sources
is retrieved from the configuration.

Proud file sources can be created by using one of the methods below:

```java

// Configuration
Proud proud = /* Configuration Creation */ ;

// File source by reading the File Name from
//  the configuration and using the default
//  field and value delimiters ("&" and "," respectively)
ProudSource.file(proud);

// File source by reading the specified file 
//  and using the default field and value 
//  delimiters ("&" and "," respectively)
ProudSource.file(proud, "$FILE_NAME");

// File source by reading the File Name from
//  the configuration and using the specified
//  field and value delimiters
ProudSource.file(proud, "$FIELD_DELIMITER", "$VALUE_DELIMITER");

// File source by reading the specified file
//  and using the specified field and value delimiters
ProudSource.file(proud, "$FILE_NAME", "$FIELD_DELIMITER", "$VALUE_DELIMITER");

```

#### Kafka Source

A Kafka source reads stream items from a Kafka broker and listening to a specific topic.
The Kafka source implemented by default in PROUD is an instance of `ProudKafkaSource` which
implements the `ProudSource` interface. By default, the Kafka connection information used in these source
are retrieved from the configuration.

Proud Kafka sources can be created by using the method below:

```java

// Configuration
Proud proud = /* Configuration Creation */ ;

// Kafka source that connects to the broker and topic
//  specified in the configuration
ProudSource.kafkaSource(proud);

```

#### User Defined Sources

The definition of custom PROUD sources can be achieved by implementing the `ProudSource`
interface or by extending the `ProudFileSource` or `ProudKafkaSource` classes. As the `ProudSource`
interface is the more abstract type in the PROUD sources type hierarchy it can be implemented
to use any type of data connector and source internally. The only constraint is that all
implementing types must implement the `readInto(Pipeline pipeline)` which reads data from a
native Jet source of any type and returns a `StreamStage<T extends AnyProudData>` instance.

The PROUD Pipeline provides a method to use any user defined or default source:

```java

// Configuration
Proud proud = /* Configuration Creation */ ;

// Proud Pipeline with a file source
pipeline.readFrom(ProudSource.file(proud))
        .partition()
        .detectOutliers()
        .aggregateAndWriteData();

// Proud Pipeline with a custom source
pipeline.readFrom(new ProudSource<AnyProudData>() { ... })
        .partition()
        .detectOutliers()
        .aggregateAndWriteData();

```

### Partition Data

The stream items that were read into the Proud Pipeline can be easily partitioned
by invoking the `partition()` method. The underlying partitioning implementation is
automatically selected by checking the configuration options.

### Detect Outliers

The partitioned stream items in the Proud Pipeline can be easily processed in order
to detect outliers by invoking the `detectOutliers()` method. The underlying 
algorithm, parameters and processing is automatically selected by checking the configuration options.

### Proud Sink

The results of the outlier detection process can be written to a sink that
implements the `ProudSink` interface and can be ultimately resolved to native
Jet `Sink`. Instances of `ProudSink` that are implemented by default in PROUD
can be used to write data to an influxDB server or print them to the console.

#### Auto Sink
As it was mentioned previously the sink can be automatically resolved from the
PROUD configuration. To create the appropriate sink automatically use the code below:

```java
// Configuration
Proud proud = /* Configuration Creation */ ;

// Create an appropriate sink based 
//  on the specific configuration. 
ProudSink.auto(proud);

```

#### Logger Sink

A logger sink writes stream items to the console stdout. The logger proud sink
implemented by default in PROUD is an instance of `ProudPrintSink` that implements
the interface `ProudSink`. 

Proud logger sinks can be created by using the method below:

```java
// Configuration
Proud proud = /* Configuration Creation */ ;

// Create a logger sink.
ProudSink.logger(proud);
```

#### InfluxDB Sink

An InfluxDB sink writes stream items to an InfluxDB database. The InfluxDB proud sink
implemented by default in PROUD is an instance of `ProudInfluxDBSink` that implements
the interface `ProudSink`.

Proud InfluxDB sinks can be created by using the method below:

```java
// Configuration
Proud proud = /* Configuration Creation */ ;

// Create an InfluxDB sink.
ProudSink.influxDB(proud);
```

#### User Defined Sinks

The definition of custom PROUD sinks can be achieved by implementing the `ProudSink`
interface or by extending the `AnyProudSink`,`ProudPrintSink` or `ProudInfluxDBSink` classes.
The `AnyProudSink` abstract class is the best type to use as an extension point in order
to use any data connector and sink internally, without losing the default implementation
of a required internal aggregation step. The only constraint is that all
implementing types must implement the `createJetSink()` and the 
`convertResultItem(long slide, OutlierQuery query)` methods.

The PROUD Pipeline provides a method to use any user defined or default sink:

```java

// Configuration
Proud proud = /* Configuration Creation */ ;

// Proud Pipeline with a logger sink
pipeline.readData()
        .partition()
        .detectOutliers()
        .aggregateAndWriteTo(ProudSink.logger(proud));

// Proud Pipeline with a custom sink
pipeline.readData()
        .partition()
        .detectOutliers()
        .aggregateAndWriteTo(new AnyProudSink<T>() { ... });

```

### Pipeline Downgrade

The Proud Pipeline can be downgraded to a native Jet pipeline at any point as the Proud Pipeline
is a proxy extension of the native Jet `Pipeline` type. A common downgrade use case is to 
further process the results produced by the outlier detection process.

Proud Pipeline downgrade example:

```java

// Configuration
Proud proud = /* Configuration Creation */ ;

// Proud Pipeline with a file source
pipeline.readFrom(ProudSource.file(proud))
        .partition() // Proud Pipeline
        .detectOutliers() // Proud Pipeline
        .filter((res)->res.second.outlierCount > 50) // Downgraded to a Jet Pipeline
        .map((it)->"Alert! Found "+it.second.outlierCount+" outliers.") // Jet Pipeline
        .writeTo(Sinks.logger()); // Jet Pipeline, Jet Sink

```

### Jet Pipeline Upgrade

Alternatively, a native Jet pipeline may be upgraded to a Proud Pipeline to easily 
incorporate outlier detection. This process can be easily achieved by using the
`from(Pipeline, ProudContext)` method in `ProudPipeline`.

A rather naive example of this operation can be seen below.

```java

// A generic native Jet Pipeline
StreamStage<ProudDataConvertible> jetPipeline = Pipeline.create()
        .readFrom(TestSources.itemStream(100))
        .withIngestionTimestamps()
        .map((num)->{
            // Map stream items to ProudDataConvertible items
            final Random random = new Random(num.timestamp());
            final List<Double> coordinates = new ArrayList<>();
            coordinates.add(random.nextDouble());
            
            return new ProudDataConvertible(){
                @Override
                public int identifier() {
                    return (int) num.sequence();
                }

                @Override
                public List<Double> coordinates() {
                    return coordinates;
                }

                @Override
                public long arrivalTime() {
                    return num.timestamp();
                }
            };
        });

// Proud Configuration
Proud proud = /* Configuration Creation */ ;

// Upgrade to Proud Pipeline
ProudPipeline.from(jetPipeline, proud)
        .partition()
        .detectOutliers()
        .aggregateAndWriteData();

```

## :jigsaw: Extension Points

In addition to the extension points for defining custom sources and sinks the Proud Pipeline
supports the definition of user defined partitioning methods and outlier detection algorithms.

### User Defined Partitioning Extension Points

Lorem ispum sit amet dolor me amor tiero accrecateur.

### User Defined Outlier Detection Extension points

Lorem ispum sit amet dolor me amor tiero accrecateur.


## :joystick: Execution

After the Proud library is configured and the pipeline is defined the pipeline can be submitted to Jet
for execution. This can be achieved either by using a predefined `ProudExecutor` helper or the
native Jet API.

```java
// Proud Executor pipeline execution
Job job = ProudExecutor.executeJob(pipeline);

// Native Jet API Execution
JetInstance jet = Jet.newJetInstance();
Job job = jet.newJob(pipeline.jetPipeline()); // IMPORTANT: Do not submit the ProudPipeline directly !!!

job.join();

```

## 	:link: References

**[1]**
Theodoros Toliopoulos, Christos Bellas, Anastasios Gounaris, and Apostolos Papadopoulos. 2020.
_PROUD: PaRallel OUtlier Detection for Streams._
In Proceedings of the 2020 ACM SIGMOD International Conference on Management of Data (SIGMOD '20).
Association for Computing Machinery, New York, NY, USA, 2717–2720.
DOI:https://doi.org/10.1145/3318464.3384688

**[2]**
Theodoros Toliopoulos, Anastasios Gounaris, Kostas Tsichlas, Apostolos Papadopoulos, Sandra Sampaio.
_Continuous outlier mining of streaming data in flink._
Information Systems, Volume 93, 2020, 101569, ISSN 0306-4379.
DOI:https://doi.org/10.1016/j.is.2020.101569.

**[3]**
Theodoros Toliopoulos and Anastasios Gounaris,
_Multi-parameter streaming outlier detection._
In Proceedings of the 2019 IEEE/WIC/ACM International Conference on Web Intelligence (WI), Thessaloniki, Greece, 2019, pp. 208-216.
DOI:https://doi.org/10.1145/3350546.3352520.
