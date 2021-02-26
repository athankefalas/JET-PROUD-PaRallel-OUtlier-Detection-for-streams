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
and Hazelcast Jet by [Athanasios Kefalas](https://github.com/athankefalas).

## :compass: Contents

1. **[Installation](#floppy_disk-installation)**
2. **[General](#bulb-general)**
3. **[Configuration](#toolbox-configuration)**
4. **[PROUD Pipeline API](#dart-proud-pipeline-api)**
5. **[Extension Points](#jigsaw-extension-points)**
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
K `-` number of neighbours, R `-` distance range, W `-` window size in milliseconds and S `-` window slide size in milliseconds. Exmaples of the 
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
file input datasource and a kafka input datasource which can be later automatically resolved to a Hazelcast Jet `Source`.

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

#### 5. Partitioing Method Configuration

The configuration of the partitioning method to be used by PROUD can also be configured via the builder instance. The available partitioning methods
and the algorithms they are supported by, are the following:

* Replication - **Naive, Advanced**
* Value Based with a Tree - **All algorithms except from Naive & Advanced**
* Value Based with a Grid - **All algorithms except from Naive & Advanced**
* User Defined - **Unknown**



### Command Line Arguments Configuration

Here 2.

## :dart: PROUD Pipeline API

Lorem ispum.

## :jigsaw: Extension Points

Lorem ispum.

## :joystick: Execution

Lorem ispum.

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
