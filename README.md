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
that support queries in both single-query[2] and multi-query[3] parameter
spaces. For the multi-query outlier detection query case there are two
distinct types. The first one is the multi-app level spatial parameters
that are the minimum number of neighbours (K) in a specific spatial range
(R).The second one is an extension that supports multiple window sizes (W)
and window slide sizes (S).

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

Lorem ispum.

## :toolbox: Configuration

Before the PROUD pipeline can be used the library must be 
correctly configured.

### Configuration Builder

Here.

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
