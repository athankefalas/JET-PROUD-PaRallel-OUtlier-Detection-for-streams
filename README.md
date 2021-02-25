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

This version of PROUD was developed as part of a thesis project in Java
and Hazelcast Jet by [Athanasios Kefalas](https://github.com/athankefalas).

## Contents

1. Installation
2. Configuration
3. Pipeline API
4. Extension Points
5. Execution

## Installation

Lorem ispum.

## Configuration

Lorem ispum.

## PROUD Pipeline API

Lorem ispum.

## Extension Points

Lorem ispum.

## Execution

Lorem ispum.

## References

[1]
Theodoros Toliopoulos, Christos Bellas, Anastasios Gounaris, and Apostolos Papadopoulos. 2020.
PROUD: PaRallel OUtlier Detection for Streams.
In Proceedings of the 2020 ACM SIGMOD International Conference on Management of Data (SIGMOD '20).
Association for Computing Machinery, New York, NY, USA, 2717–2720.
DOI:https://doi.org/10.1145/3318464.3384688

[2]
Theodoros Toliopoulos, Anastasios Gounaris, Kostas Tsichlas, Apostolos Papadopoulos, Sandra Sampaio.
Continuous outlier mining of streaming data in flink.
Information Systems, Volume 93, 2020, 101569, ISSN 0306-4379.
DOI:https://doi.org/10.1016/j.is.2020.101569.

[3]
Theodoros Toliopoulos and Anastasios Gounaris,
Multi-parameter streaming outlier detection.
In Proceedings of the 2019 IEEE/WIC/ACM International Conference on Web Intelligence (WI), Thessaloniki, Greece, 2019, pp. 208-216.
DOI:https://doi.org/10.1145/3350546.3352520.
