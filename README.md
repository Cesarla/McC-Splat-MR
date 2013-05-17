McC-Splat-MR
============

Master: [![Build Status](https://travis-ci.org/Cesarla/McC-Splat-MR.png?branch=master)](https://travis-ci.org/Cesarla/McC-Splat-MR)
Develop: [![Build Status](https://travis-ci.org/Cesarla/McC-Splat-MR.png?branch=develop)](https://travis-ci.org/Cesarla/McC-Splat-MR)

## What is it? ##
McC-Splat-MR is a MapReduce implementation of the McC-Splat Algorithm, created by Daniel Gayo Avello, using Hadoop Framework. McC-Splat is an iterative algorithm to perform vertex labeling on a partially labeled social network.

Read more about McC-Splat Alogirithm: [All liaisons are dangerous when all your friends are known to us](http://arxiv.org/PS_cache/arxiv/pdf/1012/1012.5913v1.pdf)

## Usage ##
```bin/hadoop jar McC-Splat-MR jar```
### Parameters ###

> follows <File>	:	File 'a' Follows 'b'

> data <File>          :	Data

> mode <Integer>       :	mode <"Plain Vanilla"=1, "Sink Absolute"=2, "Sink Relative"=3, "Percentile"=4>

> percentile <Integer>	:	Percentile
### Options ###
> -h (--help)	:	print this message

## License ##

```
  Copyright 2012 WESO Research Group, Daniel Gallo-Avello, César Luis Alvargonzález

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
```
