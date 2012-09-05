package org.weso.finalize;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FinalizeMapper extends Mapper<LongWritable, Text, Text, Text> {

}
