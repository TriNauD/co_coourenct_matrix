package co_coourenct_matrix;

import java.io.IOException;
//import java.nio.file.Path;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class WholeFileInputFormat extends FileInputFormat<Text,BytesWritable>{
	protected boolean isSplitable(JobContext context,Path filename) {
		return false;
	}
	
	@Override
	public SingleFileNameReader createRecordReader(
			InputSplit split,TaskAttemptContext context)throws IOException,InterruptedException{
		return new SingleFileNameReader((FileSplit)split);
	}

}
