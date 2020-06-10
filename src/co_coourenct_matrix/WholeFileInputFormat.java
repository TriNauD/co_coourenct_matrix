package co_coourenct_matrix;

import java.io.IOException;
//import java.nio.file.Path;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptContext;

public class WholeFileInputFormat extends FileInputFormat<Text,BytesWritable>{
	protected boolean isSplitable(JobContext context,Path filename) {
		return false;
	}
	
	public SingleFileNameReader createRecordReader(
			InputSplit split,TaskAttemptContext context)throws IOException,InterruptedException{
		return new SingleFileNameReader((FileSplit)split, context.getConfiguration());
	}

	@Override
	public RecordReader<Text, BytesWritable> getRecordReader(InputSplit arg0, JobConf arg1, Reporter arg2)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}
}
