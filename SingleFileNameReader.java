package co_coourenct_matrix;

import java.io.IOException;
//import java.nio.file.Path;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class SingleFileNameReader extends RecordReader<Text, BytesWritable>{
	private FileSplit fileSplit;
	@SuppressWarnings("unuser")
	private Configuration conf;
	private boolean processed=false;
	private Text key=null;
	private BytesWritable value=null;
	private FSDataInputStream fis=null;

	public SingleFileNameReader(FileSplit fileSplit,Configuration conf) {
		this.fileSplit=fileSplit;
		this.conf=conf;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return processed?1.0f:0.0f;
	}

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		fileSplit=(FileSplit)arg0;
		Configuration job=arg1.getConfiguration();
		Path file=fileSplit.getPath();
		FileSystem fs=file.getFileSystem(job);
		fis=fs.open(file);
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if(key==null)
			key=new Text();
		if(value==null)
			value=new BytesWritable();
		if(!processed) {
			byte[] content=new byte[(int)fileSplit.getLength()];
			Path file=fileSplit.getPath();
			System.out.println(file.getName());
			key.set(file.getName());
			try {
				IOUtils.readFully(fis, content,0,content.length);
				value.set(new BytesWritable(content));
			}catch(IOException e) {
				e.printStackTrace();
			}finally {
				IOUtils.closeStream(fis);
			}
			processed=true;
			return true;
		}
		return false;
	}
	
	
}
