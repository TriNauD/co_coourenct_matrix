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

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class SingleFileNameReader extends RecordReader<Text, BytesWritable>{
	private FileSplit fileSplit;//输入文件的一部分
	@SuppressWarnings("unuser")
	private Configuration conf;
	private boolean processed=false;
	private Text key=null;//是文件名
	private BytesWritable value=null;//是个可写的Byte数组
	private FSDataInputStream fis=null;//文件Input流

	//构造函数 参数是FileSplit类型的对象
	public SingleFileNameReader(FileSplit fileSplit) {
		this.fileSplit=fileSplit;
		//this.conf=conf;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	//getter 获得当前的key
	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	//getter 获得当前的value
	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	// getter 获得当前是否已经处理完成的状态
	// 如果processed==true 返回1.0 否则返回0.0
	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return processed?1.0f:0.0f;
	}

	//初始化 传入InputSplit 和 TaskAttemptContext 初始化文件流fis
	//InputSplit是input文件的一部分
	//TaskAttemptContext里面是task配置
	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		fileSplit=(FileSplit)arg0;
		Configuration job=arg1.getConfiguration();
		Path file=fileSplit.getPath();
		FileSystem fs=file.getFileSystem(job);
		fis=fs.open(file);
		
	}
	//处理好了返回true 处理不好false
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if(key==null)
			key=new Text();
		if(value==null)
			value=new BytesWritable();
		//如果未处理
		if(!processed) {

			byte[] content=new byte[(int)fileSplit.getLength()];
			Path file=fileSplit.getPath();
			System.out.println(file.getName());
			key.set(file.getName());
			//读取fis里content.length长度的流放进content 长度不够则抛出异常 然后关闭文件流
			try {
				IOUtils.readFully(fis, content,0,content.length);
				//把content放进value里面
				value.set(new BytesWritable(content));
			}catch(IOException e) {
				e.printStackTrace();
			}finally {
				IOUtils.closeStream(fis);
			}
			//设置已处理完
			processed=true;
			return true;
		}
		return false;
	}
	
	
}
