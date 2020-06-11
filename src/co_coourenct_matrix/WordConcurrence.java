package co_coourenct_matrix;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *  *统计在若干篇文档中两个英文单词在一定窗口内同时出现的次数  * 如何计算二个单词出现的频率,使用pairs算法,该算法的流程就是:  *
 * 选择一个窗口的大小,使用队列,将队列的第一个值与后面的值分别成为一个  *  e,of 1  * we,on 1 we,said 1 we,should
 * 2 we,stay 1 we,that 1 we,the 2 we,them 1 we,us 1  * we,which 1 which,Junk 1
 * which,a 1 which,assures 1 which,food 1 which,is 1  * which,necessary 1
 * which,nutritions 1 which,the 1 which,us 1 who,at 1 who,ate  * 1 who,enjoy 1
 * who,main 1 who,meal 1 who,midday 1 who,now 1 who,their 1  * who,traditionally
 *  
 */
public class WordConcurrence {
    private static int MAX_WINDOW = 20;// 单词同现的最大窗口大小
    private static String wordRegex = "([a-zA-Z]{1,})";// 仅仅匹配由字母组成的简单英文单词
    private static Pattern wordPattern = Pattern.compile(wordRegex);// 用于识别英语单词(带连字符-)
    private static IntWritable one = new IntWritable(1);

    public static class WordConcurrenceMapper
            extends Mapper<Text, BytesWritable, co_coourenct_matrix.WordPair, IntWritable> {
        private int windowSize;
        private Queue<String> windowQueue = new LinkedList<String>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            windowSize = Math.min(context.getConfiguration().getInt("window", 2), MAX_WINDOW);
        }

        /**
         * 输入键位文档的文件名，值为文档中的内容的字节形式。  
         */
        @Override
        public void map(Text docName, BytesWritable docContent, Context context)
                throws IOException, InterruptedException {
            //用正则表达式查找单词
            Matcher matcher = wordPattern.matcher(new String(docContent.getBytes(), "UTF-8"));
            //对于每一个单词
            while (matcher.find()) {
                //加入到队列
                windowQueue.add(matcher.group());
                //如果已经加到窗口的大小上限
                if (windowQueue.size() >= windowSize) {
                    // 对于队列中的元素[q1,q2,q3...qn]发射[(q1,q2),1],[(q1,q3),1],
                    // ...[(q1,qn),1]出去
                    Iterator<String> it = windowQueue.iterator();
                    //拿到第一个词
                    String w1 = it.next();
                    while (it.hasNext()) {
                        //和后面所有单词组队
                        String next = it.next();
                        context.write(new co_coourenct_matrix.WordPair(w1, next), one);
                    }
                    //把第一个词扔掉，下次匹配就是队列里的第二个词
                    windowQueue.remove();
                }
            }
            while (!(windowQueue.size() <= 1)) {
                Iterator<String> it = windowQueue.iterator();
                String w1 = it.next();
                while (it.hasNext()) {
                    context.write(new co_coourenct_matrix.WordPair(w1, it.next()), one);
                }
                windowQueue.remove();
            }
        }

    }

    public static class WordConcurrenceReducer
            extends Reducer<co_coourenct_matrix.WordPair, IntWritable, co_coourenct_matrix.WordPair, IntWritable> {
        @Override
        public void reduce(co_coourenct_matrix.WordPair wordPair, Iterable<IntWritable> frequence, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : frequence) {
                sum += val.get();
            }
            context.write(wordPair, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();
        Job wordConcurrenceJob = Job.getInstance(conf, "wordConccurrence");
        wordConcurrenceJob.setJobName("wordConcurrenceJob");
        wordConcurrenceJob.setJarByClass(WordConcurrence.class);
        // wordConcurrenceJob.getConfiguration().setInt("window",
        // Integer.parseInt(args[2]));

        wordConcurrenceJob.setMapperClass(WordConcurrenceMapper.class);
        wordConcurrenceJob.setMapOutputKeyClass(co_coourenct_matrix.WordPair.class);
        wordConcurrenceJob.setMapOutputValueClass(IntWritable.class);

        wordConcurrenceJob.setReducerClass(WordConcurrenceReducer.class);
        wordConcurrenceJob.setOutputKeyClass(co_coourenct_matrix.WordPair.class);
        wordConcurrenceJob.setOutputValueClass(IntWritable.class);

        wordConcurrenceJob.setInputFormatClass(WholeFileInputFormat.class);
        wordConcurrenceJob.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(wordConcurrenceJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(wordConcurrenceJob, new Path(args[1]));

        wordConcurrenceJob.waitForCompletion(true);
        System.out.println("finished!");
    }
}
