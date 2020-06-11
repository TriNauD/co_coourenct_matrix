package co_coourenct_matrix;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class WordPair implements WritableComparable<WordPair>{
	private String wordA;
	private String wordB;
	
	public WordPair() {
		
	}
	
	public WordPair(String wordA,String wordB) {
		this.wordA=wordA;
		this.wordB=wordB;
	}
	
	
	
	public String getWordA() {
		return wordA;
	}

	public String getWordB() {
		return wordB;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		wordA=in.readUTF();
		wordB=in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(wordA);
		out.writeUTF(wordB);
		
	}
	
	@Override
	public String toString() {
		return wordA+","+wordB;
	}

	@Override
	public int compareTo(WordPair o) {
		// TODO Auto-generated method stub
		if(this.equals(o))
			return 0;
		else
			//字符串比较
			return (wordA+wordB).compareTo(o.getWordA()+o.getWordB());
	}
	
	@Override
	public boolean equals(Object o) {
		//看一下对象是不是WordPair
		if(!(o instanceof WordPair))
			//不是这个类，肯定不相等
			return false;
		//转成这个类
		WordPair w=(WordPair)o;
		//如果两个单词都相等（不分前后顺序），就相等
		if((this.wordA.equals(w.wordA)&&this.wordB.equals(w.wordB))
				|| (this.wordB.contentEquals(w.wordA)&&this.wordA.contentEquals(w.wordB)))
			return true;
		return false;
	}
	
	@Override
	public int hashCode() {
		return (wordA.hashCode()+wordB.hashCode())*17;
	}

}
