

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Pair implements WritableComparable<Pair> {
	public Text key1;
	public Text key2;
	
	public Pair() {
		this.key1 = new Text("");
		this.key2 = new Text("");
	}
	
	public Pair(String key, String value) {
		this.key1 = new Text(key);
		this.key2 = new Text(value);
	}

	public String getKey1() {
		return key1.toString();
	}

	public void setKey1(String key) {
		this.key1 = new Text(key);
	}

	public String getKey2() {
		return key2.toString();
	}

	public void setKey2(String key) {
		this.key2 = new Text(key);
	}

	@Override
	public int compareTo(Pair o) {
		if(this.key1.toString().compareTo(o.key1.toString()) == 0) return this.key2.toString().compareTo(o.key2.toString());
		return this.key1.toString().compareTo(o.key1.toString());
	}
	
	@Override
	public String toString() {
		return "(" + key1.toString() + ", " + key2.toString() + ")";
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		key1.readFields(arg0);
		key2.readFields(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		key1.write(arg0);
		key2.write(arg0);
	}
	
}
