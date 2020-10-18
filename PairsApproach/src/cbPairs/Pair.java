package cbPairs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Pair implements WritableComparable<Pair> {
	private Text item1;
	private Text item2;

	public Pair() {
		this.item1 = new Text();
		this.item2 = new Text();
	}

	public Pair(Text item1, Text item2) {
		this.item1 = item1;
		this.item2 = item2;
	}

	public Text getItem1() {
		return item1;
	}

	public void setItem1(Text item1) {
		this.item1 = item1;
	}

	public Text getItem2() {
		return item2;
	}

	public void setItem2(Text item2) {
		this.item2 = item2;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
			item1.readFields(in);
			item2.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		item1.write(out);
		item2.write(out);

	}

	@Override
	public int compareTo(Pair o) {
		int k = item1.compareTo(o.getItem1());
		if(k != 0) {
			return k;
		}
		return item2.compareTo(o.item2);	
	}
	
	@Override
	public String toString() {	
		return item1 + ":" + item2 + "      ";
	}
}
