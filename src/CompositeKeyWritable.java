import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Created by egamkar on 7/14/2017.
 */
public class CompositeKeyWritable implements Writable,
        WritableComparable<CompositeKeyWritable> {

    private String empId;
    private int timeStamp;

    public CompositeKeyWritable() {
    }

    public CompositeKeyWritable(String empId, int timeStamp) {
        this.empId = empId;
        this.timeStamp = timeStamp;
    }

    @Override
    public String toString() {
        return (new StringBuilder().append(empId).append("\t")
                .append(timeStamp)).toString();
    }

    public void readFields(DataInput dataInput) throws IOException {
        empId = WritableUtils.readString(dataInput);
        timeStamp = WritableUtils.readVInt(dataInput);
    }

    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeString(dataOutput, empId);
        WritableUtils.writeVInt(dataOutput, timeStamp);
    }

    public int compareTo(CompositeKeyWritable objKeyPair) {

        int result = empId.compareTo(objKeyPair.empId);
        if (0 == result) {
            result = Integer.compare(timeStamp,objKeyPair.timeStamp);
        }
        return result;
    }

    public String getempId() {
        return empId;
    }

    public void setempId(String empId) {
        this.empId = empId;
    }

    public int getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(int timeStamp) {
        this.timeStamp = timeStamp;
    }
}
