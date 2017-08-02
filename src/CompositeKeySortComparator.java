/**
 * Created by egamkar on 7/14/2017.
 */
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeySortComparator extends WritableComparator {

    protected CompositeKeySortComparator() {
        super(CompositeKeyWritable.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        CompositeKeyWritable key1 = (CompositeKeyWritable) w1;
        CompositeKeyWritable key2 = (CompositeKeyWritable) w2;

        int cmpResult = key1.getempId().compareTo(key2.getempId());
        if (cmpResult == 0)// same empId
        {
            return -Integer.compare(key1.getTimeStamp(),key2.getTimeStamp());
            //If the minus is taken out, the values will be in
            //ascending order
        }
        return cmpResult;
    }
}
