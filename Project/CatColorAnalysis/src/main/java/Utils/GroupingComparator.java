package Utils;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * This class only compare keys using the CatColorKey's compare method
 * This allows a single reducer to receive all records of each cat breed
 */
public class GroupingComparator extends WritableComparator {
  protected GroupingComparator() {
    super(CatColorKey.class, true);
  }
  @Override
  public int compare(WritableComparable a, WritableComparable b) {
    CatColorKey keyOne = (CatColorKey) a,
        keyTwo = (CatColorKey) b;
    return keyOne.compare(keyTwo);
  }
}
