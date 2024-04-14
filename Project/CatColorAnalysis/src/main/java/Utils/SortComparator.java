package Utils;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * This one decides how map output keys are sorted
 */
public class SortComparator extends WritableComparator {
  protected SortComparator() {super(CatColorKey.class, true);}

  @Override
  public int compare(WritableComparable a, WritableComparable b) {
    CatColorKey keyOne = (CatColorKey) a,
        keyTwo = (CatColorKey) b;

    return keyOne.compareTo(keyTwo);
  }
}
