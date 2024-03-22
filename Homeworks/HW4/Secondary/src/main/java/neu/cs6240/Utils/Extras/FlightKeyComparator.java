package neu.cs6240.Utils.Extras;

import neu.cs6240.Utils.FlightKey;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * This class acts as a more extensive comparator where it compares the airlines,
 * then the month in ascending order using the FlightKey class's compareTo method.
 */
public class FlightKeyComparator extends WritableComparator {
  protected FlightKeyComparator() {
    super(FlightKey.class, true);
  }

  @Override
  public int compare(WritableComparable a, WritableComparable b) {
    FlightKey keyOne = (FlightKey) a,
              keyTwo = (FlightKey) b;

    return keyOne.compareTo(keyTwo);
  }
}
