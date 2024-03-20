package neu.cs6240.Utils;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * This class only compare airlines using the FlightKey's compare method
 * This allows a single reducer to receive all records of each airline
 */
public class FlightGroupComparator extends WritableComparator {

  protected FlightGroupComparator() {
    super(FlightKey.class, true);
  }
  @Override
  public int compare(WritableComparable a, WritableComparable b) {
    FlightKey keyOne = (FlightKey) a,
              keyTwo = (FlightKey) b;
    return keyOne.compare(keyTwo);
  }
}
