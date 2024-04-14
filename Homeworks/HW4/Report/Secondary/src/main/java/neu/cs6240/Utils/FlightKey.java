package neu.cs6240.Utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class FlightKey implements WritableComparable {
  private Text airline;
  private Text month;

  public FlightKey() {
  }

  public FlightKey(String airline, String month) {
    this.airline = new Text(airline);
    this.month = new Text(month);
  }

  public String getAirline() {
    return airline.toString();
  }

  public String getMonth() {
    return month.toString();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    airline.write(out);
    month.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.airline = new Text();
    this.month = new Text();

    airline.readFields(in);
    month.readFields(in);
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("FlightKey{");
    sb.append("airline=").append(airline.toString());
    sb.append(", month=").append(month.toString());
    sb.append('}');
    return sb.toString();
  }

  public int compare(Object o) {
    FlightKey other = (FlightKey) o;
    return other.airline.compareTo(this.airline);
  }

  @Override
  public int compareTo(Object o) {
    //If they have different airline name, return the result of sorting by airline
    int airlineComp = this.compare(o);
    if(airlineComp != 0) return airlineComp;

    //Else, sort by month ascending
    FlightKey other = (FlightKey) o;
    int thisMonth = Integer.parseInt(this.month.toString());
    int otherMonth = Integer.parseInt(other.month.toString());
    return Integer.compare(thisMonth, otherMonth);
  }
}
