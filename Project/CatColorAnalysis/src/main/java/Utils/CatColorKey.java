package Utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class CatColorKey implements WritableComparable {

  public static final String DUMMY_COLOR = "Dummy";

  public static CatColorKey createDummy(String breed) {
    return new CatColorKey(breed, DUMMY_COLOR);
  }

  private String breed;
  private String color;

  public CatColorKey(String breed, String color) {
    this.breed = breed;
    this.color = color;
  }

  public CatColorKey() {
  }

  public String getBreed() {
    return breed;
  }

  public String getColor() {
    return color;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Text breedText = new Text(breed);
    Text colorText = new Text(color);
    breedText.write(out);
    colorText.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    Text breedText = new Text();
    Text colorText = new Text();
    breedText.readFields(in);
    colorText.readFields(in);

    this.breed = breedText.toString();
    this.color = colorText.toString();
  }

  /**
   * This compare method is used by the grouping comparator to group keys based on their breed
   *
   * @param other the other key object
   * @return
   */
  public int compare(CatColorKey other) {
    return this.breed.compareTo(other.breed);
  }

  @Override
  public int compareTo(Object o) {
    CatColorKey other = (CatColorKey) o;
    if (other.breed.equals(this.breed)) {
      //The dummy key will always come first
      if (this.color.equals(DUMMY_COLOR)) {
        return -1;
      } else if (other.color.equals(DUMMY_COLOR)) {
        return 1;
      } else {
        return this.color.compareTo(other.color);
      }
    } else {
      return compare(other);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CatColorKey that = (CatColorKey) o;
    return Objects.equals(breed, that.breed) && Objects.equals(color, that.color);
  }

  @Override
  public int hashCode() {
    return Objects.hash(breed, color);
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("CatColorKey{");
    sb.append("breed='").append(breed).append('\'');
    sb.append(", color='").append(color).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
