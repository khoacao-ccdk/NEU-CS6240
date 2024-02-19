package Utils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class Configuration {
  public static final Set<Character> VALID_START_WORDS =
      new HashSet<>(Arrays.asList('m','n','o','p','q','M','N','O','P','Q'));
  public static final int NUM_REDUCER = 5;
}
