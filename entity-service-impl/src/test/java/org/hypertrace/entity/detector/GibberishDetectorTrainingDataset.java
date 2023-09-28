package org.hypertrace.entity.detector;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GibberishDetectorTrainingDataset {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(GibberishDetectorTrainingDataset.class);
  private static final String TRAINING_FILE_NAME = "trainingData.txt";
  private static final String VALID_DATA_FILE_NAME = "validData.txt";
  private static final String INVALID_DATA_FILE_NAME = "invalidData.txt";
  private final List<String> trainingData;
  private final List<String> validData;
  private final List<String> invalidData;
  private final List<Character> eligibleCharacters =
      "abcdefghijklmnopqrstuvwxyz "
          .chars()
          .mapToObj(e -> (char) e)
          .collect(Collectors.toUnmodifiableList());

  public GibberishDetectorTrainingDataset(String baseDir) throws MalformedURLException {
    this.trainingData =
        getAllLinesFromFile(new URL(Paths.get(baseDir, TRAINING_FILE_NAME).toString()));
    this.validData =
        getAllLinesFromFile(new URL(Paths.get(baseDir, VALID_DATA_FILE_NAME).toString()));
    this.invalidData =
        getAllLinesFromFile(new URL(Paths.get(baseDir, INVALID_DATA_FILE_NAME).toString()));
  }

  private List<String> getAllLinesFromFile(URL fileURL) {
    List<String> lines = new ArrayList<>();
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(fileURL.openStream()))) {
      while (reader.ready()) {
        lines.add(reader.readLine().trim());
      }
    } catch (IOException | NullPointerException e) {
      throw new RuntimeException("Can not initiate file: ", e);
    }
    return lines;
  }

  public List<String> getTrainingData() {
    return trainingData;
  }

  public List<String> getValidData() {
    return validData;
  }

  public List<String> getInvalidData() {
    return invalidData;
  }

  public List<Character> getEligibleCharacters() {
    return eligibleCharacters;
  }
}
