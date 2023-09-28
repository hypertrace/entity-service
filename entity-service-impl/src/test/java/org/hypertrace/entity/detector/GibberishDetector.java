package org.hypertrace.entity.detector;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This GibberishDetector is derived from https://github.com/paypal/Gibberish-Detector-Java. This is
 * an experimental feature, where we want to evaluate whether input segment contains gibberish
 * content or not. The intent is to provide better detection of random strings while evaluating
 * given input.
 */
public class GibberishDetector {
  private static final Logger LOGGER = LoggerFactory.getLogger(GibberishDetector.class);
  private static final int MIN_COUNT_VAL = 10;
  private final Map<Character, Integer> alphabetPositionMap = new HashMap<>();
  private final GibberishDetectorTrainingDataset trainingDataset;
  private double[][] logProbabilityMatrix;
  private double threshold = 0d;

  public GibberishDetector(String baseDir) throws MalformedURLException {
    this.trainingDataset = new GibberishDetectorTrainingDataset(baseDir);
    initializePositionMap();
    train();
  }

  /** determines if a sentence is gibberish or not. */
  public boolean isGibberish(String line) {
    return getAvgTransitionProbability(line, logProbabilityMatrix) <= threshold;
  }

  private double getAvgTransitionProbability(String line, double[][] logProbabilityMatrix) {
    double logProb = 0d;
    int transitionCount = 0;
    List<String> nGram = get2Gram(line);
    for (String touple : nGram) {
      logProb +=
          logProbabilityMatrix[alphabetPositionMap.get(touple.charAt(0))][
              alphabetPositionMap.get(touple.charAt(1))];
      transitionCount++;
    }
    return Math.exp(logProb / Math.max(transitionCount, 1));
  }

  private void train() {
    long startTs = System.currentTimeMillis();
    int[][] alphabetCouplesMatrix = getAlphaBetCouplesMatrix();
    logProbabilityMatrix = getLogProbabilityMatrix(alphabetCouplesMatrix);

    List<Double> validDataProbability =
        getAvgTransitionProbability(trainingDataset.getValidData(), logProbabilityMatrix);
    List<Double> invalidDataProbability =
        getAvgTransitionProbability(trainingDataset.getInvalidData(), logProbabilityMatrix);

    double minGood = Collections.min(validDataProbability);
    double maxBad = Collections.max(invalidDataProbability);

    if (minGood <= maxBad) {
      throw new AssertionError("cannot create a threshold");
    }
    threshold = getThreshold(minGood, maxBad);
    LOGGER.info(
        "Time taken to train gibberish detector is {} ms", System.currentTimeMillis() - startTs);
  }

  // can be overridden for another threshold heuristic implementation
  protected double getThreshold(double minGood, double maxBad) {
    return (minGood + maxBad) / 2;
  }

  private void initializePositionMap() {
    List<Character> alphabetChars = trainingDataset.getEligibleCharacters();
    for (int i = 0; i < alphabetChars.size(); i++) {
      alphabetPositionMap.put(alphabetChars.get(i), i);
    }
  }

  private String normalize(String line) {
    StringBuilder normalizedLine = new StringBuilder();
    for (char c : line.toLowerCase().toCharArray()) {
      normalizedLine.append(trainingDataset.getEligibleCharacters().contains(c) ? c : "");
    }
    return normalizedLine.toString();
  }

  private List<String> get2Gram(String line) {
    int n = 2;
    String filteredLine = normalize(line);
    List<String> nGram = new ArrayList<>();
    for (int start = 0; start < filteredLine.length() - n + 1; start++) {
      nGram.add(filteredLine.substring(start, start + n));
    }
    return nGram;
  }

  private int[][] getAlphaBetCouplesMatrix() {
    int[][] counts = createArray(trainingDataset.getEligibleCharacters().size());
    for (String line : trainingDataset.getTrainingData()) {
      List<String> nGram = get2Gram(line);
      for (String touple : nGram) {
        counts[alphabetPositionMap.get(touple.charAt(0))][
            alphabetPositionMap.get(touple.charAt(1))]++;
      }
    }
    return counts;
  }

  private double[][] getLogProbabilityMatrix(int[][] alphabetCouplesMatrix) {
    int alphabetLength = trainingDataset.getEligibleCharacters().size();
    double[][] probabilityMatrix = new double[alphabetLength][alphabetLength];
    for (int i = 0; i < alphabetCouplesMatrix.length; i++) {
      double sum = Arrays.stream(alphabetCouplesMatrix[i]).sum();
      for (int j = 0; j < alphabetCouplesMatrix[i].length; j++) {
        probabilityMatrix[i][j] = Math.log(alphabetCouplesMatrix[i][j] / sum);
      }
    }
    return probabilityMatrix;
  }

  private List<Double> getAvgTransitionProbability(
      List<String> lines, double[][] logProbabilityMatrix) {
    List<Double> result = new ArrayList<>();
    for (String line : lines) {
      result.add(getAvgTransitionProbability(line, logProbabilityMatrix));
    }
    return result;
  }

  private int[][] createArray(int length) {
    int[][] counts = new int[length][length];
    for (int[] count : counts) {
      Arrays.fill(count, MIN_COUNT_VAL);
    }
    return counts;
  }
}
