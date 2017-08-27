package CDCStream;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;


public class DILCA2 {

    static class DatabaseStatistics {

        public DatabaseStatistics(ImmutableList<ImmutableList<String>> database) {
            if (database.size() == 0) {
                throw new IllegalArgumentException("Database cannot be empty.");
            }

            this.database = database;
            this.numAttributes = database.get(0).size();

            this.attributeDomains = new ArrayList<>(numAttributes);
            for (int i = 0; i < numAttributes; i++) {
                attributeDomains.add(new HashMap<>());
            }

            // Find domain of all attributes.
            int[] nextIndex = new int[numAttributes];
            for (ImmutableList<String> record : database) {
                for (int i = 0; i < numAttributes; i++) {
                    String value = record.get(i);
                    if (attributeDomains.get(i).containsKey(value)) {
                        attributeDomains.get(i).put(value, nextIndex[i]);
                        nextIndex[i]++;
                    }
                }
            }

            this.lazyCooccurrences = new int[numAttributes][][][];
            for (int i = 0; i < numAttributes; i++) {
                this.lazyCooccurrences[i] = new int[numAttributes - i][][];
            }
        }

        public int[][] cooccurrencesFor(int firstAttributeIndex, int secondAttributeIndex) {
            // Make sure `firstAttributeIndex` is the smaller index.
            if (firstAttributeIndex > secondAttributeIndex) {
                int temp = firstAttributeIndex;
                firstAttributeIndex = secondAttributeIndex;
                secondAttributeIndex = temp;
            }

            int[][] cooccurrenceMatrix = lazyCooccurrences[firstAttributeIndex][secondAttributeIndex];

            if (null == cooccurrenceMatrix) {
                int firstAttributeDomainSize = attributeDomains.get(firstAttributeIndex).size();
                int secondAttributeDomainSize = attributeDomains.get(secondAttributeIndex).size();

                cooccurrenceMatrix = new int[firstAttributeDomainSize][];
                for (int i = 0; i < firstAttributeDomainSize; i++) {
                    cooccurrenceMatrix[i] = new int[secondAttributeDomainSize];
                }

                for (ImmutableList<String> record : database) {
                    String firstAttributeValue = record.get(firstAttributeIndex);
                    String secondAttributeValue = record.get(secondAttributeIndex);

                    int firstValueIndex = attributeDomains.get(firstAttributeIndex).get(firstAttributeValue);
                    int secondValueIndex = attributeDomains.get(secondAttributeIndex).get(secondAttributeValue);

                    cooccurrenceMatrix[firstValueIndex][secondValueIndex]++;
                }

                lazyCooccurrences[firstAttributeIndex][secondAttributeIndex] = cooccurrenceMatrix;
            }

            return cooccurrenceMatrix;
        }

        private final ImmutableList<ImmutableList<String>> database;
        private final int numAttributes;
        private final List<Map<String, Integer>> attributeDomains;
        private final int[][][][] lazyCooccurrences;

    }

    public static DILCA distanceMatrixFor(ImmutableList<ImmutableList<String>> database,
                                          int targetAttributeIndex) {
        return null;
    }

}
