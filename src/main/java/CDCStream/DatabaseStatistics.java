package CDCStream;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


class DatabaseStatistics {

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
                if (!attributeDomains.get(i).containsKey(value)) {
                    attributeDomains.get(i).put(value, nextIndex[i]);
                    nextIndex[i]++;
                }
            }
        }

        // Initialize co-occurrences data structure with zero.
        int firstDomainSize, secondDomainSize;

        cooccurrences = new int[numAttributes][][][];
        for (int i = 0; i < numAttributes; i++) {
            cooccurrences[i] = new int[numAttributes][][];
            firstDomainSize = attributeDomains.get(i).size();

            for (int j = i + 1; j < numAttributes; j++) {
                cooccurrences[i][j] = new int[firstDomainSize][];
                secondDomainSize = attributeDomains.get(j).size();

                for (int k = 0; k < firstDomainSize; k++) {
                    cooccurrences[i][j][k] = new int[secondDomainSize];
                }
            }
        }

        // Count co-occurrences.
        int firstIndex, secondIndex;

        for (ImmutableList<String> record : database) {
            for (int i = 0; i < numAttributes; i++) {
                for (int j = i + 1; j < numAttributes; j++) {
                    firstIndex = attributeDomains.get(i).get(record.get(i));
                    secondIndex = attributeDomains.get(j).get(record.get(j));
                    this.cooccurrences[i][j][firstIndex][secondIndex]++;
                }
            }
        }
    }

    public int[][] cooccurrencesFor(int lesserAttributeIndex, int greaterAttributeIndex) {
        return cooccurrences[lesserAttributeIndex][greaterAttributeIndex];
    }

    public int numAttributes() {
        return numAttributes;
    }

    public int domainSize(int attributeIndex) {
        return attributeDomains.get(attributeIndex).size();
    }

    private final ImmutableList<ImmutableList<String>> database;
    private final int numAttributes;
    private final List<Map<String, Integer>> attributeDomains;
    private final int[][][][] cooccurrences;

}

