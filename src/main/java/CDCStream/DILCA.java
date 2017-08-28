package CDCStream;

import java.util.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;


public class DILCA {

    /*--------------------------------------------------------------------------*
     *                        STATIC MEMBERS AND METHODS                        *
     *--------------------------------------------------------------------------*/

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

                for (int j = 0; j < numAttributes; j++) {
                    cooccurrences[i][j] = new int[firstDomainSize][];
                    secondDomainSize = attributeDomains.get(j).size();

                    for (int k = 0; k < firstDomainSize; k++) {
                        cooccurrences[i][j][k] = new int[secondDomainSize];
                    }
                }
            }

            // Count co-occurrences.
            // Get rid of half of the co-occurrences.
            // TODO: Play around with the order of looping to find out whether they differ in speed.
            int firstIndex, secondIndex;

            for (ImmutableList<String> record : database) {
                for (int i = 0; i < numAttributes; i++) {
                    for (int j = 0; j < numAttributes; j++) {
                        firstIndex = attributeDomains.get(i).get(record.get(i));
                        secondIndex = attributeDomains.get(j).get(record.get(j));

                        this.cooccurrences[i][j][firstIndex][secondIndex]++;
                        this.cooccurrences[j][i][secondIndex][firstIndex]++;
                    }
                }
            }
        }

        public int[][] cooccurrencesFor(int firstAttributeIndex, int secondAttributeIndex) {
            return cooccurrences[firstAttributeIndex][secondAttributeIndex];
        }

        public int domainSize(int attributeIndex) {
            return attributeDomains.get(attributeIndex).size();
        }

        private final ImmutableList<ImmutableList<String>> database;
        private final int numAttributes;
        private final List<Map<String, Integer>> attributeDomains;
        private final int[][][][] cooccurrences;

    }

    public static DILCA distanceMatrixFor(ImmutableList<ImmutableList<String>> database,
                                          int targetAttributeIndex) {
        System.out.print(".");
        System.out.flush();

        if (database.size() == 0) {
            throw new IllegalArgumentException("Empty database.");
        }

        // Calculate statistics.
        DatabaseStatistics statistics = new DatabaseStatistics(database);

        // Find context attributes.
        Set<Integer> contextAttributeIndexes = contextAttributeIndexesFor(statistics, targetAttributeIndex);

        // Build the distance matrix.
        int targetDomainSize = statistics.domainSize(targetAttributeIndex);
        double[][] distances = new double[targetDomainSize - 1][];
        for (int i = 0; i < distances.length; i++) {
            distances[i] = new double[targetDomainSize - i - 1];
        }

        int[][] cooccurrences;
        int[] valueCooccurrences;
        double[] valueDistances;

        double totalContextDomainSizes = 0;
        for (int attributeIndex : contextAttributeIndexes) {
            cooccurrences = statistics.cooccurrencesFor(attributeIndex, targetAttributeIndex);

            // Calculate total sum of domain sizes for all attributes.
            totalContextDomainSizes += cooccurrences.length;

            // Calculate the sum of squared differences over all the values of current context attribute.
            for (int i = 0; i < cooccurrences.length; i++) {
                valueCooccurrences = cooccurrences[i];

                for (int j = 0; j < valueCooccurrences.length; j++) {
                    for (int k = j + 1; k < valueCooccurrences.length; k++) {
                        double difference = valueCooccurrences[j] - valueCooccurrences[k];
                        distances[j][k - j - 1] += difference * difference;
                    }
                }
            }
        }

        // Normalize sum of squared differences.
        for  (int i = 0; i < distances.length; i++) {
            valueDistances = distances[i];

            for (int j = 0; j < valueDistances.length; j++) {
                valueDistances[j] = Math.sqrt(valueDistances[j] / totalContextDomainSizes);
            }
        }

        return new DILCA(distances);
    }

    private static final double log2 = Math.log(2);

    private static Set<Integer> contextAttributeIndexesFor(DatabaseStatistics statistics,
                                                           int targetAttributeIndex) {
        int numAttributes = statistics.numAttributes;
        List<Double> uncertainties = new ArrayList<>(numAttributes);
        List<Integer> indexes = new ArrayList<>(numAttributes);

        // Calculate attribute relevance.
        for (int i = 0; i < numAttributes; i++) {
            if (i != targetAttributeIndex) {
                indexes.add(i);
                uncertainties.add(symmetricalUncertainty(statistics, targetAttributeIndex, i));
            } else {
                uncertainties.add(null);
            }
        }

        // Sort indexes in descending order based on their corresponding symmetrical uncertainty.
        indexes.sort((i, j) -> (int) Math.signum(uncertainties.get(i) - uncertainties.get(j)));

        // Remove redundant attributes.
        int firstAttribute, secondAttribute;
        for (int i = 0; i < indexes.size(); i++) {
            firstAttribute = indexes.get(i);
            if (-1 != firstAttribute) {
                for (int j = i; j < indexes.size(); j++) {
                    secondAttribute = indexes.get(j);
                    if (-1 != secondAttribute && symmetricalUncertainty(statistics, firstAttribute, secondAttribute) <=
                            uncertainties.get(secondAttribute)) {
                        indexes.set(j, -1);
                    }
                }
            }
        }

        ImmutableSet.Builder<Integer> contextBuilder = ImmutableSet.builder();
        for (int i : indexes) {
            if (i != -1) {
                contextBuilder.add(i);
            }
        }
        return contextBuilder.build();
    }

    private static double symmetricalUncertainty(DatabaseStatistics statistics,
                                                 int targetAttributeIndex,
                                                 int attributeIndex) {
        int[][] targetOccurrences = statistics.cooccurrencesFor(targetAttributeIndex, targetAttributeIndex);
        int[][] attributeOccurrences = statistics.cooccurrencesFor(attributeIndex, attributeIndex);
        int[][] cooccurrences = statistics.cooccurrencesFor(targetAttributeIndex, attributeIndex);

        double probability;

        double targetEntropy = 0;
        // Calculate target attribute's entropy.
        double totalTargetOccurrences = 0;
        for (int i = 0; i < targetOccurrences.length; i++) {
            totalTargetOccurrences += targetOccurrences[i][i];
        }
        for (int i = 0; i < targetOccurrences.length; i++) {
            probability = (double) targetOccurrences[i][i] / totalTargetOccurrences;
            targetEntropy -= probability * Math.log(probability) / log2;
        }

        double attributeEntropy = 0;
        // Calculate attribute's entropy.
        double totalAttributeOccurrences = 0;
        for (int i = 0; i < attributeOccurrences.length; i++) {
            totalAttributeOccurrences += attributeOccurrences[i][i];
        }
        for (int i = 0; i < attributeOccurrences.length; i++) {
            probability = (double) attributeOccurrences[i][i] / totalAttributeOccurrences;
            attributeEntropy -= probability * Math.log(probability) / log2;
        }

        double conditionalEntropy = 0;
        // Calculate conditional entropy of target attribute with respect to the given attribute.
        for (int i = 0; i < cooccurrences.length; i++) {
            int[] valueCooccurrences = cooccurrences[i];

            double totalValueCooccurrences = 0;
            for (int j = 0; j < valueCooccurrences.length; j++) {
                totalValueCooccurrences += valueCooccurrences[j];
            }

            for (int j = 0; j < valueCooccurrences.length; j++) {
                probability = (double) valueCooccurrences[j] / totalValueCooccurrences;
                conditionalEntropy -= probability * Math.log(probability) / log2;
            }
        }

        // Calculate symmetrical uncertainty.
        return (targetEntropy - conditionalEntropy) / (targetEntropy + attributeEntropy);
    }

    /*--------------------------------------------------------------------------*
     *                       INSTANCE MEMBERS AND METHODS                       *
     *--------------------------------------------------------------------------*/

    public double normalizedSquaredSumRoot() {
        double sum = 0;
        double[] valueDistances;

        for (int i = 0; i < distances.length; i++) {
            valueDistances = distances[i];

            for (int j = 0; j < valueDistances.length; j++) {
                sum += valueDistances[j];
            }
        }

        if (0 == sum) {
            return 0;
        } else {
            return (2 * Math.sqrt(sum)) / (distances.length * (distances.length - 1));
        }
    }

    private double[][] distances;

    private DILCA(double[][] distances) {
        this.distances = distances;
    }

}
