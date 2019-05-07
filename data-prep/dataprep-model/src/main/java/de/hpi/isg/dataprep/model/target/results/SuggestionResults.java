package de.hpi.isg.dataprep.model.target.results;

import java.util.*;
import java.util.stream.Collectors;

/**
 * A {@link SuggestionResults} instance stores the list of suggested preparators as well as their scores for a particular step in a pipeline.
 *
 * @author Lan Jiang
 * @since 2019-05-07
 */
public class SuggestionResults {

    /**
     * This array stores the list of the suggested preparators.
     */
    private SuggestedPreparator[] suggestedPreparators;

    public SuggestionResults(int suggestionListSize) {
        this.suggestedPreparators = new SuggestedPreparator[suggestionListSize];
    }

    /**
     * Add the suggested preparator and its corresponding score to the result list.
     *
     * @param suggestedPreparators the list of suggested preparators
     */
    public void addSuggestion(List<SuggestedPreparator> suggestedPreparators) {
        suggestedPreparators = getSortedSuggestedPreparators(suggestedPreparators);
        if (suggestedPreparators.size() > this.suggestedPreparators.length) {
            this.suggestedPreparators = suggestedPreparators
                    .subList(0, this.suggestedPreparators.length).toArray(new SuggestedPreparator[0]);
        }
        else {
            this.suggestedPreparators = suggestedPreparators.toArray(new SuggestedPreparator[0]);
        }
    }

    /**
     * Sort the suggested preparator list by their scores.
     *
     * @param descending whether the sorting is descending or not.
     */
    private List<SuggestedPreparator> sortByScore(List<SuggestedPreparator> suggestedPreparators, boolean descending) {
        if (descending) {
            return suggestedPreparators.stream()
                    .sorted(Comparator.comparing(SuggestedPreparator::getScore).reversed()).collect(Collectors.toList());
        }
        else {
            return suggestedPreparators.stream()
                    .sorted(Comparator.comparing(SuggestedPreparator::getScore)).collect(Collectors.toList());
        }
    }

    public SuggestedPreparator[] getSuggestedPreparators() {
        return suggestedPreparators;
    }

    private List<SuggestedPreparator> getSortedSuggestedPreparators(List<SuggestedPreparator> suggestedPreparators) {
        // Todo: currently only support descending sort.
        return sortByScore(suggestedPreparators, true);
    }
}
