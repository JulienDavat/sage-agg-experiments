package agg.engine.reducers;

import agg.http.data.SolutionGroup;

import java.util.List;

public interface Reducer {
    void accumulate(SolutionGroup group);

    List<SolutionGroup> getGroups();
}
