package biz.schr.cdcdemo.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.ArrayList;

public class Roster implements Serializable  {

    @JsonProperty("soupiskaid")
    public Long rosterId;

    @JsonProperty("hracid")
    public Long playerId;

    public int goalCount;

    public Roster() {
    }

    public Roster(Long rosterId, Long playerId, int goalCount) {
        this.rosterId = rosterId;
        this.playerId = playerId;
        this.goalCount = goalCount;
    }

    public Roster addGoal(Goal newGoal) {
        // goal table is append-only, so just increasiong the aggregated goal count
        Roster newRoster = new Roster(rosterId, playerId, goalCount + 1);
        return newRoster;
    }

    /**
     * Update this version from the previous one
     */
    public void updateFrom(Roster oldRoster) {
        goalCount = oldRoster.goalCount;
    }


    @Override public String toString() {
        return "Roster{rosterId=" + rosterId + ", goals=" + goalCount + '}';
    }
}
