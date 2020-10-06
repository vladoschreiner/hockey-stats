package biz.schr.cdcdemo.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.List;

/**
 * Goal table expected to be append-only (no updates)
 */
public class Goal implements Serializable {

    @JsonProperty("brankaid")
    public Long goalId;

    @JsonProperty("soupiskaid_strelec")
    public Long rosterId;

    public Goal() {
    }

    public Goal(Long rosterId) {
        this.rosterId = rosterId;
    }


    @Override public String toString() {
        return "Goal{goalId=" + goalId + ", rosterId=" + rosterId + '}';
    }
}
