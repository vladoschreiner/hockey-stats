package biz.schr.cdcdemo.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Player
        implements Serializable {

    @JsonProperty("hracid")
    public Long playerId;

    @JsonProperty("jmeno")
    public String firstName;

    @JsonProperty("prijmeni")
    public String lastName;

    /**
     * List of Roster records, each with a goal count
     */
    public List<Roster> rosters = new ArrayList<>();

    /**
     * Goal count cache
     */
    public int goals = 0;


    public Player() {
    }

    public Player(long playerId, String firstName, String lastName) {
        this.playerId = playerId;
        this.firstName = firstName;
        this.lastName = lastName;
    }

    /**
     * Update player form a previous version
     */
    public void updateFrom(Player oldPlayer) {
        rosters = new ArrayList<>(oldPlayer.rosters);
        goals = oldPlayer.goals;
    }

    /**
     * Add new roster to this Player.
     * Creating a new Player object;
     */
    public Player addRoster(Roster newRoster) {
        Player newPlayer = new Player(playerId, firstName, lastName);

        boolean updated = false;
        for (Roster roster : rosters) {

            if (roster.rosterId.equals(newRoster.rosterId)) { // Update previous value
                newPlayer.rosters.add(newRoster);
                newPlayer.goals += newRoster.goalCount;
                updated = true;
            } else { // copy previous rosters
                newPlayer.rosters.add(roster);
                newPlayer.goals += roster.goalCount;
            }
        }

        // add a new roster
        if (!updated) {
            newPlayer.rosters.add(newRoster);
            newPlayer.goals += newRoster.goalCount;
        }

        return newPlayer;
    }


    public Long getGoals() {
        return Long.valueOf(goals);
    }

    @Override
    public String toString() {
        return firstName + ' ' + lastName + ": " + goals + " goals";
    }
}
