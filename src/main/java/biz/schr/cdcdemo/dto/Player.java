package biz.schr.cdcdemo.dto;

import java.io.Serializable;

public class Player
        implements Serializable {

    private long playerId;

    private String firstName;

    private String lastName;

    private long goals;

    public Player(long playerId, String firstName, String lastName) {
        this.playerId = playerId;
        this.firstName = firstName;
        this.lastName = lastName;
    }


    public long getPlayerId() {
        return playerId;
    }

    public void setPlayerId(long playerId) {
        this.playerId = playerId;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public long getGoals() {
        return goals;
    }

    public void setGoals(long goals) {
        this.goals = goals;
    }

    @Override
    public String toString() {
        return getFirstName() + " " + getLastName() + ": " + getGoals() + " goals";
    }
}
