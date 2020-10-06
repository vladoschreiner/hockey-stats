package biz.schr.cdcdemo;

import biz.schr.cdcdemo.dto.Goal;
import biz.schr.cdcdemo.dto.Player;
import biz.schr.cdcdemo.dto.Roster;
import biz.schr.cdcdemo.util.Constants;
import biz.schr.cdcdemo.util.JSONUtils;
import biz.schr.cdcdemo.util.TopNUnique;
import com.hazelcast.function.ComparatorEx;
import com.hazelcast.function.PredicateEx;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.cdc.Operation;
import com.hazelcast.jet.cdc.ParsingException;
import com.hazelcast.jet.cdc.mysql.MySqlCdcSources;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.WindowDefinition;
import io.debezium.config.Configuration;
import com.hazelcast.jet.cdc.CdcSinks;
import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.cdc.mysql.MySqlCdcSources;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.hazelcast.function.Functions.wholeItem;

/**
 * Simple Jet pipeline which consumes "goal" events from MySQL
 * using CDC and maintains top scorers
 */
public class TopScorers {

    public static final int RANKING_TABLE_SIZE = 5;
    private static final String GOALS_TABLE = "branka";
    private static final String ROSTER_TABLE = "soupiska";
    private static final String PLAYER_TABLE = "hrac";

    static JobConfig getJobConfig() {
        JobConfig jc = new JobConfig();
        jc.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        jc.setName("Top scorers using CDC");
        return jc;
    }

    static Pipeline buildPipeline() throws ParsingException {
        Pipeline p = Pipeline.create();

        String[] TABLE_WHITELIST = { "ahlcz5.branka", "ahlcz5.soupiska", "ahlcz5.hrac"  };

        StreamSource<ChangeRecord> source = MySqlCdcSources.mysql("cdc-demo-connector")
                                                           .setDatabaseAddress("192.168.56.101")
                                                           .setDatabasePort(3306)
                                                           .setDatabaseUser("dbz")
                                                           .setDatabasePassword("dbz")
                                                           .setClusterName("dbserver1")
                                                           .setDatabaseWhitelist("ahlcz5")
                                                           .setTableWhitelist(TABLE_WHITELIST)
                                                           .setCustomProperty("include.schema.changes", "false")
                                                           //.setReconnectBehavior(RetryStrategies.indefinitely(500))
                                                           //.setShouldStateBeResetOnReconnect(false)
                                                           .build();

        // Continuously get and parse change records
        StreamStage<ChangeRecord> allChangeRecords = p.readFrom(source).withIngestionTimestamps();

        allChangeRecords
                .window(WindowDefinition.tumbling(5_000))
                .aggregate(AggregateOperations.counting())
                .writeTo(Sinks.logger(c -> c.result() + "/last 5 sec"
                        + ""));

        StreamStage<Object> goals = allChangeRecords
                .filter(table(GOALS_TABLE))
                .map(change -> (Object) change.value().toObject(Goal.class));

        StreamStage<Object>  rosters = allChangeRecords
                .filter(table(ROSTER_TABLE))
                .map(change -> (Object) change.value().toObject(Roster.class))
                .merge(goals)
                .groupingKey(TopScorers::getRosterId)
                .mapStateful(
                    () -> new OneToManyMapper<>(Roster.class,
                            Goal.class,
                            Roster::updateFrom,
                            Roster::addGoal),
                    OneToManyMapper::mapState);

        // join roster with players
        StreamStage<Player> playersWithGoals = allChangeRecords
                .filter(table(PLAYER_TABLE))
                .map(change -> (Object) change.value().toObject(Player.class))
                .merge(rosters)
                .groupingKey(TopScorers::getPlayerId)
                .mapStateful(
                        () -> new OneToManyMapper<>(Player.class,
                                Roster.class,
                                Player::updateFrom,
                                Player::addRoster),
                        OneToManyMapper::mapState);

        // log the output for player Tošnar
        playersWithGoals
                .filter(a -> a.lastName.compareTo("Tošnar") == 0)
                .writeTo(Sinks.logger( h -> "(" + h.playerId + ") " +   h.firstName + " " + h.lastName + ": " + h.goals));


        // update top 5
//        playersWithGoals
//                .map(player -> Util.entry(player.playerId, player))
//                .rollingAggregate(TopNUnique.topNUnique(RANKING_TABLE_SIZE, ComparatorEx.comparingLong(entry -> entry.getValue().goals)))
//                .writeTo(Sinks.logger());
                // .writeTo(Sinks.observable(Constants.TOP_SCORERS_OBSERVABLE));

        return p;
    }

    private static Long getPlayerId(Object o) {
        if (o instanceof Roster) {
            Roster roster = (Roster) o;
            return roster.playerId;
        } else if (o instanceof Player) {
            Player player = (Player) o;
            return player.playerId;
        } else {
            throw new IllegalArgumentException("Unknown type " + o.getClass());
        }
    }

    private static Long getRosterId(Object o) {
        if (o instanceof Roster) {
            Roster roster = (Roster) o;
            return roster.rosterId;
        } else if (o instanceof Goal) {
            Goal goal = (Goal) o;
            return goal.rosterId;
        } else {
            throw new IllegalArgumentException("Unknown type " + o.getClass());
        }
    }

    private static PredicateEx<ChangeRecord> table(String table) throws ParsingException {
        return (changeRecord) -> changeRecord.table().equals(table);
    }
    
    private static StreamStage<List<Map.Entry<Long, Long>>> sendUpdatesOnlyForChanges(StreamStage<List<Map.Entry<Long, Long>>> input) {
        return input.filterStateful(
                () -> new Object[1],
                (lastItem, item) -> {
                    if (lastItem[0] == null) {
                        lastItem[0] = item;
                        return true;
                    } else if (lastItem[0].equals(item)) {
                        return false;
                    } else {
                        lastItem[0] = item;
                        return true;
                    }
                }
        );
    }

}