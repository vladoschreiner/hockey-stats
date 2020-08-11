package biz.schr.cdcdemo;

import biz.schr.cdcdemo.dto.Player;
import biz.schr.cdcdemo.loader.PlayerMapLoader;
import biz.schr.cdcdemo.loader.RosterMapLoader;
import biz.schr.cdcdemo.util.Constants;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.map.IMap;

public class Starter {

    public static void main(String[] args) {

        // Start Jet cluster member
        JetInstance jet = Jet.newJetInstance(configureJet());

        // Trigger loaders to eagerly populate lookup tables from the database
        preloadLookupCaches(jet);

        // Start the streaming job
        jet.newJobIfAbsent(TopScorers.buildPipeline(), TopScorers.getJobConfig());

    }

    private static JetConfig configureJet() {
        JetConfig jc = JetConfig.loadDefault();

        // Reduce discovery to localhost
        jc.getHazelcastConfig().getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        jc.getHazelcastConfig().getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).addMember("localhost");

        // Set read-through backend for Roster-Player mapping cache
        MapStoreConfig rosterPlayerCacheConfig = new MapStoreConfig();
        rosterPlayerCacheConfig.setImplementation(new RosterMapLoader())
            .setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER);

        jc.getHazelcastConfig().getMapConfig(Constants.ROSTER_CACHE).setMapStoreConfig(rosterPlayerCacheConfig);

        // Set read-through backend for Roster-Player mapping cache
        MapStoreConfig playerDetailsCacheConfig = new MapStoreConfig();
        playerDetailsCacheConfig.setImplementation(new PlayerMapLoader())
                      .setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER);

        jc.getHazelcastConfig().getMapConfig(Constants.PLAYER_CACHE).setMapStoreConfig(playerDetailsCacheConfig);

        return jc;
    }

    private static void preloadLookupCaches(JetInstance jet) {
        System.out.println("Loading lookup data to cluster cache..");

        // Trigger map loaders to eagerly populate maps from the database
        IMap<Long,Long> roster = jet.getMap(Constants.ROSTER_CACHE);
        IMap<Long, Player> player = jet.getMap(Constants.PLAYER_CACHE);

        System.out.println("Roster->Player mapping lookup table size: " + roster.size());
        System.out.println("Player Details cache size: " + player.size());
    }

}
