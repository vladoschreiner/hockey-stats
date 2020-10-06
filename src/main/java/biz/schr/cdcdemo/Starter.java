package biz.schr.cdcdemo;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.cdc.ParsingException;
import com.hazelcast.jet.config.JetConfig;

public class Starter {


    public static void main(String[] args) {

        // Start Jet cluster member
        JetInstance jet = Jet.newJetInstance(configureJet());

        // Trigger loaders to eagerly populate lookup tables from the database
        // preloadLookupCaches(jet);

        // Start the streaming job
        try {
            jet.newJobIfAbsent(TopScorers.buildPipeline(), TopScorers.getJobConfig()).join();
        } catch (ParsingException e) {
            e.printStackTrace();
        } finally {
            jet.shutdown();
        }
    }

    private static JetConfig configureJet() {
        JetConfig jc = JetConfig.loadDefault();

        // Reduce discovery to localhost
        jc.getHazelcastConfig().getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        jc.getHazelcastConfig().getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).addMember("localhost");

        return jc;
    }

}
