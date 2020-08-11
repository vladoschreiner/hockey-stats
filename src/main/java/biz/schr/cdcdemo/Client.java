package biz.schr.cdcdemo;

import biz.schr.cdcdemo.dto.Player;
import biz.schr.cdcdemo.util.Constants;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Observable;

import java.util.List;

public class Client {

    public static void main(String[] args) {

        JetInstance jet = Jet.newJetClient();

        Observable<List<Player>> observable = jet.getObservable(Constants.TOP_SCORERS_OBSERVABLE);
        observable.addObserver(System.out::println);

    }

}
