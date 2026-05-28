package maelstrombroadcast.layers;

import java.util.List;
import org.json.JSONObject;

public class BestEffortBroadcast {
    private final List<String> allNodes;
    private final PerfectLink pl;

    public BestEffortBroadcast(List<String> allNodes, PerfectLink pl) {
        this.allNodes = allNodes;
        this.pl = pl;
    }

    // Abstracția bebBcast(m)
    public void bebBcast(JSONObject payload) {
        for (String node : allNodes) {
            // "Împachetăm" mesajul conform cerinței de mesaje cuibărite (nestable events)
            JSONObject bebMessage = new JSONObject();
            bebMessage.put("type", "beb_data");
            bebMessage.put("payload", payload);
            
            pl.plSend(node, bebMessage);
        }
    }
}