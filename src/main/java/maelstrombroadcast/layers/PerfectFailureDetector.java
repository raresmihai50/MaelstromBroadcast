package maelstrombroadcast.layers;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import maelstrombroadcast.Event;
import maelstrombroadcast.EventProcessor;
import org.json.JSONObject;

public class PerfectFailureDetector {
    private final String selfId;
    private final List<String> allNodes;
    private final PerfectLink pl;
    private final EventProcessor processor;
    
    private final Map<String, Long> lastHeartbeat = new ConcurrentHashMap<>();
    private final Set<String> crashed = new HashSet<>();
    private final long TIMEOUT_MS = 2500; // Considerăm crash după 2.5 secunde de tăcere

    public PerfectFailureDetector(String selfId, List<String> allNodes, PerfectLink pl, EventProcessor processor) {
        this.selfId = selfId;
        this.allNodes = allNodes;
        this.pl = pl;
        this.processor = processor;
        
        long now = System.currentTimeMillis();
        for (String node : allNodes) {
            lastHeartbeat.put(node, now);
        }
    }

    public void onHeartbeat(String node) {
        lastHeartbeat.put(node, System.currentTimeMillis());
    }

    // Această funcție este apelată de EventProcessor când procesează un PFD_TIMEOUT din coadă
    public void onTimeout() {
        long now = System.currentTimeMillis();
        
        // 1. Detectăm cine a picat (Crash) și punem evenimentul în coadă
        for (String node : allNodes) {
            if (!node.equals(selfId) && !crashed.contains(node)) {
                if (now - lastHeartbeat.get(node) > TIMEOUT_MS) {
                    crashed.add(node);
                    JSONObject crashPayload = new JSONObject();
                    crashPayload.put("node", node);
                    processor.submitEvent(new Event(Event.Type.CRASH_DETECTED, crashPayload));
                }
            }
        }
        
        // 2. Trimitem propriul Heartbeat către toate celelalte noduri
        JSONObject hbBody = new JSONObject();
        hbBody.put("type", "heartbeat");
        for (String node : allNodes) {
            if (!node.equals(selfId)) {
                pl.plSend(node, hbBody);
            }
        }
    }
}