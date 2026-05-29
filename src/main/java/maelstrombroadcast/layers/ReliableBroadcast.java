package maelstrombroadcast.layers;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import org.json.JSONObject;

public class ReliableBroadcast {
    private final BestEffortBroadcast beb;
    private final String selfId;
    
    // correct := Pi (Inițializată cu toate nodurile active)
    private final Set<String> correct = new CopyOnWriteArraySet<>();
    
    // from[p] := ∅ (Ce mesaje au fost generate inițial de fiecare nod)
    private final Map<String, Set<Integer>> from = new ConcurrentHashMap<>();

    public ReliableBroadcast(BestEffortBroadcast beb, String selfId, List<String> allNodes) {
        this.beb = beb;
        this.selfId = selfId;
        
        // La Init, toate nodurile sunt considerate "corecte"
        this.correct.addAll(allNodes);
        
        for (String node : allNodes) {
            from.put(node, new CopyOnWriteArraySet<>());
        }
    }

    // upon event <rb, Broadcast | m> do
    public void rbBcast(int messageValue) {
        // trigger <beb, Broadcast | [DATA, self, m]>
        JSONObject payload = new JSONObject();
        payload.put("message", messageValue);
        payload.put("original_sender", selfId);
        
        beb.bebBcast(payload);
    }

    // upon event <beb, Deliver | p, [DATA, s, m]> do
    public boolean rbDeliver(String senderOfBeb, String originalSender, int messageValue) {
        Set<Integer> senderMessages = from.computeIfAbsent(originalSender, k -> new CopyOnWriteArraySet<>());
        
        // if m ∉ from[s] then
        if (!senderMessages.contains(messageValue)) {
            // from[s] := from[s] U {m}
            senderMessages.add(messageValue);
            
            // if s ∉ correct then trigger <beb, Broadcast | [DATA, s, m]>
            if (!correct.contains(originalSender)) {
                JSONObject payload = new JSONObject();
                payload.put("message", messageValue);
                payload.put("original_sender", originalSender);
                beb.bebBcast(payload);
            }
            
            return true; // Semnalăm aplicației (App) să îl livreze local
        }
        return false;
    }

    // upon event <P, Crash | p> do
    public void onCrash(String p) {
        // correct := correct \ {p}
        correct.remove(p);
        
        // forall m ∈ from[p] do trigger <beb, Broadcast | [DATA, p, m]>
        Set<Integer> senderMessages = from.get(p);
        if (senderMessages != null) {
            for (int m : senderMessages) {
                JSONObject payload = new JSONObject();
                payload.put("message", m);
                payload.put("original_sender", p); // Menținem sursa originală a mesajului
                beb.bebBcast(payload);
            }
        }
    }
}