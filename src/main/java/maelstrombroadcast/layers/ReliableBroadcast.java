package maelstrombroadcast.layers;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import org.json.JSONObject;

public class ReliableBroadcast {
    private final BestEffortBroadcast beb;
    private final String selfId;
    
    // "delivered" din pseudocod: ține minte ce mesaje am procesat deja
    // pentru a nu le re-trimite la infinit într-o buclă
    private final Set<Integer> delivered = new CopyOnWriteArraySet<>();

    public ReliableBroadcast(BestEffortBroadcast beb, String selfId) {
        this.beb = beb;
        this.selfId = selfId;
    }

    // Funcția rbBcast(m) - apelată când primim un mesaj nou de la client
    public void rbBcast(int messageValue) {
        JSONObject payload = new JSONObject();
        payload.put("message", messageValue);
        payload.put("original_sender", selfId); // Pentru debugging
        
        // Trimitem prin BEB
        beb.bebBcast(payload);
    }

    // Funcția rbDeliver(m) - apelată de EventProcessor când primim un mesaj prin BEB de la alt nod
    public boolean rbDeliver(int messageValue) {
        // Dacă nu am mai văzut acest mesaj
        if (!delivered.contains(messageValue)) {
            delivered.add(messageValue);
            
            // RELAY (Partea vitală din Reliable Broadcast):
            // Îl dăm mai departe prin BEB ca să fim siguri că ajunge la toți
            rbBcast(messageValue);
            
            return true; // Spunem aplicației noastre să îl salveze
        }
        return false; // L-am mai văzut, îl ignorăm
    }
}