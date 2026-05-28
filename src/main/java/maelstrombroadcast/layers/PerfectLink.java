package maelstrombroadcast.layers;

import org.json.JSONObject;

public class PerfectLink {
    private final String selfId;

    public PerfectLink(String selfId) {
        this.selfId = selfId;
    }

    // Abstracția plSend(q, m)
    public void plSend(String dest, JSONObject body) {
        JSONObject msg = new JSONObject();
        msg.put("src", selfId);
        msg.put("dest", dest);
        msg.put("body", body);
        
        // Trimiterea fizică a mesajului pe rețea (către orchestrator)
        System.out.println(msg.toString());
    }
}