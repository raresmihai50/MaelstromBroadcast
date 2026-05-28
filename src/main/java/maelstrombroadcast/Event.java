package maelstrombroadcast;

import org.json.JSONObject;

public class Event {
    public enum Type { STDIN_MESSAGE, PFD_TIMEOUT, CRASH_DETECTED }
    
    private final Type type;
    private final JSONObject payload;

    public Event(Type type, JSONObject payload) {
        this.type = type;
        this.payload = payload;
    }

    public Type getType() { return type; }
    public JSONObject getPayload() { return payload; }
}