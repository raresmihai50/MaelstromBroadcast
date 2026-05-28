package maelstrombroadcast;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import maelstrombroadcast.layers.BestEffortBroadcast;
import maelstrombroadcast.layers.PerfectLink;
import maelstrombroadcast.layers.ReliableBroadcast;
import org.json.JSONObject;

public class EventProcessor implements Runnable {
    private final BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<>();
    private String selfId;
    private List<String> allNodes;

    // Stiva de straturi (Layers)
    private PerfectLink pl;
    private BestEffortBroadcast beb;
    private ReliableBroadcast rb;

    // Memoria nodului (ține minte numerele primite pentru operația de read)
    private final java.util.Set<Integer> messages = new java.util.concurrent.CopyOnWriteArraySet<>();

    public void init(String selfId, List<String> allNodes) {
        this.selfId = selfId;
        this.allNodes = allNodes;

        // Construim stiva de jos în sus (PL -> BEB -> RB)
        this.pl = new PerfectLink(selfId);
        this.beb = new BestEffortBroadcast(allNodes, pl);
        this.rb = new ReliableBroadcast(beb, selfId);
    }

    public void submitEvent(Event event) {
        eventQueue.add(event);
    }

    @Override
    public void run() {
        int messageCounter = 1;

        while (!Thread.currentThread().isInterrupted()) {
            try {
                Event event = eventQueue.take();
                if (event.getType() == Event.Type.STDIN_MESSAGE) {
                    JSONObject msg = event.getPayload();
                    JSONObject body = msg.getJSONObject("body");
                    String msgType = body.getString("type");

                    if ("echo".equals(msgType)) {
                        JSONObject replyBody = new JSONObject();
                        replyBody.put("type", "echo_ok");
                        replyBody.put("msg_id", messageCounter++);
                        replyBody.put("in_reply_to", body.getInt("msg_id"));
                        replyBody.put("echo", body.getString("echo"));

                        JSONObject reply = new JSONObject();
                        reply.put("src", selfId);
                        reply.put("dest", msg.getString("src"));
                        reply.put("body", replyBody);
                        System.out.println(reply.toString());
                    } 
                    else if ("topology".equals(msgType)) {
                        JSONObject replyBody = new JSONObject();
                        replyBody.put("type", "topology_ok");
                        replyBody.put("msg_id", messageCounter++);
                        replyBody.put("in_reply_to", body.getInt("msg_id"));

                        JSONObject reply = new JSONObject();
                        reply.put("src", selfId);
                        reply.put("dest", msg.getString("src"));
                        reply.put("body", replyBody);
                        System.out.println(reply.toString());
                    }
                    else if ("broadcast".equals(msgType)) {
                        int value = body.getInt("message");
                        
                        // 1. Salvăm mesajul în memoria locală
                        messages.add(value); 
                        
                        // 2. Cerem stratului Reliable Broadcast să îl distribuie sigur
                        rb.rbBcast(value);

                        // 3. Confirmăm clientului că am preluat cererea
                        JSONObject replyBody = new JSONObject();
                        replyBody.put("type", "broadcast_ok");
                        replyBody.put("msg_id", messageCounter++);
                        replyBody.put("in_reply_to", body.getInt("msg_id"));

                        JSONObject reply = new JSONObject();
                        reply.put("src", selfId);
                        reply.put("dest", msg.getString("src"));
                        reply.put("body", replyBody);
                        System.out.println(reply.toString());
                    }
                    else if ("beb_data".equals(msgType)) {
                        JSONObject payload = body.getJSONObject("payload");
                        int value = payload.getInt("message");
                        
                        // Întrebăm Reliable Broadcast dacă acest mesaj e nou
                        // Dacă e nou, funcția rbDeliver îl va și trimite automat (relay) mai departe!
                        boolean isNewMessage = rb.rbDeliver(value);
                        
                        if (isNewMessage) {
                            // Dacă e un mesaj pe care nu l-am mai văzut, îl reținem
                            messages.add(value);
                        }
                    }
                    else if ("read".equals(msgType)) {
                        JSONObject replyBody = new JSONObject();
                        replyBody.put("type", "read_ok");
                        replyBody.put("msg_id", messageCounter++);
                        replyBody.put("in_reply_to", body.getInt("msg_id"));
                        replyBody.put("messages", new org.json.JSONArray(messages));

                        JSONObject reply = new JSONObject();
                        reply.put("src", selfId);
                        reply.put("dest", msg.getString("src"));
                        reply.put("body", replyBody);
                        System.out.println(reply.toString());
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}