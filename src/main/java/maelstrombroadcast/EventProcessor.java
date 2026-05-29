package maelstrombroadcast;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import maelstrombroadcast.layers.BestEffortBroadcast;
import maelstrombroadcast.layers.PerfectFailureDetector;
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
    private PerfectFailureDetector pfd; // Noul strat PFD adăugat

    private final java.util.Set<Integer> messages = new java.util.concurrent.CopyOnWriteArraySet<>();

    public void init(String selfId, List<String> allNodes) {
        this.selfId = selfId;
        this.allNodes = allNodes;

        // Construim stiva
        this.pl = new PerfectLink(selfId);
        this.beb = new BestEffortBroadcast(allNodes, pl);
        this.rb = new ReliableBroadcast(beb, selfId, allNodes);
        this.pfd = new PerfectFailureDetector(selfId, allNodes, pl, this);

        // Respectarea cerinței: "a timer that puts a timeout event in the queue for PFD"
        Thread timer = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(1000); // Generăm eveniment de check în fiecare secundă
                    submitEvent(new Event(Event.Type.PFD_TIMEOUT, new JSONObject()));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        timer.setDaemon(true);
        timer.start();
    }

    public void submitEvent(Event event) {
        eventQueue.add(event);
    }

    @Override
    public void run() {
        int messageCounter = 1;

        while (!Thread.currentThread().isInterrupted()) {
            try {
                Event event = eventQueue.take(); // Un singur thread consumă secvențial coada
                
                // --- 1. Tratăm evenimentele de la PFD (Timeouts și Crashes) ---
                if (event.getType() == Event.Type.PFD_TIMEOUT) {
                    pfd.onTimeout();
                } 
                else if (event.getType() == Event.Type.CRASH_DETECTED) {
                    String crashedNode = event.getPayload().getString("node");
                    rb.onCrash(crashedNode);
                } 
                // --- 2. Tratăm mesajele normale de la rețea ---
                else if (event.getType() == Event.Type.STDIN_MESSAGE) {
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
                    else if ("heartbeat".equals(msgType)) {
                        // Semnalăm PFD-ului că nodul este viu
                        pfd.onHeartbeat(msg.getString("src"));
                    }
                    else if ("broadcast".equals(msgType)) {
                        int value = body.getInt("message");
                        
                        rb.rbBcast(value);
                        messages.add(value); // Îl livrăm și local (APP level)

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
                        String originalSender = payload.getString("original_sender");
                        String senderOfBeb = msg.getString("src");
                        
                        // rbDeliver se va ocupa singur de verificări și relay-uri Lazy!
                        boolean isNewMessage = rb.rbDeliver(senderOfBeb, originalSender, value);
                        
                        if (isNewMessage) {
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