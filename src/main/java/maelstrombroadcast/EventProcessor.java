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
    
    // =======================================================
    // BUTONUL DE COMUTARE IMPLEMENTĂRI
    // true  = Eager Reliable Broadcast (Fără PFD, retransmisie instantanee, flood pe rețea)
    // false = Lazy Reliable Broadcast (Algoritmul 3.2, folosește PFD și mulțimea 'correct')
    private final boolean EAGER_MODE = false; 
    // =======================================================

    private final BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<>();
    private String selfId;
    private List<String> allNodes;

    private PerfectLink pl;
    private BestEffortBroadcast beb;
    private ReliableBroadcast rb;
    private PerfectFailureDetector pfd;

    private final java.util.Set<Integer> messages = new java.util.concurrent.CopyOnWriteArraySet<>();

    public void init(String selfId, List<String> allNodes) {
        this.selfId = selfId;
        this.allNodes = allNodes;

        this.pl = new PerfectLink(selfId);
        this.beb = new BestEffortBroadcast(allNodes, pl);
        this.rb = new ReliableBroadcast(beb, selfId, allNodes);
        this.pfd = new PerfectFailureDetector(selfId, allNodes, pl, this);

        // Pornim timer-ul pentru PFD DOAR dacă rulăm în modul Lazy
        if (!EAGER_MODE) {
            Thread timer = new Thread(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        Thread.sleep(1000);
                        submitEvent(new Event(Event.Type.PFD_TIMEOUT, new JSONObject()));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
            timer.setDaemon(true);
            timer.start();
        }
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
                
                if (event.getType() == Event.Type.PFD_TIMEOUT) {
                    pfd.onTimeout();
                } 
                else if (event.getType() == Event.Type.CRASH_DETECTED) {
                    String crashedNode = event.getPayload().getString("node");
                    rb.onCrash(crashedNode);
                } 
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
                        pfd.onHeartbeat(msg.getString("src"));
                    }
                    else if ("broadcast".equals(msgType)) {
                        int value = body.getInt("message");
                        messages.add(value);
                        
                        if (EAGER_MODE) {
                            // EAGER APP -> BEB
                            JSONObject payload = new JSONObject();
                            payload.put("message", value);
                            payload.put("original_sender", selfId);
                            beb.bebBcast(payload);
                        } else {
                            // LAZY APP -> RB
                            rb.rbBcast(value);
                        }

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
                        
                        if (EAGER_MODE) {
                            // EAGER: relay instantaneu prin BEB
                            if (!messages.contains(value)) {
                                messages.add(value);
                                beb.bebBcast(payload);
                            }
                        } else {
                            // LAZY: trimitem către RB pentru a respecta setul "correct" și vectorul "from"
                            String originalSender = payload.getString("original_sender");
                            String senderOfBeb = msg.getString("src");
                            boolean isNewMessage = rb.rbDeliver(senderOfBeb, originalSender, value);
                            
                            if (isNewMessage) {
                                messages.add(value);
                            }
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