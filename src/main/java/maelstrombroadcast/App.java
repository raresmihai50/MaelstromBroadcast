package maelstrombroadcast;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.json.JSONObject;

/**
 * Universitatea Babeș-Bolyai
 * Proiect: Reliable Broadcast Node
 * Nume: Ghiurau Rares Mihai
 * Grupa: 244
 */
public class App {
    public static void main(String[] args) {
        EventProcessor processor = new EventProcessor();
        Thread processorThread = new Thread(processor);
        processorThread.start();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
            String line;
            while ((line = reader.readLine()) != null) {
                JSONObject msg = new JSONObject(line);
                JSONObject body = msg.getJSONObject("body");
                String msgType = body.getString("type");

                if ("init".equals(msgType)) {
                    String selfId = body.getString("node_id");
                    
                    // Extragem lista de noduri
                    org.json.JSONArray nodesArray = body.getJSONArray("node_ids");
                    java.util.List<String> allNodes = new java.util.ArrayList<>();
                    for (int i = 0; i < nodesArray.length(); i++) {
                        allNodes.add(nodesArray.getString(i));
                    }

                    processor.init(selfId, allNodes);

                    // Construim răspunsul de init_ok pentru Maelstrom
                    JSONObject replyBody = new JSONObject();
                    replyBody.put("type", "init_ok");
                    replyBody.put("in_reply_to", body.getInt("msg_id"));

                    JSONObject reply = new JSONObject();
                    reply.put("src", selfId);
                    reply.put("dest", msg.getString("src"));
                    reply.put("body", replyBody);

                    System.out.println(reply.toString());
                } else {
                    processor.submitEvent(new Event(Event.Type.STDIN_MESSAGE, msg));
                }
            }
        } catch (Exception e) {
            System.err.println("Eroare fatala: " + e.getMessage());
        }
    }
}