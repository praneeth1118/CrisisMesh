import java.awt.*;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.List;                       // resolves clash between java.awt.List and java.util.List
import java.util.concurrent.*;
import java.util.function.Consumer;
import javax.swing.*;

/**
 * CrisisMesh - offline emergency communication network (single-file version).
 *
 * Build : javac CrisisMesh.java
 * Run   : java CrisisMesh <nodeName> <port> [host:port ...] [--gui]
 *
 * Demo (3 terminals, chain A - B - C):
 *   java CrisisMesh B 5002
 *   java CrisisMesh A 5001 localhost:5002
 *   java CrisisMesh C 5003 localhost:5002
 * Then type in A:  sos Building 4 collapsed   -> C receives it via B (hops=2).
 *
 * How it works:
 *  - Flooding           : every new message is forwarded to all neighbours.
 *  - Duplicate control  : unique message ID, each node handles an ID only once (stops loops).
 *  - TTL                : a message travels at most DEFAULT_TTL hops.
 *  - Store-and-forward  : if a neighbour is down, messages are queued and retried every 5 s.
 *  - Two-way links      : connecting A -> B makes B learn A automatically (HELLO handshake).
 */
public class CrisisMesh {

    // =====================================================================
    // Message model
    // =====================================================================
    static class Message {
        enum Type { SOS, RESOURCE_REQUEST, STATUS, INFO }

        final String id;        // globally unique -> duplicate suppression
        final String origin;    // node that created it
        final Type type;
        final String text;
        final long timestamp;
        final int ttl;          // remaining hops allowed
        final int hops;         // hops travelled so far

        Message(String id, String origin, Type type, String text, long timestamp, int ttl, int hops) {
            this.id = id; this.origin = origin; this.type = type; this.text = text;
            this.timestamp = timestamp; this.ttl = ttl; this.hops = hops;
        }

        Message forwarded() {
            return new Message(id, origin, type, text, timestamp, ttl - 1, hops + 1);
        }

        private static String b64(String s) {
            return Base64.getEncoder().encodeToString(s.getBytes(StandardCharsets.UTF_8));
        }
        private static String unb64(String s) {
            return new String(Base64.getDecoder().decode(s), StandardCharsets.UTF_8);
        }

        /** Wire format: id|origin|type|text|timestamp|ttl|hops (origin and text Base64-encoded). */
        String encode() {
            return String.join("|", id, b64(origin), type.name(), b64(text),
                    String.valueOf(timestamp), String.valueOf(ttl), String.valueOf(hops));
        }

        static Message decode(String line) {
            String[] p = line.split("\\|", -1);
            if (p.length != 7) throw new IllegalArgumentException("Bad message: " + line);
            return new Message(p[0], unb64(p[1]), Type.valueOf(p[2]), unb64(p[3]),
                    Long.parseLong(p[4]), Integer.parseInt(p[5]), Integer.parseInt(p[6]));
        }

        @Override
        public String toString() {
            return String.format("[%s] %s (hops=%d): %s", type, origin, hops, text);
        }
    }

    // =====================================================================
    // Mesh node: networking, flooding, dedupe, store-and-forward
    // =====================================================================
    static class MeshNode {
        static final int DEFAULT_TTL = 8;
        private static final int MAX_PENDING_PER_PEER = 200;
        private static final int RETRY_SECONDS = 5;

        private final String name;
        private final int port;

        private final Set<String> seen = ConcurrentHashMap.newKeySet();
        private final List<Message> inbox = new CopyOnWriteArrayList<>();
        private final Set<InetSocketAddress> neighbors = ConcurrentHashMap.newKeySet();
        private final Map<InetSocketAddress, Queue<Message>> pending = new ConcurrentHashMap<>();

        private final ExecutorService pool = Executors.newCachedThreadPool(daemon());
        private final ScheduledExecutorService retryTimer = Executors.newSingleThreadScheduledExecutor(daemon());
        private volatile Consumer<Message> listener = m -> {};
        private ServerSocket server;

        MeshNode(String name, int port) {
            this.name = name;
            this.port = port;
        }

        private static ThreadFactory daemon() {
            return r -> { Thread t = new Thread(r); t.setDaemon(true); return t; };
        }

        String getName() { return name; }
        int getPort() { return port; }
        void setListener(Consumer<Message> l) { this.listener = l; }
        List<Message> getInbox() { return new ArrayList<>(inbox); }
        Set<InetSocketAddress> getNeighbors() { return new HashSet<>(neighbors); }

        void start() throws IOException {
            server = new ServerSocket(port);
            pool.submit(() -> {
                while (!server.isClosed()) {
                    try {
                        Socket s = server.accept();
                        pool.submit(() -> handle(s));
                    } catch (IOException e) {
                        if (!server.isClosed()) System.err.println("accept error: " + e.getMessage());
                    }
                }
            });
            retryTimer.scheduleWithFixedDelay(this::drainPending, RETRY_SECONDS, RETRY_SECONDS, TimeUnit.SECONDS);
        }

        void stop() {
            try { if (server != null) server.close(); } catch (IOException ignored) {}
            retryTimer.shutdownNow();
            pool.shutdownNow();
        }

        /** Add a neighbour written as "host:port". */
        void addNeighbor(String hostPort) {
            int i = hostPort.lastIndexOf(':');
            if (i < 0) throw new IllegalArgumentException("Use host:port");
            String host = hostPort.substring(0, i);
            int p = Integer.parseInt(hostPort.substring(i + 1));
            neighbors.add(new InetSocketAddress(host, p));
        }

        /** Create a new message here and flood it into the mesh. */
        Message send(Message.Type type, String text) {
            Message m = new Message(UUID.randomUUID().toString(), name, type, text,
                    System.currentTimeMillis(), DEFAULT_TTL, 0);
            seen.add(m.id);
            inbox.add(m);
            listener.accept(m);
            flood(m.forwarded());
            return m;
        }

        // ---------- receiving ----------
        private void handle(Socket s) {
            try (s; BufferedReader in = new BufferedReader(
                    new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = in.readLine()) != null) {
                    if (line.startsWith("HELLO|")) {
                        int peerPort = Integer.parseInt(line.substring(6).trim());
                        neighbors.add(new InetSocketAddress(s.getInetAddress(), peerPort));
                    } else {
                        onReceive(Message.decode(line));
                    }
                }
            } catch (Exception e) {
                // a malformed or dropped connection must never crash the node
            }
        }

        private void onReceive(Message m) {
            if (!seen.add(m.id)) return;          // already handled -> drop (stops loops)
            inbox.add(m);
            listener.accept(m);
            if (m.ttl > 0) flood(m.forwarded());  // pass it on
        }

        // ---------- forwarding ----------
        private void flood(Message m) {
            if (m.ttl < 0) return;
            for (InetSocketAddress peer : neighbors) {
                pool.submit(() -> {
                    if (!trySend(peer, m)) queue(peer, m);
                });
            }
        }

        private boolean trySend(InetSocketAddress peer, Message m) {
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress(peer.getAddress(), peer.getPort()), 1500);
                Writer w = new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8);
                w.write("HELLO|" + port + "\n");
                w.write(m.encode() + "\n");
                w.flush();
                return true;
            } catch (Exception e) {
                return false;
            }
        }

        private void queue(InetSocketAddress peer, Message m) {
            Queue<Message> q = pending.computeIfAbsent(peer, k -> new ConcurrentLinkedQueue<>());
            if (q.size() < MAX_PENDING_PER_PEER) q.add(m);
        }

        /** Store-and-forward: retry messages that could not be delivered earlier. */
        private void drainPending() {
            for (Map.Entry<InetSocketAddress, Queue<Message>> e : pending.entrySet()) {
                Queue<Message> q = e.getValue();
                Message m;
                while ((m = q.peek()) != null) {
                    if (trySend(e.getKey(), m)) q.poll(); else break;   // peer still down
                }
            }
        }
    }

    // =====================================================================
    // Swing GUI
    // =====================================================================
    static void launchGui(MeshNode node) {
        SwingUtilities.invokeLater(() -> {
            JFrame f = new JFrame("CrisisMesh - " + node.getName() + " (port " + node.getPort() + ")");
            f.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

            JTextArea log = new JTextArea();
            log.setEditable(false);
            log.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 13));

            JComboBox<Message.Type> type = new JComboBox<>(Message.Type.values());
            JTextField input = new JTextField();
            JButton send = new JButton("Send");
            JButton peers = new JButton("Peers");

            Runnable doSend = () -> {
                String t = input.getText().trim();
                if (t.isEmpty()) return;
                node.send((Message.Type) type.getSelectedItem(), t);
                input.setText("");
            };
            send.addActionListener(e -> doSend.run());
            input.addActionListener(e -> doSend.run());
            peers.addActionListener(e -> JOptionPane.showMessageDialog(f,
                    node.getNeighbors().isEmpty() ? "No neighbours yet" : node.getNeighbors().toString()));

            node.setListener(m -> SwingUtilities.invokeLater(() -> {
                String time = new SimpleDateFormat("HH:mm:ss").format(new Date(m.timestamp));
                log.append(time + "  " + m + "\n");
                log.setCaretPosition(log.getDocument().getLength());
            }));

            JPanel bottom = new JPanel(new BorderLayout(5, 5));
            bottom.add(type, BorderLayout.WEST);
            bottom.add(input, BorderLayout.CENTER);
            JPanel btns = new JPanel(new GridLayout(1, 2, 5, 0));
            btns.add(send);
            btns.add(peers);
            bottom.add(btns, BorderLayout.EAST);
            bottom.setBorder(BorderFactory.createEmptyBorder(5, 5, 5, 5));

            f.add(new JScrollPane(log), BorderLayout.CENTER);
            f.add(bottom, BorderLayout.SOUTH);
            f.setSize(700, 450);
            f.setLocationByPlatform(true);
            f.setVisible(true);
        });
    }

    // =====================================================================
    // Command-line interface + entry point
    // =====================================================================
    static void runCli(MeshNode node) {
        node.setListener(m -> System.out.println("\n>> " + m));
        System.out.println("CrisisMesh node '" + node.getName() + "' listening on port " + node.getPort());
        System.out.println("Commands: sos <text> | help <text> | status <text> | info <text> | connect host:port | peers | inbox | quit");

        Scanner sc = new Scanner(System.in);
        while (true) {
            System.out.print(node.getName() + "> ");
            if (!sc.hasNextLine()) break;
            String line = sc.nextLine().trim();
            if (line.isEmpty()) continue;
            String[] parts = line.split("\\s+", 2);
            String cmd = parts[0].toLowerCase();
            String arg = parts.length > 1 ? parts[1] : "";
            try {
                switch (cmd) {
                    case "sos":     node.send(Message.Type.SOS, arg); break;
                    case "help":    node.send(Message.Type.RESOURCE_REQUEST, arg); break;
                    case "status":  node.send(Message.Type.STATUS, arg); break;
                    case "info":    node.send(Message.Type.INFO, arg); break;
                    case "connect": node.addNeighbor(arg); System.out.println("Neighbour added."); break;
                    case "peers":   System.out.println(node.getNeighbors()); break;
                    case "inbox":   node.getInbox().forEach(System.out::println); break;
                    case "quit":    node.stop(); return;
                    default:        System.out.println("Unknown command.");
                }
            } catch (Exception e) {
                System.out.println("Error: " + e.getMessage());
            }
        }
        node.stop();
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage: java CrisisMesh <nodeName> <port> [host:port ...] [--gui]");
            return;
        }
        String name = args[0];
        int port = Integer.parseInt(args[1]);
        boolean gui = false;
        List<String> peers = new ArrayList<>();
        for (int i = 2; i < args.length; i++) {
            if (args[i].equals("--gui")) gui = true; else peers.add(args[i]);
        }

        MeshNode node = new MeshNode(name, port);
        node.start();
        for (String p : peers) node.addNeighbor(p);

        if (gui) launchGui(node); else runCli(node);
    }
}
