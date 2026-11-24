package com.soc.reducer;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * SimulationServer: High-Throughput Java 21 Virtual Thread Notification Dispatcher
 * and Java NIO WebSocket Telemetry Server.
 * 
 * <p>Architectural Responsibilities:
 * <ul>
 *   <li><b>Project Loom Virtual Thread Ingestion:</b> Dispatches high-volume incoming notification
 *       streams across lightweight virtual threads without thread-pool exhaustion or OS blocking.</li>
 *   <li><b>Full Algorithmic Pipeline Integration:</b>
 *       Raw Event &rarr; {@link TopicTrie#match(String)} &rarr; {@link O1Reducer#ingest(Notification, RuleMetadata)}
 *       &rarr; {@link PriorityMaxHeap#insertOrUpdate(AlertNode)}.</li>
 *   <li><b>Java NIO WebSocket Engine:</b> Zero-dependency RFC 6455 compliant WebSocket and HTTP server
 *       streaming telemetry, waterfall logs, and heap-ranked alert matrices at 50+ messages per second.</li>
 *   <li><b>Interactive Bidirectional Control:</b> Accepts client control frames to trigger P0 cyber attacks,
 *       WhatsApp spam storms, ingestion throttling, or buffer purges.</li>
 * </ul>
 * </p>
 */
public class SimulationServer {

    private static final int DEFAULT_PORT = 8080;
    private static final String WS_MAGIC_KEY = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

    // Core Data Structures
    private final TopicTrie topicTrie;
    private final O1Reducer reducer;
    private final PriorityMaxHeap priorityHeap;

    // Concurrency & Simulation State
    private final ExecutorService virtualThreadExecutor;
    private final AtomicBoolean isRunning;
    private final AtomicBoolean isSimulationPaused;
    private final AtomicInteger targetEventsPerSecond;

    // Rate Tracking
    private final AtomicLong rawEventsCounter;
    private final AtomicLong reducedEventsCounter;
    private volatile long currentRawRate;
    private volatile long currentReducedRate;

    // Waterfall Log Buffer (bounded thread-safe queue)
    private final ConcurrentLinkedDeque<Notification> recentRawLogs;
    private static final int MAX_RAW_LOGS = 100;

    // NIO WebSocket Connections
    private final Set<SocketChannel> activeWebSockets;
    private final Path staticResourceRoot;

    /**
     * Initializes the SimulationServer with core data structures and virtual thread executors.
     * 
     * Time Complexity: O(1)
     * Space Complexity: O(1)
     */
    public SimulationServer() {
        this.topicTrie = TopicTrie.createDefaultSOCEngine();
        this.reducer = new O1Reducer(10_000);
        this.priorityHeap = new PriorityMaxHeap(128);

        this.virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();
        this.isRunning = new AtomicBoolean(true);
        this.isSimulationPaused = new AtomicBoolean(false);
        this.targetEventsPerSecond = new AtomicInteger(800); // 800 msgs/sec default

        this.rawEventsCounter = new AtomicLong(0);
        this.reducedEventsCounter = new AtomicLong(0);
        this.currentRawRate = 0;
        this.currentReducedRate = 0;

        this.recentRawLogs = new ConcurrentLinkedDeque<>();
        this.activeWebSockets = ConcurrentHashMap.newKeySet();
        this.staticResourceRoot = Path.of(".").toAbsolutePath().normalize();
    }

    /**
     * Entry point to launch the SimulationServer.
     */
    public static void main(String[] args) {
        int port = DEFAULT_PORT;
        if (args.length > 0) {
            try {
                port = Integer.parseInt(args[0]);
            } catch (NumberFormatException ignored) {}
        }

        SimulationServer server = new SimulationServer();
        server.start(port);
    }

    /**
     * Starts the Java NIO Server, background virtual thread generators, and 50Hz telemetry broadcaster.
     * 
     * @param port Port to bind the HTTP/WebSocket server to.
     */
    public void start(int port) {
        System.out.println("================================================================================");
        System.out.println(" AGY-SOC :: NOTIFICATION PRIORITY REDUCER [JAVA 21 & ADVANCED DATA STRUCTURES]");
        System.out.println("================================================================================");
        System.out.printf(" >> Java Runtime: %s (PID: %d)%n", System.getProperty("java.version"), ProcessHandle.current().pid());
        System.out.printf(" >> Virtual Threads (Project Loom): ENABLED%n");
        System.out.printf(" >> TopicTrie Rules Loaded: %d rules%n", topicTrie.getRuleCount());
        System.out.printf(" >> O(1) Reducer Capacity: 10,000 slots%n");
        System.out.printf(" >> PriorityMaxHeap: Inverted-Hash Rebalancing Active%n");
        System.out.println("--------------------------------------------------------------------------------");

        // 1. Launch Background Telemetry Calculation Task (Every 1 Second)
        virtualThreadExecutor.submit(this::runRateCalculatorLoop);

        // 2. Launch Ingestion Simulation Stream Tasks
        startSimulationWorkers();

        // 3. Launch 50Hz WebSocket Broadcaster Task (Every 20ms)
        virtualThreadExecutor.submit(this::runTelemetryBroadcasterLoop);

        // 4. Start Java NIO Server Loop (Main Thread)
        runNioServer(port);
    }

    /**
     * Ingestion pipeline: Processes a single notification through the entire 3-tier
     * data structure pipeline.
     * 
     * Pipeline flow:
     * 1. TopicTrie.match() in O(k) time -> assigns base priority weight & metadata.
     * 2. O1Reducer.ingest() in O(1) time -> deduplicates or adds to DLL head.
     * 3. PriorityMaxHeap.insertOrUpdate() in O(log n) time -> rebalances heap.
     * 
     * Time Complexity: O(k + log n) total per notification.
     * Space Complexity: O(1) amortized.
     */
    public void processNotification(Notification notification) {
        rawEventsCounter.incrementAndGet();

        // Circular buffer for waterfall display
        recentRawLogs.addFirst(notification);
        while (recentRawLogs.size() > MAX_RAW_LOGS) {
            recentRawLogs.pollLast();
        }

        // 1. TopicTrie Rule Match: O(k)
        RuleMetadata rule = topicTrie.match(notification.routingKey());

        // 2. O(1) Reducer Ingestion: O(1)
        O1Reducer.ReductionResult result = reducer.ingest(notification, rule);

        // 3. MaxHeap Inverted-Index Priority Update: O(log n)
        priorityHeap.insertOrUpdate(result.node());
        reducedEventsCounter.incrementAndGet();
    }

    /**
     * Virtual Thread Workers simulating diverse real-world notification traffic firehoses.
     */
    private void startSimulationWorkers() {
        // Worker 1: WhatsApp Group & Spam Traffic (Massive burst duplicate streams)
        virtualThreadExecutor.submit(() -> runSimulatedFirehose("WHATSAPP_WORKER", this::generateWhatsAppStream));

        // Worker 2: Infrastructure & K8s Cluster Monitoring (Periodic & Flapping)
        virtualThreadExecutor.submit(() -> runSimulatedFirehose("INFRA_WORKER", this::generateInfraStream));

        // Worker 3: Cyber Defense & SOC Intrusion Detection (High-priority breaches)
        virtualThreadExecutor.submit(() -> runSimulatedFirehose("CYBER_WORKER", this::generateCyberDefenseStream));

        // Worker 4: IoT & Edge Telemetry (Continuous steady-state pings)
        virtualThreadExecutor.submit(() -> runSimulatedFirehose("IOT_WORKER", this::generateIotStream));
    }

    private void runSimulatedFirehose(String workerName, Runnable streamGenerator) {
        while (isRunning.get()) {
            if (isSimulationPaused.get()) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignored) {}
                continue;
            }

            streamGenerator.run();

            // Pacing based on targetEventsPerSecond
            int targetRate = Math.max(10, targetEventsPerSecond.get());
            long sleepNanos = (1_000_000_000L / targetRate) * 4; // 4 parallel workers
            long sleepMillis = sleepNanos / 1_000_000L;
            int remainNanos = (int) (sleepNanos % 1_000_000L);

            try {
                if (sleepMillis > 0 || remainNanos > 0) {
                    Thread.sleep(sleepMillis, remainNanos);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    // Generator 1: WhatsApp Bulk & Duplicate Spam
    private final String[] whatsappGroups = {
            "Crypto Alpha Moonshot VIP", "Global Family Chat 2026", "Apartment Community Board",
            "College Alumni Batch 2024", "Flash Sale Daily Deals"
    };
    private final String[] whatsappSpamPayloads = {
            "BUY 500x GEM NOW! Telegram link: t.me/cryptopump",
            "Good morning family! Have a blessed day ahead :)",
            "Reminder: Maintenance fees are due by end of month.",
            "Can someone share notes for Distributed Systems Exam?",
            "Flash 90% discount on mechanical keyboards! Click here."
    };

    private void generateWhatsAppStream() {
        int idx = ThreadLocalRandom.current().nextInt(whatsappGroups.length);
        String group = whatsappGroups[idx];
        String text = whatsappSpamPayloads[idx];
        String id = UUID.randomUUID().toString().substring(0, 8);
        String routing = (idx == 0 || idx == 4) ? "comm.whatsapp.spam" : "comm.whatsapp.group";

        Notification notif = Notification.of(
                id, "whatsapp-gw-node-" + (idx % 2 + 1), routing,
                "Msg in [" + group + "]", text, System.currentTimeMillis()
        );
        processNotification(notif);
    }

    // Generator 2: Infrastructure Events
    private final String[] infraHosts = {"prod-k8s-worker-01", "db-primary-us-east", "edge-proxy-04"};
    private void generateInfraStream() {
        int r = ThreadLocalRandom.current().nextInt(100);
        String routing;
        String title;
        String payload;

        if (r < 20) {
            routing = "infra.db.deadlock";
            title = "PostgreSQL Lock Contention on pg_catalog";
            payload = "Deadlock detected between transaction 849204 and 849209 on table 'payments'";
        } else if (r < 50) {
            routing = "infra.k8s.oomkill";
            title = "Pod cgroup memory limit exceeded (1024MB)";
            payload = "Container 'auth-gateway' terminated with OOMKilled status code 137";
        } else if (r < 80) {
            routing = "sys.cpu.high";
            title = "Node CPU Throttling Alert";
            payload = "Worker node CPU utilization spiked to 92.4% over 3m window";
        } else {
            routing = "sys.disk.full";
            title = "Volume /var/log Disk Pressure";
            payload = "Available disk storage dropped below threshold: 4.8% remaining";
        }

        String host = infraHosts[ThreadLocalRandom.current().nextInt(infraHosts.length)];
        Notification notif = Notification.of(
                UUID.randomUUID().toString().substring(0, 8), host, routing,
                title, payload, System.currentTimeMillis()
        );
        processNotification(notif);
    }

    // Generator 3: Cyber Defense Threat Stream
    private final String[] attackerIps = {"198.51.100.42", "203.0.113.89", "192.0.2.14", "185.220.101.5"};
    private void generateCyberDefenseStream() {
        int r = ThreadLocalRandom.current().nextInt(100);
        if (r > 35) return; // Cyber threats are high impact, slightly lower frequency

        String routing;
        String title;
        String payload;
        String ip = attackerIps[ThreadLocalRandom.current().nextInt(attackerIps.length)];

        if (r < 8) {
            routing = "sec.rce.exploit";
            title = "Remote Code Execution Signature Match";
            payload = "Payload match CVE-2026-4401 on /api/v2/ingest from IP " + ip;
        } else if (r < 18) {
            routing = "sec.auth.bruteforce";
            title = "SSH / Admin Credential Stuffing";
            payload = "Failed password authentication attempt #412 for user 'root' from " + ip;
        } else if (r < 28) {
            routing = "sec.ddos.synflood";
            title = "Edge Ingress SYN Flood Volume Warning";
            payload = "Border gateway packet rate breached 450,000 pps from source prefix " + ip;
        } else {
            routing = "sec.firewall.drop";
            title = "Perimeter Security Rule Violation";
            payload = "Disallowed port scan detected on port range 8000-9000 from " + ip;
        }

        Notification notif = Notification.of(
                UUID.randomUUID().toString().substring(0, 8), "firewall-soc-core", routing,
                title, payload, System.currentTimeMillis()
        );
        processNotification(notif);
    }

    // Generator 4: IoT & Keep-Alives
    private void generateIotStream() {
        int r = ThreadLocalRandom.current().nextInt(100);
        String routing = (r < 15) ? "iot.sensor.temp.critical" : ((r < 50) ? "sys.battery.low" : "telemetry.heartbeat");
        String title = (r < 15) ? "Rack 4B Thermal Warning" : ((r < 50) ? "Drone Node Battery Warning" : "Health Ping");
        String payload = "Telemetry reading value=" + (ThreadLocalRandom.current().nextDouble() * 100.0);

        Notification notif = Notification.of(
                UUID.randomUUID().toString().substring(0, 8), "iot-mesh-controller", routing,
                title, payload, System.currentTimeMillis()
        );
        processNotification(notif);
    }

    /**
     * Injects an instantaneous high-priority P0 Cyber Attack wave into the pipeline.
     */
    public void injectP0Burst(int count) {
        for (int i = 0; i < count; i++) {
            virtualThreadExecutor.submit(() -> {
                Notification notif = Notification.of(
                        UUID.randomUUID().toString().substring(0, 8),
                        "border-firewall-01",
                        "sec.auth.bruteforce",
                        "CRITICAL: Distributed SSH Brute Force Cluster Breach",
                        "Attacker IP botnet cluster 185.220.101.0/24 conducting coordinated dictionary attack",
                        System.currentTimeMillis()
                );
                processNotification(notif);
            });
        }
    }

    /**
     * Injects a massive WhatsApp spam flood (hundreds of duplicate messages with identical key).
     */
    public void injectWhatsAppSpamFlood(int count) {
        String fixedId = "wp-spam-burst";
        String group = "Crypto Pump & Dump VIP Channel";
        String payload = "🚨 URGENT: Buy token $MOON now before 1000x listing on Binance! Link: spam.xyz/moon";

        for (int i = 0; i < count; i++) {
            virtualThreadExecutor.submit(() -> {
                Notification notif = Notification.of(
                        fixedId,
                        "whatsapp-gw-01",
                        "comm.whatsapp.spam",
                        "Msg in [" + group + "]",
                        payload,
                        System.currentTimeMillis()
                );
                processNotification(notif);
            });
        }
    }

    /**
     * 1-Second Metric Calculation Loop: Computes live throughput rates (msgs/sec).
     */
    private void runRateCalculatorLoop() {
        long lastRaw = 0;
        long lastReduced = 0;

        while (isRunning.get()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                break;
            }

            long currentRaw = rawEventsCounter.get();
            long currentReduced = reducedEventsCounter.get();

            currentRawRate = currentRaw - lastRaw;
            currentReducedRate = currentReduced - lastReduced;

            lastRaw = currentRaw;
            lastReduced = currentReduced;
        }
    }

    /**
     * 50Hz (20ms interval) WebSocket Telemetry Broadcaster.
     * Streams high-density JSON updates to all connected browser SOC clients.
     */
    private void runTelemetryBroadcasterLoop() {
        while (isRunning.get()) {
            try {
                Thread.sleep(20); // 50 updates per second
            } catch (InterruptedException e) {
                break;
            }

            if (activeWebSockets.isEmpty()) {
                continue;
            }

            String jsonPayload = buildTelemetryJson();
            byte[] frameBytes = encodeWebSocketTextFrame(jsonPayload);
            ByteBuffer buffer = ByteBuffer.wrap(frameBytes);

            for (SocketChannel client : activeWebSockets) {
                if (client.isOpen()) {
                    try {
                        buffer.rewind();
                        client.write(buffer);
                    } catch (IOException e) {
                        try {
                            client.close();
                        } catch (IOException ignored) {}
                        activeWebSockets.remove(client);
                    }
                }
            }
        }
    }

    /**
     * Assembles the telemetry snapshot JSON string.
     */
    private String buildTelemetryJson() {
        List<AlertNode> topAlerts = priorityHeap.getTopK(18);
        List<Notification> rawLogsSnapshot = new ArrayList<>(recentRawLogs);
        if (rawLogsSnapshot.size() > 25) {
            rawLogsSnapshot = rawLogsSnapshot.subList(0, 25);
        }

        StringBuilder sb = new StringBuilder(4096);
        sb.append("{");
        sb.append("\"type\":\"TELEMETRY_TICK\",");
        sb.append("\"timestamp\":").append(System.currentTimeMillis()).append(",");

        // Metrics Object
        sb.append("\"metrics\":{");
        sb.append("\"rawRate\":").append(currentRawRate).append(",");
        sb.append("\"reducedRate\":").append(currentReducedRate).append(",");
        sb.append("\"dedupEfficiency\":").append(String.format(Locale.US, "%.2f", reducer.getDeduplicationEfficiency())).append(",");
        sb.append("\"totalRaw\":").append(rawEventsCounter.get()).append(",");
        sb.append("\"totalDuplicates\":").append(reducer.getTotalDuplicatesCollapsed()).append(",");
        sb.append("\"heapDepth\":").append(priorityHeap.size()).append(",");
        sb.append("\"activeNodes\":").append(reducer.size()).append(",");
        sb.append("\"virtualThreads\":").append(Thread.activeCount()).append(",");
        sb.append("\"targetIngestRate\":").append(targetEventsPerSecond.get()).append(",");
        sb.append("\"isPaused\":").append(isSimulationPaused.get());
        sb.append("},");

        // Top Alerts Array (Ranked by PriorityMaxHeap)
        sb.append("\"topAlerts\":[");
        for (int i = 0; i < topAlerts.size(); i++) {
            if (i > 0) sb.append(",");
            AlertNode node = topAlerts.get(i);
            sb.append("{");
            sb.append("\"rank\":").append(i + 1).append(",");
            sb.append("\"id\":\"").append(escapeJson(node.getId())).append("\",");
            sb.append("\"dedupKey\":\"").append(escapeJson(node.getDeduplicationKey())).append("\",");
            sb.append("\"source\":\"").append(escapeJson(node.getSource())).append("\",");
            sb.append("\"routingKey\":\"").append(escapeJson(node.getRoutingKey())).append("\",");
            sb.append("\"title\":\"").append(escapeJson(node.getTitle())).append("\",");
            sb.append("\"payload\":\"").append(escapeJson(node.getPayload())).append("\",");
            sb.append("\"category\":\"").append(escapeJson(node.getCategory())).append("\",");
            sb.append("\"severity\":\"").append(escapeJson(node.getSeverity())).append("\",");
            sb.append("\"score\":").append(String.format(Locale.US, "%.1f", node.getDynamicPriority())).append(",");
            sb.append("\"frequency\":").append(node.getFrequency()).append(",");
            sb.append("\"firstSeen\":").append(node.getFirstSeenMs()).append(",");
            sb.append("\"lastSeen\":").append(node.getLastSeenMs()).append(",");
            sb.append("\"justUpdated\":").append(node.isJustUpdated());
            sb.append("}");

            // Clear transient update flag after snapshotting
            node.clearUpdatedFlag();
        }
        sb.append("],");

        // Recent Raw Logs Array (Waterfall)
        sb.append("\"recentRawLogs\":[");
        for (int i = 0; i < rawLogsSnapshot.size(); i++) {
            if (i > 0) sb.append(",");
            Notification n = rawLogsSnapshot.get(i);
            sb.append("{");
            sb.append("\"id\":\"").append(escapeJson(n.id())).append("\",");
            sb.append("\"source\":\"").append(escapeJson(n.source())).append("\",");
            sb.append("\"routingKey\":\"").append(escapeJson(n.routingKey())).append("\",");
            sb.append("\"title\":\"").append(escapeJson(n.title())).append("\",");
            sb.append("\"payload\":\"").append(escapeJson(n.payload())).append("\",");
            sb.append("\"timestamp\":").append(n.timestamp());
            sb.append("}");
        }
        sb.append("]");

        sb.append("}");
        return sb.toString();
    }

    private static String escapeJson(String input) {
        if (input == null) return "";
        return input.replace("\\", "\\\\")
                    .replace("\"", "\\\"")
                    .replace("\b", "\\b")
                    .replace("\f", "\\f")
                    .replace("\n", "\\n")
                    .replace("\r", "\\r")
                    .replace("\t", "\\t");
    }

    /**
     * Java NIO Server: Handles both HTTP Static Requests and WebSocket RFC 6455 Handshakes.
     */
    private void runNioServer(int port) {
        try (ServerSocketChannel serverChannel = ServerSocketChannel.open()) {
            serverChannel.configureBlocking(true); // Virtual thread per connection
            serverChannel.bind(new InetSocketAddress(port));
            System.out.printf(" >> SOC Control Center HTTP & WebSocket Gateway bound to: http://localhost:%d/%n", port);
            System.out.printf(" >> Live WebSocket Stream Endpoint: ws://localhost:%d/ws%n", port);
            System.out.println("================================================================================");
            System.out.println(" Ready. Open your web browser at http://localhost:" + port + "/ to monitor the SOC.");
            System.out.println("================================================================================");

            while (isRunning.get()) {
                SocketChannel clientChannel = serverChannel.accept();
                virtualThreadExecutor.submit(() -> handleClientConnection(clientChannel));
            }
        } catch (IOException e) {
            System.err.println("Fatal NIO Server Exception: " + e.getMessage());
        }
    }

    /**
     * Handles an incoming client connection: performs HTTP file serving or WebSocket upgrade.
     */
    private void handleClientConnection(SocketChannel client) {
        try {
            client.configureBlocking(true);
            ByteBuffer buffer = ByteBuffer.allocate(8192);
            int bytesRead = client.read(buffer);
            if (bytesRead <= 0) {
                client.close();
                return;
            }

            buffer.flip();
            String request = StandardCharsets.UTF_8.decode(buffer).toString();
            String[] lines = request.split("\r\n");
            if (lines.length == 0) {
                client.close();
                return;
            }

            String requestLine = lines[0];
            String[] parts = requestLine.split(" ");
            if (parts.length < 2) {
                client.close();
                return;
            }

            String method = parts[0];
            String path = parts[1];

            // Check if this is a WebSocket Handshake request
            boolean isWebSocket = false;
            String wsKey = null;

            for (String line : lines) {
                String lower = line.toLowerCase();
                if (lower.startsWith("upgrade:") && lower.contains("websocket")) {
                    isWebSocket = true;
                }
                if (lower.startsWith("sec-websocket-key:")) {
                    wsKey = line.substring(line.indexOf(":") + 1).trim();
                }
            }

            if (isWebSocket && wsKey != null) {
                // Perform RFC 6455 Handshake
                performWebSocketHandshake(client, wsKey);
                activeWebSockets.add(client);
                handleWebSocketSession(client);
            } else if ("GET".equalsIgnoreCase(method)) {
                // Serve Static HTTP Files
                serveStaticFile(client, path);
            } else {
                sendHttpResponse(client, 405, "Method Not Allowed", "text/plain", "Method Not Allowed".getBytes());
            }
        } catch (Exception e) {
            try {
                client.close();
            } catch (IOException ignored) {}
        }
    }

    /**
     * Executes RFC 6455 WebSocket Handshake.
     */
    private void performWebSocketHandshake(SocketChannel client, String wsKey) throws Exception {
        String acceptInput = wsKey + WS_MAGIC_KEY;
        MessageDigest md = MessageDigest.getInstance("SHA-1");
        byte[] digest = md.digest(acceptInput.getBytes(StandardCharsets.UTF_8));
        String acceptKey = Base64.getEncoder().encodeToString(digest);

        String response = "HTTP/1.1 101 Switching Protocols\r\n" +
                          "Upgrade: websocket\r\n" +
                          "Connection: Upgrade\r\n" +
                          "Sec-WebSocket-Accept: " + acceptKey + "\r\n" +
                          "\r\n";

        client.write(ByteBuffer.wrap(response.getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * Serves static web frontend files (index.html, style.css, app.js).
     */
    private void serveStaticFile(SocketChannel client, String requestPath) throws IOException {
        String cleanPath = requestPath.split("\\?")[0];
        if ("/".equals(cleanPath)) {
            cleanPath = "/index.html";
        }

        // Strip leading slash
        if (cleanPath.startsWith("/")) {
            cleanPath = cleanPath.substring(1);
        }

        Path target = staticResourceRoot.resolve(cleanPath).normalize();
        if (!target.startsWith(staticResourceRoot) || !Files.exists(target) || Files.isDirectory(target)) {
            sendHttpResponse(client, 404, "Not Found", "text/plain", ("404 File Not Found: " + cleanPath).getBytes());
            return;
        }

        String contentType = "text/plain";
        if (cleanPath.endsWith(".html")) contentType = "text/html; charset=UTF-8";
        else if (cleanPath.endsWith(".css")) contentType = "text/css; charset=UTF-8";
        else if (cleanPath.endsWith(".js")) contentType = "application/javascript; charset=UTF-8";
        else if (cleanPath.endsWith(".json")) contentType = "application/json";
        else if (cleanPath.endsWith(".ico")) contentType = "image/x-icon";

        byte[] content = Files.readAllBytes(target);
        sendHttpResponse(client, 200, "OK", contentType, content);
    }

    private void sendHttpResponse(SocketChannel client, int statusCode, String statusText, String contentType, byte[] body) throws IOException {
        String header = "HTTP/1.1 " + statusCode + " " + statusText + "\r\n" +
                        "Content-Type: " + contentType + "\r\n" +
                        "Content-Length: " + body.length + "\r\n" +
                        "Access-Control-Allow-Origin: *\r\n" +
                        "Connection: close\r\n\r\n";

        ByteBuffer headerBuf = ByteBuffer.wrap(header.getBytes(StandardCharsets.UTF_8));
        ByteBuffer bodyBuf = ByteBuffer.wrap(body);

        while (headerBuf.hasRemaining()) client.write(headerBuf);
        while (bodyBuf.hasRemaining()) client.write(bodyBuf);
        client.close();
    }

    /**
     * Handles inbound WebSocket frames from the frontend client (e.g. user triggers P0 attack).
     */
    private void handleWebSocketSession(SocketChannel client) {
        ByteBuffer buffer = ByteBuffer.allocate(16384);
        try {
            while (client.isOpen() && isRunning.get()) {
                buffer.clear();
                int read = client.read(buffer);
                if (read <= 0) break;

                buffer.flip();
                String message = decodeWebSocketFrame(buffer);
                if (message != null && !message.isEmpty()) {
                    handleClientCommand(message);
                }
            }
        } catch (IOException ignored) {
        } finally {
            activeWebSockets.remove(client);
            try {
                client.close();
            } catch (IOException ignored) {}
        }
    }

    /**
     * Parses client commands received via WebSocket.
     */
    private void handleClientCommand(String message) {
        try {
            if (message.contains("INJECT_P0")) {
                injectP0Burst(25);
            } else if (message.contains("WHATSAPP_SPAM")) {
                injectWhatsAppSpamFlood(150);
            } else if (message.contains("TOGGLE_PAUSE")) {
                isSimulationPaused.set(!isSimulationPaused.get());
            } else if (message.contains("SET_RATE")) {
                // e.g. {"action":"SET_RATE","rate":1200}
                int idx = message.indexOf("\"rate\":");
                if (idx != -1) {
                    int end = message.indexOf("}", idx);
                    String rateStr = message.substring(idx + 7, end != -1 ? end : message.length()).trim();
                    int rate = Integer.parseInt(rateStr.replaceAll("[^0-9]", ""));
                    targetEventsPerSecond.set(Math.max(10, Math.min(5000, rate)));
                }
            } else if (message.contains("CLEAR_BUFFERS")) {
                reducer.clear();
                priorityHeap.clear();
                recentRawLogs.clear();
                rawEventsCounter.set(0);
                reducedEventsCounter.set(0);
            }
        } catch (Exception e) {
            System.err.println("Error processing client command: " + e.getMessage());
        }
    }

    /**
     * Decodes an inbound WebSocket frame (handles masking).
     */
    private String decodeWebSocketFrame(ByteBuffer buffer) {
        if (buffer.remaining() < 2) return null;

        byte b1 = buffer.get();
        int opcode = b1 & 0x0F;
        if (opcode == 0x08) { // Close frame
            return null;
        }

        byte b2 = buffer.get();
        boolean masked = (b2 & 0x80) != 0;
        int payloadLen = b2 & 0x7F;

        if (payloadLen == 126) {
            if (buffer.remaining() < 2) return null;
            payloadLen = buffer.getShort() & 0xFFFF;
        } else if (payloadLen == 127) {
            if (buffer.remaining() < 8) return null;
            payloadLen = (int) buffer.getLong();
        }

        byte[] mask = new byte[4];
        if (masked) {
            if (buffer.remaining() < 4) return null;
            buffer.get(mask);
        }

        if (buffer.remaining() < payloadLen) return null;

        byte[] payload = new byte[payloadLen];
        buffer.get(payload);

        if (masked) {
            for (int i = 0; i < payloadLen; i++) {
                payload[i] = (byte) (payload[i] ^ mask[i % 4]);
            }
        }

        return new String(payload, StandardCharsets.UTF_8);
    }

    /**
     * Encodes a UTF-8 text string into an unmasked RFC 6455 WebSocket frame sent to the browser.
     */
    private static byte[] encodeWebSocketTextFrame(String text) {
        byte[] payload = text.getBytes(StandardCharsets.UTF_8);
        int len = payload.length;

        ByteArrayOutputStream baos = new ByteArrayOutputStream(len + 10);
        baos.write(0x81); // FIN = 1, Opcode = 1 (Text)

        if (len <= 125) {
            baos.write(len);
        } else if (len <= 65535) {
            baos.write(126);
            baos.write((len >> 8) & 0xFF);
            baos.write(len & 0xFF);
        } else {
            baos.write(127);
            for (int i = 7; i >= 0; i--) {
                baos.write((int) ((len >> (8 * i)) & 0xFF));
            }
        }

        baos.write(payload, 0, len);
        return baos.toByteArray();
    }
}
