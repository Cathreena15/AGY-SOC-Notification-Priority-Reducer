# AGY-SOC // Notification Priority Reducer
### Advanced Java 21+ Distributed Systems & Custom Algorithmic Data Structures

---

## 1. Executive Summary & Objective

The **Notification Priority Reducer** is a mission-critical distributed systems architecture designed to ingest, deduplicate, and prioritize massive real-time streams of notifications (e.g., automated WhatsApp spam storms, IoT sensor spikes, infrastructure flapping, and zero-day cyber defense alerts).

Built from first principles in **Java 21+**, the core engine strictly avoids high-level collection shortcuts, featuring hand-crafted algorithmic data structures with explicit time and space complexity guarantees:
1. **$O(1)$ Reducer:** Custom thread-safe `DoublyLinkedList` paired with a `ConcurrentHashMap` for constant-time deduplication, recency promotion, and eviction.
2. **$O(\log n)$ Priority Engine:** Custom array-backed `PriorityMaxHeap` featuring an **Inverted Hash Index** (`keyToIndex`) that elevates standard $O(n)$ search to an instantaneous $O(1)$ lookup and $O(\log n)$ in-place rebalance, guarded by a `ReentrantLock`.
3. **$O(k)$ Rule Engine:** Character-by-character `TopicTrie` (Prefix Tree) implementing Longest Prefix Matching (LPM) on hierarchical routing keys (e.g., `sys.battery.low`, `sec.auth.bruteforce`).
4. **Concurrency & Networking:** Java 21 **Virtual Threads (Project Loom)** dispatching thousands of concurrent notifications without OS blocking, coupled with a pure **Java NIO RFC 6455 WebSocket Server** streaming telemetry at 50+ frames per second.
5. **Palantir SOC Cyber Dashboard:** High-density 2D Security Operations Center interface rendered in OLED True Black (`#000000`), complete with real-time 60-second Canvas sparklines, waterfall JSON logs, and **CSS FLIP (First-Last-Invert-Play)** animations.

---

## 2. Algorithmic Complexity Matrix

| Component | Operation | Time Complexity | Space Complexity | Description / Invariant |
| :--- | :--- | :--- | :--- | :--- |
| **TopicTrie** | `insertRule(pattern, meta)` | $O(k)$ | $O(k)$ | Inserts rule pattern character-by-character where $k$ is pattern length |
| **TopicTrie** | `match(routingKey)` | $O(k)$ | $O(1)$ | Longest Prefix Matching (LPM) traversing trie nodes character-by-character |
| **O1Reducer** | `ingest(notification, rule)` | $O(1)$ avg | $O(1)$ | Map lookup + counter increment + DLL move-to-head or eviction |
| **O1Reducer** | `moveToHead(node)` | $O(1)$ | $O(1)$ | Splices node pointers using sentinel `dummyHead` and `dummyTail` |
| **O1Reducer** | `evictTail()` | $O(1)$ | $O(1)$ | Unlinks least recently updated node and removes from hash map |
| **PriorityMaxHeap** | `insertOrUpdate(node)` | $O(\log n)$ | $O(1)$ amortized | $O(1)$ hash index lookup; in-place sift-up/down rebalancing |
| **PriorityMaxHeap** | `extractMax()` | $O(\log n)$ | $O(1)$ | Removes root, places last element at root, sifts down |
| **PriorityMaxHeap** | `peekMax()` | $O(1)$ | $O(1)$ | Inspects root element index 0 |
| **PriorityMaxHeap** | `getTopK(k)` | $O(k \log n)$ | $O(k)$ | Non-destructive snapshot of top $K$ alerts on transient array clone |

---

## 3. Data Structure Architecture Deep Dive

### 3.1. Data Structure 1: The O(1) Reducer (`O1Reducer.java`)
- **Challenge:** In high-volume notification storms (e.g., 200 duplicate WhatsApp messages arriving within 100ms), linear iteration degrades system throughput exponentially.
- **Solution:** 
  - An internal `ConcurrentHashMap<String, AlertNode>` indexes alerts by a composite deduplication key (`source|routingKey|titleHash`).
  - Paired with a custom `DoublyLinkedList` maintained with sentinel nodes `dummyHead` and `dummyTail`.
  - **Duplicate Ingestion:** When an incoming alert matches an existing key in the map, its frequency counter is incremented, dynamic priority is boosted logarithmically, and the node is unlinked and spliced to the head of the DLL in strictly $O(1)$ time.
  - **Capacity Eviction:** When the cache reaches `maxCapacity` (e.g., 10,000 slots), the tail node is evicted in $O(1)$ time.

### 3.2. Data Structure 2: The Priority Engine (`PriorityMaxHeap.java`)
- **Challenge:** Standard binary heaps require an $O(n)$ search to locate an element when its priority changes. In a real-time SOC, dynamic alerts frequently gain weight as their repetition count rises.
- **Solution:** 
  - **Array-Backed Binary Heap:** Elements stored in contiguous flat memory (`AlertNode[] heap`). Children located at $2i + 1$ and $2i + 2$; parent at $(i - 1) / 2$.
  - **Hash Inverted Index (`keyToIndex`):** A synchronization map tracks the precise array index of every deduplication key. Every swap operation updates both the array slots and the inverted index entries in $O(1)$ time.
  - **In-Place Priority Escalation:** When an alert's score changes, its index is resolved in $O(1)$ time and rebalanced via `siftUp(idx)` or `siftDown(idx)` in strictly $O(\log n)$ time.
  - **Thread-Safety:** Heap mutations and index synchronization are guarded by `java.util.concurrent.locks.ReentrantLock`.

### 3.3. Data Structure 3: The Rule Engine (`TopicTrie.java`)
- **Challenge:** Routing keys arrive as dot-delimited strings (e.g., `sec.auth.bruteforce`, `sys.battery.low`). Regular expressions or hash lookups fail to capture hierarchical inheritance and prefix matching.
- **Solution:** 
  - A character-by-character Prefix Tree (`TrieNode`) with child edge maps.
  - Supports **Longest Prefix Matching (LPM)**: Deepest matching rule takes precedence. If no rule matches, safely defaults to `RuleMetadata.DEFAULT_FALLBACK`.
  - Concurrency protected via `ReentrantReadWriteLock` for zero-contention concurrent virtual-thread read access.

---

## 4. Concurrency & Networking Architecture

### 4.1. Java 21 Project Loom (Virtual Threads)
- All simulation firehoses, background telemetry aggregation, and client socket dispatchers run on lightweight virtual threads using `Executors.newVirtualThreadPerTaskExecutor()`.
- Virtual threads consume minimal RAM (~1 KB per thread vs. 1 MB for platform OS threads), enabling thousands of unblocked concurrent event generation tasks.

### 4.2. Java NIO WebSocket Server (RFC 6455)
- Pure standard Java SE implementation using `ServerSocketChannel` and `SocketChannel`.
- Zero third-party dependencies required.
- Implements:
  - HTTP static file serving for `index.html`, `style.css`, and `app.js`.
  - RFC 6455 WebSocket Handshake via `Sec-WebSocket-Key` SHA-1 / Base64 calculation.
  - Text frame encoding/decoding with masking/unmasking.
  - 50Hz (20ms interval) broadcast loop streaming aggregated telemetry packets to all connected SOC dashboards.

---

## 5. Frontend SOC / Palantir Cyber Aesthetic

The user interface is designed strictly to military-grade Security Operations Center specifications:
- **Palette:** OLED True Black (`#000000`) background with electric data accents:
  - Cyan (`#00FFFF`): Standard telemetry & normal ingress.
  - Magenta (`#FF00FF`): Deduplication engine, frequency multipliers, hash collisions.
  - Crimson (`#FF0033`): P0 Critical security breaches and top heap priority.
  - Neon Amber (`#FFB000`): P1 high infrastructure alerts.
  - Green (`#00FF66`): Nominal Loom virtual thread heartbeat.
- **Typography:** Strict monospace (`Fira Code`, `Geist Mono`, `Consolas`).
- **Telemetry Sparkline:** Real-time 60-second Canvas area chart rendering raw ingestion vs. reduced output volume divergence.
- **CSS FLIP (First-Last-Invert-Play) Animation:**
  - When the Java `PriorityMaxHeap` emits an updated ranking, DOM elements calculate bounding client rect offsets and smoothly glide vertically into their new ranks without layout stutter.
- **500ms Deduplication Flash:**
  - Whenever an alert's repetition count increments ($O(1)$ deduplication event), the row triggers a high-intensity `@keyframes dedupFlashAnimation` border flash for 500ms.
- **Synthesized Tactical Audio:** Built-in Web Audio API tone generator producing acoustic alerts on P0 threats.

---

## 6. Project Directory Layout

```text
Adv Java and DS/
│
├── src/
│   └── com/
│       └── soc/
│           └── reducer/
│               ├── AlertNode.java
│               ├── Notification.java
│               ├── O1Reducer.java
│               ├── PriorityMaxHeap.java
│               ├── RuleMetadata.java
│               ├── SimulationServer.java
│               └── TopicTrie.java
│
├── bin/                         # Compiled class files (excluded from Git)
│
├── index.html                   # SOC dashboard HTML
├── style.css                    # Dashboard styling
├── app.js                       # WebSocket client and dashboard logic
├── VerifyDataStructures.java    # Data structure verification suite
├── run.bat                      # Windows batch launcher
├── run.ps1                      # PowerShell launcher
├── .gitignore                   # Git exclusion rules
└── README.md                    # Project documentation
```

**Note:** The `bin/` directory contains generated Java class files. These are not required in the GitHub repository because they can be generated by compiling the source files.
---
## 7. How to Compile & Run

### Prerequisites
- Java Development Kit (JDK) 21 or higher.
- Windows, macOS, or Linux.

### Option A: Manual Command Line (Recommended)

Open Command Prompt or Terminal in the project root directory.

**1. Compile the Java source files:**

```bash
javac -d bin src/com/soc/reducer/*.java
```

**2. Start the Simulation Server:**

```bash
java -cp bin com.soc.reducer.SimulationServer 8080
```

**3. Open the SOC Dashboard:**

Open your browser and navigate to:

http://localhost:8080/

The server also provides a WebSocket telemetry endpoint:

```text
ws://localhost:8080/ws
```

### Option B: Windows Launchers

You can also use the included Windows launchers:

- `run.bat`
- `run.ps1`

**Note:** The launchers must be configured to compile and run the package-based Java source files in `src/com/soc/reducer/`. The manual commands above are the verified way to build and start the application.

### Stopping the Application

Keep the Command Prompt or Terminal window open while using the dashboard.

Press `Ctrl + C` to stop the server.


---

## 8. Interactive Dashboard Controls

Once the dashboard is running:
- **`[⚡ INJECT P0 CRITICAL]`**: Injects a wave of high-severity cyber attacks (SSH Brute Force / Zero-Day RCE) to demonstrate immediate heap rebalancing to the #1 root rank.
- **`[💬 WHATSAPP SPAM FLOOD]`**: Injects 150 duplicate messages within milliseconds to showcase $O(1)$ deduplication frequency escalation (`x150 DEDUPED`) and the 500ms magenta border flash.
- **Ingestion Pressure Slider**: Dynamically adjusts simulated event generation from 50 to 3,000 messages/sec.
- **`[⏸ PAUSE / RESUME]`**: Freezes or resumes the real-time simulation stream.
- **`[🧹 PURGE]`**: Clears heap and reducer memory in $O(N)$ time.
- **`[🔊 AUDIO: ON/OFF]`**: Toggles tactical cyber synthesizer audio feedback.
- **Waterfall Search Bar**: Instant real-time filtering of raw terminal logs by IP, routing key, or source.
