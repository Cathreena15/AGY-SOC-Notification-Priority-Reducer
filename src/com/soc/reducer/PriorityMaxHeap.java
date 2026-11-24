package com.soc.reducer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

/**
 * PriorityMaxHeap: A high-performance, thread-safe, array-backed Binary Max-Heap
 * with Hash-Indexed O(log n) In-Place Priority Rebalancing.
 * 
 * <p>Algorithmic Highlights:
 * <ul>
 *   <li><b>Array-Backed Storage:</b> Zero pointer fragmentation, cache-friendly flat array layout.</li>
 *   <li><b>O(log n) Insert & Extract:</b> Standard binary max-heap invariants maintained via sift-up and sift-down.</li>
 *   <li><b>Hash-Indexed Priority Escalation:</b> Standard binary heaps require O(N) linear scan to locate an
 *       existing alert when its frequency increases. This implementation maintains an internal inverted index
 *       {@code keyToIndex} map. When an existing alert's frequency increments, its array index is retrieved in O(1)
 *       and rebalanced in <b>O(log n)</b> time via sift-up/down.</li>
 *   <li><b>Thread-Safety via ReentrantLock:</b> Rebalancing and index synchronization are guarded by
 *       a low-overhead {@link ReentrantLock}, preventing race conditions across massive virtual-thread ingress.</li>
 * </ul>
 * </p>
 */
public class PriorityMaxHeap {

    private static final int DEFAULT_INITIAL_CAPACITY = 64;

    private AlertNode[] heap;
    private int size;
    private final Map<String, Integer> keyToIndex;
    private final ReentrantLock lock;

    /**
     * Initializes an empty PriorityMaxHeap with a specified initial capacity.
     * 
     * @param initialCapacity Initial size of the backing array
     * 
     * Time Complexity: O(1)
     * Space Complexity: O(initialCapacity) for backing array allocation.
     */
    public PriorityMaxHeap(int initialCapacity) {
        int cap = Math.max(initialCapacity, DEFAULT_INITIAL_CAPACITY);
        this.heap = new AlertNode[cap];
        this.size = 0;
        this.keyToIndex = new HashMap<>(cap);
        this.lock = new ReentrantLock();
    }

    /**
     * Default constructor with capacity 64.
     */
    public PriorityMaxHeap() {
        this(DEFAULT_INITIAL_CAPACITY);
    }

    /**
     * Inserts a new alert or dynamically updates an existing alert's priority in O(log n) time.
     * If the alert already exists in the heap (matched via its deduplication key), its position
     * is retrieved in O(1) time and rebalanced via sift-up or sift-down in O(log n) time.
     * 
     * @param node The alert node to insert or re-prioritize
     * 
     * Time Complexity: O(log n) average and worst-case.
     * Space Complexity: O(1) amortized auxiliary space.
     */
    public void insertOrUpdate(AlertNode node) {
        Objects.requireNonNull(node, "AlertNode cannot be null");

        lock.lock();
        try {
            String key = node.getDeduplicationKey();
            Integer existingIndex = keyToIndex.get(key);

            if (existingIndex != null) {
                // In-place update: Alert already exists in heap
                int idx = existingIndex;
                heap[idx] = node;

                // Rebalance: try siftUp first; if no movement, siftDown
                int newIdx = siftUp(idx);
                if (newIdx == idx) {
                    siftDown(idx);
                }
            } else {
                // New insertion: ensure capacity
                ensureCapacity(size + 1);

                int idx = size;
                heap[idx] = node;
                keyToIndex.put(key, idx);
                size++;

                siftUp(idx);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Retrieves and removes the highest-priority alert from the root of the heap.
     * 
     * @return The highest-priority {@link AlertNode}, or null if heap is empty
     * 
     * Time Complexity: O(log n)
     * Space Complexity: O(1)
     */
    public AlertNode extractMax() {
        lock.lock();
        try {
            if (size == 0) {
                return null;
            }

            AlertNode maxNode = heap[0];
            keyToIndex.remove(maxNode.getDeduplicationKey());

            AlertNode lastNode = heap[size - 1];
            heap[size - 1] = null; // GC dereference
            size--;

            if (size > 0) {
                heap[0] = lastNode;
                keyToIndex.put(lastNode.getDeduplicationKey(), 0);
                siftDown(0);
            }

            return maxNode;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Inspects the highest-priority alert at the root without removing it.
     * 
     * @return Top {@link AlertNode}, or null if empty
     * 
     * Time Complexity: O(1)
     * Space Complexity: O(1)
     */
    public AlertNode peekMax() {
        lock.lock();
        try {
            return (size == 0) ? null : heap[0];
        } finally {
            lock.unlock();
        }
    }

    /**
     * Sifts an element upward along its ancestral path until the max-heap invariant is satisfied:
     * parent >= child.
     * 
     * @param index Starting index to sift up
     * @return Final settled array index of the node
     * 
     * Time Complexity: O(log n)
     * Space Complexity: O(1)
     */
    private int siftUp(int index) {
        while (index > 0) {
            int parentIndex = (index - 1) >>> 1;
            if (compareNodes(heap[index], heap[parentIndex]) > 0) {
                swap(index, parentIndex);
                index = parentIndex;
            } else {
                break;
            }
        }
        return index;
    }

    /**
     * Sifts an element downward toward leaf positions until the max-heap invariant is satisfied:
     * parent >= both children.
     * 
     * @param index Starting index to sift down
     * 
     * Time Complexity: O(log n)
     * Space Complexity: O(1)
     */
    private void siftDown(int index) {
        int half = size >>> 1; // Leaf nodes begin at size / 2
        while (index < half) {
            int leftChild = (index << 1) + 1;
            int rightChild = leftChild + 1;
            int largest = leftChild;

            if (rightChild < size && compareNodes(heap[rightChild], heap[leftChild]) > 0) {
                largest = rightChild;
            }

            if (compareNodes(heap[largest], heap[index]) > 0) {
                swap(index, largest);
                index = largest;
            } else {
                break;
            }
        }
    }

    /**
     * Swaps two elements in the array and synchronizes their updated positions
     * within the inverted hash index.
     * 
     * @param i First index
     * @param j Second index
     * 
     * Time Complexity: O(1)
     * Space Complexity: O(1)
     */
    private void swap(int i, int j) {
        AlertNode temp = heap[i];
        heap[i] = heap[j];
        heap[j] = temp;

        if (heap[i] != null) {
            keyToIndex.put(heap[i].getDeduplicationKey(), i);
        }
        if (heap[j] != null) {
            keyToIndex.put(heap[j].getDeduplicationKey(), j);
        }
    }

    /**
     * Total ordering comparison function:
     * 1. Primary sort key: Dynamic Priority Score (descending)
     * 2. Secondary sort key: Last Seen Timestamp (newer timestamps break ties)
     * 
     * @param a First node
     * @param b Second node
     * @return Positive if a > b, negative if a < b, 0 if equal
     * 
     * Time Complexity: O(1)
     * Space Complexity: O(1)
     */
    private int compareNodes(AlertNode a, AlertNode b) {
        if (a == null && b == null) return 0;
        if (a == null) return -1;
        if (b == null) return 1;

        int scoreComp = Double.compare(a.getDynamicPriority(), b.getDynamicPriority());
        if (scoreComp != 0) {
            return scoreComp;
        }
        // Tie-breaker: recency
        return Long.compare(a.getLastSeenMs(), b.getLastSeenMs());
    }

    /**
     * Ensures backing array has sufficient capacity, doubling size if necessary.
     * 
     * Time Complexity: O(1) amortized, O(N) when resizing occurs.
     * Space Complexity: O(N) during reallocation.
     */
    private void ensureCapacity(int minCapacity) {
        if (minCapacity > heap.length) {
            int newCapacity = heap.length + (heap.length >> 1); // 1.5x expansion
            if (newCapacity < minCapacity) {
                newCapacity = minCapacity;
            }
            heap = Arrays.copyOf(heap, newCapacity);
        }
    }

    /**
     * Returns the current number of active alerts in the heap.
     * 
     * Time Complexity: O(1)
     * Space Complexity: O(1)
     */
    public int size() {
        lock.lock();
        try {
            return size;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Extracts a non-destructive snapshot of the top K highest-priority alerts in sorted order.
     * Operates on an internal array copy to avoid mutating the live heap structure.
     * 
     * @param k Maximum number of top alerts to retrieve
     * @return List of up to K alerts in strictly descending priority order
     * 
     * Time Complexity: O(k log n)
     * Space Complexity: O(n) to clone backing slice, O(k) for output list.
     */
    public List<AlertNode> getTopK(int k) {
        lock.lock();
        try {
            int count = Math.min(k, size);
            if (count == 0) {
                return new ArrayList<>();
            }

            // Clone active portion of heap array
            AlertNode[] cloneArr = Arrays.copyOf(heap, size);
            int cloneSize = size;
            List<AlertNode> topList = new ArrayList<>(count);

            for (int i = 0; i < count; i++) {
                AlertNode max = cloneArr[0];
                topList.add(max);

                // Rebalance clone
                cloneArr[0] = cloneArr[cloneSize - 1];
                cloneSize--;
                siftDownArray(cloneArr, cloneSize, 0);
            }

            return topList;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Sift-down helper for transient array snapshotting without locking overhead or index mutation.
     * 
     * Time Complexity: O(log n)
     * Space Complexity: O(1)
     */
    private void siftDownArray(AlertNode[] arr, int n, int index) {
        int half = n >>> 1;
        while (index < half) {
            int left = (index << 1) + 1;
            int right = left + 1;
            int largest = left;

            if (right < n && compareNodes(arr[right], arr[left]) > 0) {
                largest = right;
            }

            if (compareNodes(arr[largest], arr[index]) > 0) {
                AlertNode temp = arr[index];
                arr[index] = arr[largest];
                arr[largest] = temp;
                index = largest;
            } else {
                break;
            }
        }
    }

    /**
     * Clears all elements from the heap and resets inverted index.
     * 
     * Time Complexity: O(N) to clear array references.
     * Space Complexity: O(1).
     */
    public void clear() {
        lock.lock();
        try {
            Arrays.fill(heap, 0, size, null);
            keyToIndex.clear();
            size = 0;
        } finally {
            lock.unlock();
        }
    }
}
