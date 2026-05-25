# N-N Atomic Register (NNAR)

**File**: `src/main/java/mydist/process/abstraction/NNAtomicRegister.java`

**Abstraction ID**: `app.nnar[registerName]`

---

## What It Does

An N-N Atomic Register allows **any** of the N processes to read or write. "Atomic" means operations appear to execute instantaneously and in some sequential order consistent with their real-time ordering — even when reads and writes overlap concurrently.

Specifically: a read always returns the value of the most recent completed write (or the most recent write that overlapped with the read, if they ran concurrently).

This is also known as **linearizability** for a shared register.

---

## Algorithm Family

Implements the ABD-style (Attiya-Bar-Noy-Dolev) quorum protocol, extended to N-writers. Each write carries a `(timestamp, writerRank)` pair. Timestamps increase monotonically; rank breaks ties when timestamps are equal. This gives a total order over all writes.

Reads use a **write-back** phase: after collecting a quorum of responses, the reader broadcasts the highest value back to all processes before returning. This ensures that a value seen by any reader is seen by all future readers, providing linearizability.

---

## State

Each `NNAtomicRegister` instance manages one named register:

| Field | Type | Meaning |
|---|---|---|
| `registerKey` | `String` | Register name (from `app.nnar[name]`) |
| `registerValue` | `int` | Locally stored value (-1 = undefined) |
| `timestamp` | `int` | Timestamp of locally stored value |
| `writerRank` | `int` | This process's rank (used as write-phase tiebreaker) |
| `totalProcesses` | `int` | N (needed to compute majority: `> N/2`) |
| `readIdGen` | `AtomicInteger` | Monotonically increasing ID for each operation |
| `activeOps` | `Map<Integer, OperationContext>` | In-flight operations keyed by readId |

### `OperationContext`

```java
record OperationContext(
    boolean writing,               // true = WRITE op, false = READ op
    Value writeValue,              // value to write (null if reading)
    Map<Integer, Value> readValues,    // responses: writerRank → value
    Map<Integer, Integer> timestamps,  // responses: writerRank → timestamp
    Map<Integer, Integer> ranks,       // responses: writerRank → rank (redundant but explicit)
    AtomicInteger readAcks,        // count of phase-1 (INTERNAL_VALUE) responses
    AtomicInteger writeAcks        // count of phase-2 (INTERNAL_ACK) responses
)
```

Multiple operations can be in-flight simultaneously (e.g., if the hub sends overlapping `APP_WRITE` and `APP_READ`). Each gets a unique `readId` so their responses don't collide.

---

## Write Operation

Triggered by `NNAR_WRITE` from App.

```
Phase 1 — Read quorum:
    readId = readIdGen.incrementAndGet()
    activeOps[readId] = OperationContext(writing=true, writeValue=v, ...)
    BEB_BROADCAST NNAR_INTERNAL_READ { readId }

    On each NNAR_INTERNAL_VALUE { readId, timestamp, writerRank, value }:
        store in ctx.readValues, ctx.timestamps, ctx.ranks
        if ctx.readAcks.incrementAndGet() > totalProcesses / 2:
            → proceed to phase 2

    Find highest (timestamp, rank) among collected responses:
        highestTs = max of all ctx.timestamps.values()
        highestRank = max rank among all entries with timestamp == highestTs
        (highestValue is the value at highestRank with highestTs)

Phase 2 — Write quorum:
    newTimestamp = highestTs + 1
    BEB_BROADCAST NNAR_INTERNAL_WRITE { readId, newTimestamp, writerRank=this.writerRank, value=writeValue }

    On each NNAR_INTERNAL_ACK { readId }:
        if ctx.writeAcks.incrementAndGet() > totalProcesses / 2:
            activeOps.remove(readId)
            emit NNAR_WRITE_RETURN to "app"
```

Source: `NNAtomicRegister.java:122-159` (phase 1 start), `NNAtomicRegister.java:200-302` (phase 1 completion + phase 2 start), `NNAtomicRegister.java:305-350` (phase 2 completion).

---

## Read Operation

Triggered by `NNAR_READ` from App. **Same two-phase structure as write**, but uses a write-back instead of a new value.

```
Phase 1 — same as write:
    readId = readIdGen.incrementAndGet()
    activeOps[readId] = OperationContext(writing=false, writeValue=null, ...)
    BEB_BROADCAST NNAR_INTERNAL_READ { readId }

    On majority NNAR_INTERNAL_VALUE responses:
        find (highestTs, highestRank, highestValue) same as write

Phase 2 — Write-back (linearizability requirement):
    BEB_BROADCAST NNAR_INTERNAL_WRITE {
        readId,
        timestamp = highestTs,     ← same timestamp, not +1
        writerRank = this.writerRank,
        value = highestValue
    }

    On majority NNAR_INTERNAL_ACK:
        activeOps.remove(readId)
        emit NNAR_READ_RETURN { value = registerValue } to "app"
```

**Why write-back?** Without it, a reader could see a value that was written by a writer that crashed before broadcasting to a full quorum. By writing the value back to a majority, the reader ensures all future readers will also see it. Without this, linearizability breaks.

Source: `NNAtomicRegister.java:161-196` (phase 1 start), `NNAtomicRegister.java:277-302` (read path in phase 1 completion).

---

## Responder Side

Every process responds to others' broadcast messages:

**On `BEB_DELIVER` of `NNAR_INTERNAL_READ { readId }`**:
```
emit PL_SEND to sender: NNAR_INTERNAL_VALUE {
    readId,
    timestamp = this.timestamp,
    writerRank = this.writerRank,
    value = (registerValue == -1 ? defined=false : defined=true, v=registerValue)
}
```

Source: `NNAtomicRegister.java:51-86`.

**On `BEB_DELIVER` of `NNAR_INTERNAL_WRITE { readId, timestamp, value }`**:
```
this.registerValue = value.v
this.timestamp = incomingTimestamp

emit PL_SEND to sender: NNAR_INTERNAL_ACK { readId }
```

Note: the write happens unconditionally — there is no check whether the incoming timestamp is higher than the local one. This is correct because the protocol ensures the broadcast timestamp is always the highest seen (highestTs + 1 for writes, highestTs for read write-backs). The per-process `timestamp` field tracks the last written value's timestamp for future reads.

Source: `NNAtomicRegister.java:88-119`.

---

## Late/Early Message Handling

Because operations are asynchronous and may be processed out of order, the code handles stale messages:

```java
OperationContext ctx = activeOps.get(readId);
if (ctx == null) {
    if (readId > currentMaxReadId) {
        // Future readId — message arrived before operation started — requeue
        messageQueue.offer(msg);
    } else {
        // Past readId — operation already completed — discard
        logger.info("Ignoring unexpected message with readId {}", readId);
    }
    return;
}
```

Source: `NNAtomicRegister.java:205-215`.

---

## Quorum Size

Majority quorum: `> totalProcesses / 2`.

For 6 processes (3 student + 3 hub nodes), majority = 4. For 3 processes, majority = 2. Integer division truncates, so the condition `> N/2` with integer division correctly implements "strictly more than half".

---

## Safety and Liveness

**Safety (atomicity)** holds as long as a majority of processes are correct (do not crash). The quorum intersection property guarantees that any two majority sets share at least one process, ensuring reads always see the last committed write.

**Liveness (progress)** holds as long as a majority remains correct and messages are eventually delivered.
