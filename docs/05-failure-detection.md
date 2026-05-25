# Failure Detection and Leader Election

Two cooperating abstractions form the foundation of consensus:

1. **EPFD** (Eventually Perfect Failure Detector) — detects crashed processes
2. **ELD** (Eventual Leader Detector) — elects a stable leader

---

## Eventually Perfect Failure Detector (EPFD)

**File**: `src/main/java/mydist/process/abstraction/consensus/EventuallyPerfectFailureDetector.java`

**Abstraction ID**: `app.uc[t].ec.eld.epfd`

### What "Eventually Perfect" Means (◇P model)

- **Strong completeness**: every crashed process is eventually permanently suspected
- **Eventual strong accuracy**: after some time, no correct process is suspected

During initial transient periods, the detector may incorrectly suspect correct (live) processes. Eventually, the timeout adapts and false suspicions stop. This is the weakest failure detector sufficient for consensus.

### State

| Field | Type | Meaning |
|---|---|---|
| `alive` | `ConcurrentHashMap<String, ProcessId>` | Processes that replied in last interval |
| `suspected` | `ConcurrentHashMap<String, ProcessId>` | Currently suspected processes |
| `delay` | `int` | Current heartbeat timeout in ms |
| `DELTA` | `static final int = 100` | Baseline delay increment |
| `scheduler` | `ScheduledExecutorService` | Timer thread for timeout events |

### Thread Model Note

EPFD is the **only abstraction that uses a separate thread** (the scheduler). All other abstractions are called synchronously on the single `message-processing-thread`. The scheduler only calls `messageQueue.offer()` to inject `EPFD_TIMEOUT` events — it never mutates `alive` or `suspected` directly. State mutations happen on the main processing thread when `EPFD_TIMEOUT` is handled.

### Timer Loop

Every `delay` milliseconds, the scheduler offers `EPFD_TIMEOUT` to the queue.

On `EPFD_TIMEOUT`:
```
For each process p:
    key = p.owner + ":" + p.index
    isAlive    = alive.containsKey(key)
    isSuspected = suspected.containsKey(key)

    if !isAlive && !isSuspected:
        suspected.put(key, p)
        emit EPFD_SUSPECT { process: p } to parentId (ELD)

    if isAlive && isSuspected:
        suspected.remove(key)
        emit EPFD_RESTORE { process: p } to parentId (ELD)
        delayIncreased = true     ← false suspicion detected

    emit PL_SEND to p: EPFD_INTERNAL_HEARTBEAT_REQUEST

if delayIncreased:
    delay += DELTA

alive.clear()                     ← reset for next round
scheduleTimeout()                 ← reschedule
```

Source: `EventuallyPerfectFailureDetector.java:95-147`.

### Heartbeat Reply

On `PL_DELIVER` of `EPFD_INTERNAL_HEARTBEAT_REQUEST`:
```
emit PL_SEND: EPFD_INTERNAL_HEARTBEAT_REPLY
```

On `PL_DELIVER` of `EPFD_INTERNAL_HEARTBEAT_REPLY`:
```
alive.put(getProcessKey(sender), sender)
```

Source: `EventuallyPerfectFailureDetector.java:58-72`.

### Adaptive Delay

When a process is in `suspected` but then appears in `alive` (i.e., the previous suspicion was wrong), `delay += DELTA`. This slows the heartbeat cycle to give slow-but-alive processes more time to respond, eventually eliminating false suspicions.

### Cleanup

`cleanup()` calls `scheduler.shutdownNow()` — important because the scheduler thread must be stopped when the system is torn down. Source: `EventuallyPerfectFailureDetector.java:150-152`.

---

## Eventual Leader Detector (ELD)

**File**: `src/main/java/mydist/process/abstraction/consensus/EventualLeaderDetector.java`

**Abstraction ID**: `app.uc[t].ec.eld`

### What It Does

Maintains a set of suspected processes. Elects the **highest-rank non-suspected process** as leader. Emits `ELD_TRUST` events to `EpochChange` when the leader changes.

The "eventual" guarantee: once the system stabilizes (EPFD stops making mistakes), ELD permanently elects the same correct highest-rank process.

### State

| Field | Type | Meaning |
|---|---|---|
| `processes` | `List<ProcessId>` | All N processes |
| `processMap` | `Map<String, ProcessId>` | Key → ProcessId lookup |
| `suspected` | `Set<String>` | Keys of currently suspected processes |
| `currentLeader` | `ProcessId` | Last emitted leader (null initially) |

### Leader Election Logic

```java
ProcessId newLeader = processes.stream()
    .filter(p -> !suspected.contains(key(p)))
    .max(Comparator.comparingInt(ProcessId::getRank))
    .orElse(null);

if (newLeader != null && (currentLeader == null || !key(currentLeader).equals(key(newLeader)))) {
    currentLeader = newLeader;
    emit ELD_TRUST { process: newLeader } to parentAbstractionId (EC)
}
```

`ELD_TRUST` is emitted **only when the leader changes** — not on every suspect/restore. This prevents unnecessary epoch changes.

Source: `EventualLeaderDetector.java:68-88`.

### Initial Election

The constructor calls `electNewLeader()` immediately — before any failures are detected. This elects the max-rank process (all unsuspected) and emits the initial `ELD_TRUST` to EC.

Source: `EventualLeaderDetector.java:39-40`.

### Handling EPFD Events

```
EPFD_SUSPECT { process: p }:
    suspected.add(key(p))
    electNewLeader()

EPFD_RESTORE { process: p }:
    suspected.remove(key(p))
    electNewLeader()
```

Source: `EventualLeaderDetector.java:44-66`.

---

## Interaction Between EPFD and ELD

```
EPFD scheduler fires EPFD_TIMEOUT
    │
    ├── sends heartbeats to all via PL
    │   └── alive processes reply with HEARTBEAT_REPLY → added to alive set
    │
    └── on next timeout:
        ├── non-repliers → EPFD_SUSPECT → ELD
        │   └── ELD updates suspected, re-elects, may emit ELD_TRUST
        └── restored processes → EPFD_RESTORE → ELD
            └── ELD removes from suspected, re-elects, may emit ELD_TRUST
```

ELD then feeds `ELD_TRUST` events to `EpochChange`, which triggers epoch transitions in `UniformConsensus`. See [docs/06-consensus.md](06-consensus.md).
