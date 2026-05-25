# Uniform Consensus

Consensus is implemented as a stack of four cooperating abstractions:

```
UniformConsensus (UC)          app.uc[topic]
    └── EpochChange (EC)       app.uc[topic].ec
        └── EventualLeaderDetector (ELD)   app.uc[topic].ec.eld
            └── EventuallyPerfectFailureDetector (EPFD)  ...epfd
    └── EpochConsensus (EP)    app.uc[topic].ep[N]  (dynamic)
```

EPFD and ELD are documented in [05-failure-detection.md](05-failure-detection.md).

---

## EpState

**File**: `src/main/java/mydist/process/abstraction/consensus/EpState.java`

```java
public record EpState(int ets, DistributedAlg.Value value) {}
```

Represents the state of a process within an epoch: the epoch timestamp at which the value was last updated (`ets`), and the value itself. Used by EP to carry state across epoch boundaries.

---

## EpochChange (EC)

**File**: `src/main/java/mydist/process/abstraction/consensus/EpochChange.java`

**Abstraction ID**: `app.uc[t].ec`

### Purpose

Converts leader-change events from ELD into epoch-number increments. Ensures all processes agree on who the current leader is and what epoch number they are in.

### State

| Field | Meaning |
|---|---|
| `trusted` | Current trusted leader (initialized to max-rank process) |
| `ts` | This process's proposed epoch timestamp |
| `lastTs` | Highest epoch timestamp accepted so far |
| `self` | This process's own ProcessId |

**Initialization**: `trusted = max-rank process`, `ts = self.rank`, `lastTs = 0`.

### Timestamp Spacing

Timestamps advance by `N` (number of processes) each time a leader retries: `ts = lastTs + N`.

This ensures different leaders never collide on timestamp values: if process with rank 2 starts at `ts=2` and process with rank 5 starts at `ts=5`, and each increments by N=6, their sequences are `2, 8, 14, ...` and `5, 11, 17, ...` — no overlap.

### Message Handling

**`ELD_TRUST { process: p }`** (from ELD):
```
trusted = p
if self == trusted:
    ts = lastTs + N
    BEB_BROADCAST EC_INTERNAL_NEW_EPOCH { timestamp: ts }
```

**`BEB_DELIVER` of `EC_INTERNAL_NEW_EPOCH { timestamp: newTs }` from sender s**:
```
if s == trusted AND newTs > lastTs:
    lastTs = newTs
    emit EC_START_EPOCH { newTimestamp: newTs, newLeader: s } to UC
else:
    PL_SEND EC_INTERNAL_NACK to sender s
```

**`PL_DELIVER` of `EC_INTERNAL_NACK`**:
```
handleSelfTrust(systemId)     ← re-trigger: bump ts and broadcast again
```

The NACK loop handles the case where the leader's new epoch announcement is rejected (because the recipient has already seen a higher timestamp from the same leader). The leader bumps its timestamp and retries.

Source: `EpochChange.java:42-125`.

---

## EpochConsensus (EP)

**File**: `src/main/java/mydist/process/abstraction/consensus/EpochConsensus.java`

**Abstraction ID**: `app.uc[t].ep[N]` (where N = epoch timestamp)

### Purpose

Single-epoch consensus. The leader proposes a value; all processes accept or the epoch is aborted. If the leader gets a majority to accept, it broadcasts a decision and all processes deliver `EP_DECIDE`.

### State

| Field | Meaning |
|---|---|
| `ets` | This epoch's timestamp (immutable, set at creation) |
| `state` | `EpState(valueTimestamp, value)` — last accepted (ts, value) pair |
| `tmpVal` | Value currently being proposed by the leader |
| `states` | Map of `"owner:index"` → `EpState` (phase 1 responses, cleared after use) |
| `accepted` | Count of `EP_INTERNAL_ACCEPT` replies in phase 2 |
| `aborted` | Latch; once true, all messages are ignored |

### Full Protocol Flow

**Leader side** (triggered by `EP_PROPOSE { value }` from UC):

```
EP_PROPOSE { value: v }:
    tmpVal = v
    BEB_BROADCAST EP_INTERNAL_READ

On majority EP_INTERNAL_STATE responses (from PL_DELIVER):
    highest = max by EpState.ets among collected states
    if highest.value.defined: tmpVal = highest.value
    states.clear()
    BEB_BROADCAST EP_INTERNAL_WRITE { value: tmpVal }

On majority EP_INTERNAL_ACCEPT responses (from PL_DELIVER):
    accepted = 0
    BEB_BROADCAST EP_INTERNAL_DECIDED { value: tmpVal }
```

**All processes** (via BEB_DELIVER):

```
BEB_DELIVER EP_INTERNAL_READ:
    PL_SEND to sender: EP_INTERNAL_STATE { valueTimestamp: state.ets, value: state.value }

BEB_DELIVER EP_INTERNAL_WRITE { value: v }:
    state = EpState(ets, v)
    PL_SEND to sender: EP_INTERNAL_ACCEPT

BEB_DELIVER EP_INTERNAL_DECIDED:
    emit EP_DECIDE { ets, value: state.value } to UC (parentId)
```

**Abort**:
```
EP_ABORT:
    emit EP_ABORTED { ets, valueTimestamp: state.ets, value: state.value } to UC
    aborted = true
    ← all future messages ignored (if (aborted) return; at top of handleMessage)
```

### Why Read Phase in EP?

Before the leader writes its proposed value, it first reads everyone's state. If a previous leader made progress (got some processes to write), the new leader must discover and continue from that value — otherwise two leaders could decide different values.

The rule: if any collected state has a defined value, use it instead of the freshly proposed value. This preserves any partial progress from aborted epochs.

Source: `EpochConsensus.java:37-214`.

### Creation

Each new epoch creates a fresh `EpochConsensus` instance in `UniformConsensus.addEpAbstractions`:
```java
abstractions.put(epRoot, new EpochConsensus(
    abstractionId,    // parent = UC
    epRoot,           // self ID = app.uc[t].ep[N]
    messageQ, processes, ets,
    initialState      // state carried over from aborted epoch
));
```

The `initialState` comes from `EP_ABORTED` — ensures continuity across epochs.

---

## UniformConsensus (UC)

**File**: `src/main/java/mydist/process/abstraction/consensus/UniformConsensus.java`

**Abstraction ID**: `app.uc[topic]`

### Purpose

Orchestrates epoch transitions. Starts with epoch 0, aborts it when EC announces a new leader, transitions to the new epoch, and eventually decides when an epoch consensus succeeds.

### State

| Field | Meaning |
|---|---|
| `val` | Value proposed to this UC instance (from `UC_PROPOSE`) |
| `proposed` | True after `EP_PROPOSE` sent in current epoch; reset on epoch change |
| `decided` | True after `UC_DECIDE` emitted; never reset (one-shot) |
| `ets` | Current epoch timestamp |
| `l` | Current leader `ProcessId` |
| `newTs` | Pending new epoch timestamp (from `EC_START_EPOCH`) |
| `newL` | Pending new leader (from `EC_START_EPOCH`) |

**Initialization**: `l = max-rank process`, create `EP[0]` with `EpState(0, undefined)`.

### Message Handling

**`UC_PROPOSE { value: v }`**:
```
val = v
→ updateLeader()
```

**`EC_START_EPOCH { newTimestamp, newLeader }`**:
```
if newTimestamp > ets:
    store newTs = newTimestamp, newL = newLeader
    emit EP_ABORT to app.uc[t].ep[ets]
→ updateLeader()
```

**`EP_ABORTED { ets: abortedEts, valueTimestamp, value }`**:
```
if abortedEts == this.ets:
    if newTs > 0 && newL != null:
        ets = newTs
        l = newL
        proposed = false
        addEpAbstractions(EpState(valueTimestamp, value))   ← new EP[ets]
→ updateLeader()
```

**`EP_DECIDE { ets: decideEts, value }`**:
```
if decideEts == this.ets && !decided:
    decided = true
    emit UC_DECIDE { value } to "app"
→ updateLeader()
```

### `updateLeader` — The Proposal Trigger

Called after every message. Proposes to the current epoch if all conditions met:

```java
if (l.owner == owner && l.index == index   // I am the leader
    && val.getDefined()                      // I have a value to propose
    && !proposed                             // I haven't proposed yet this epoch
    && !decided) {                           // Not yet decided
    proposed = true;
    emit EP_PROPOSE { value: val } to app.uc[t].ep[ets]
}
```

The `proposed` flag prevents sending `EP_PROPOSE` twice in the same epoch. It is reset when the epoch changes (`EP_ABORTED` handler sets `proposed = false`).

Source: `UniformConsensus.java:112-131`.

### Epoch Transition Sequence

```
Leader crashes or changes:
1. EPFD detects → EPFD_SUSPECT to ELD
2. ELD re-elects new leader → ELD_TRUST to EC
3. EC sees self == trusted → BEB_BROADCAST EC_INTERNAL_NEW_EPOCH
4. All processes receive, lastTs updated → EC_START_EPOCH to UC
5. UC aborts current EP → EP_ABORT to ep[ets]
6. EP[ets] responds EP_ABORTED { ets, state }
7. UC advances: ets=newTs, l=newL, creates ep[newTs] with old state
8. updateLeader() may propose if new l == self
```

### Why "Uniform"?

In uniform consensus, even processes that crash after deciding must not have decided a different value from correct processes. The epoch read-phase in EP guarantees this: if any process decided in a previous epoch, its value will be discovered by the new leader's read phase and propagated.

---

## Consensus Invariants Summary

| Invariant | Enforced by |
|---|---|
| Only one value decided per topic | `decided` latch in UC |
| Epoch transitions are monotone | `newTimestamp > ets` check in UC |
| Aborted EP ignores all future messages | `aborted` latch in EP |
| New epoch inherits previous state | `initialState` passed to `EpochConsensus` constructor |
| Leader proposes at most once per epoch | `proposed` flag reset on epoch change |
| Quorum majority required | `states.size() > N/2` and `accepted > N/2` in EP |
