# Reimplementation Guide

Self-contained recipe for building an equivalent distributed algorithms node in any language. This is the recommended starting point for AI agents and developers building a compatible implementation.

---

## What You Are Building

A process that:
1. Connects to a hub via TCP and registers
2. Receives commands (`APP_READ`, `APP_WRITE`, `APP_PROPOSE`, `APP_BROADCAST`)
3. Executes distributed algorithms across 6 nodes (3 yours + 3 hub's)
4. Reports results back to the hub (`APP_READ_RETURN`, `APP_WRITE_RETURN`, `APP_DECIDE`, `APP_VALUE`)

The hub is a teacher-provided black box. Your job is to speak the correct wire protocol.

---

## Prerequisites

1. **Protobuf bindings** from `distributed_alg.proto` in your target language
2. **TCP socket** support with 4-byte length-prefix framing
3. **Concurrent queue** (thread-safe FIFO) for message passing
4. **Scheduled timer** capability (for EPFD heartbeat)

---

## Component Build Order

Build and test in this order. Each layer depends on those below it.

### Step 1: Wire Protocol

Implement the TCP framing layer:

```
Send:
    payload = proto_serialize(message)
    send big_endian_int32(len(payload))
    send payload

Receive:
    size = read_big_endian_int32()
    data = read_exactly(size)
    message = proto_deserialize(data)
```

Retry policy: 3 attempts, exponential backoff (100ms × 2^attempt). Log and drop after 3 failures.

### Step 2: Message Queue + Router

Core loop:
```
queue = thread_safe_blocking_queue()
abstractions = dict<string, handler>    # keyed by abstraction ID

def process_loop():
    while True:
        msg = queue.take()              # blocks
        handle_message(msg)

def handle_message(msg):
    to_id = msg.to_abstraction_id
    if to_id not in abstractions:
        if to_id.startswith("app.nnar["):
            register_nnar(extract_key(to_id))
        elif to_id.startswith("app.uc["):
            register_uc(extract_topic(to_id))
    handler = abstractions.get(to_id)
    if handler:
        handler.handle(msg)
    else:
        log_error("no handler for", to_id)
```

**Critical**: `handle_message` must run on a **single thread**. Algorithm state mutations are not thread-safe.

### Step 3: Registration and Initialization

On startup:
```
1. Send ProcRegistration to hub:
   outer = Message(
       type=NETWORK_MESSAGE,
       to_abstraction_id="app",
       system_id="",
       network_message=NetworkMessage(
           sender_listening_port=my_port,
           message=Message(
               type=PROC_REGISTRATION,
               to_abstraction_id="app",
               proc_registration=ProcRegistration(owner=my_owner, index=my_index)
           )
       )
   )
   tcp_send(hub_host, hub_port, outer)

2. Start TCP listener on my_port
   For each incoming connection:
       receive message
       if PROC_INITIALIZE_SYSTEM: handle directly (sets up processes list)
       else: queue.offer(msg)

3. On PROC_INITIALIZE_SYSTEM:
   system_id = msg.system_id
   processes = msg.proc_initialize_system.processes
   current_process = find process by (owner, index) in processes
   register_base_abstractions()
   start process_loop thread
```

### Step 4: PerfectLink

Two responsibilities: **send** and **receive**.

```python
class PerfectLink:
    def __init__(self, queue, processes, host, port, hub_host, hub_port,
                 system_id, parent_abstraction_id):
        ...

    def handle(self, msg):
        if msg.type == NETWORK_MESSAGE:
            inner = msg.network_message.message
            sender = find_process(
                msg.network_message.sender_host,
                msg.network_message.sender_listening_port
            )
            queue.offer(Message(
                type=PL_DELIVER,
                system_id=msg.system_id,
                from_abstraction_id=msg.to_abstraction_id,
                to_abstraction_id=self.parent_abstraction_id,
                pl_deliver=PlDeliver(sender=sender, message=inner)
            ))

        elif msg.type == PL_SEND:
            dest = msg.pl_send.destination if msg.pl_send.has_destination else hub
            wrapped = wrap_network_message(msg)
            tcp_send_with_retry(dest.host, dest.port, wrapped)

    def wrap_network_message(self, msg):
        return Message(
            type=NETWORK_MESSAGE,
            system_id=self.system_id,
            from_abstraction_id=self.parent_abstraction_id + ".pl",
            to_abstraction_id=msg.to_abstraction_id,
            network_message=NetworkMessage(
                sender_host=self.host,
                sender_listening_port=self.port,
                message=msg.pl_send.message
            )
        )
```

**Factory pattern**: each parent abstraction gets its own PL instance with a different `parent_abstraction_id`. Share network config but differ on routing.

### Step 5: BestEffortBroadcast

```python
class BestEffortBroadcast:
    def __init__(self, queue, processes, abstraction_id):
        ...

    def handle(self, msg):
        if msg.type == BEB_BROADCAST:
            for p in self.processes:
                self.queue.offer(Message(
                    type=PL_SEND,
                    from_abstraction_id=self.id,
                    to_abstraction_id=self.id + ".pl",
                    pl_send=PlSend(
                        destination=p,
                        message=msg.beb_broadcast.message
                    )
                ))

        elif msg.type == PL_DELIVER:
            self.queue.offer(Message(
                type=BEB_DELIVER,
                from_abstraction_id=self.id,
                to_abstraction_id=msg.pl_deliver.message.to_abstraction_id,
                beb_deliver=BebDeliver(
                    sender=msg.pl_deliver.sender,
                    message=msg.pl_deliver.message
                )
            ))
```

Note: `BEB_DELIVER` is routed to the **inner message's** `to_abstraction_id`, not to the BEB's parent.

### Step 6: App (Hub Bridge)

Translates hub commands to internal events and internal results back to hub.

Key translations:
```
PL_DELIVER(APP_BROADCAST(v))  →  BEB_BROADCAST(APP_VALUE(v))  to app.beb
PL_DELIVER(APP_VALUE(v))      →  PL_SEND(APP_VALUE(v))         to app.pl (→ hub)
PL_DELIVER(APP_READ(r))       →  NNAR_READ                     to app.nnar[r]
PL_DELIVER(APP_WRITE(r,v))    →  NNAR_WRITE(v)                 to app.nnar[r]
PL_DELIVER(APP_PROPOSE(t,v))  →  UC_PROPOSE(v)                 to app.uc[t]
BEB_DELIVER(APP_VALUE(v))     →  PL_SEND(APP_VALUE(v))         to app.pl
NNAR_READ_RETURN(v)           →  PL_SEND(APP_READ_RETURN(r,v)) to app.pl
NNAR_WRITE_RETURN             →  PL_SEND(APP_WRITE_RETURN(r))  to app.pl
UC_DECIDE(v)                  →  PL_SEND(APP_DECIDE(v))        to app.pl
```

Extract register name from `from_abstraction_id` for read/write returns:
`"app.nnar[x]"` → `"x"` (substring between `[` and `]`).

### Step 7: NNAtomicRegister

Implement the quorum read/write protocol. See [04-nnar.md](04-nnar.md) for full algorithm.

Key invariants:
- Each operation (read or write) has a unique `read_id` (monotonic integer per process)
- Track in-flight operations in a dict: `read_id → OperationContext`
- Majority threshold: `acks > N // 2` (integer division)
- Both READ and WRITE use a two-phase protocol (BEB read, PL collect, BEB write, PL collect)
- READ write-back uses same timestamp (not +1); WRITE uses `highest_ts + 1`
- Discard late messages (completed `read_id`) silently; requeue early messages (future `read_id`)

### Step 8: EventuallyPerfectFailureDetector

```python
class EPFD:
    delay = 100     # ms
    DELTA = 100

    def __init__(...):
        alive = {key(p): p for p in processes}
        suspected = {}
        schedule_timeout()

    def schedule_timeout():
        timer.schedule(delay, lambda: queue.offer(EPFD_TIMEOUT))

    def handle_timeout(system_id):
        for p in processes:
            if p not in alive and p not in suspected:
                suspected[key(p)] = p
                queue.offer(EPFD_SUSPECT(p) → parent_id)
            if p in alive and p in suspected:
                del suspected[key(p)]
                queue.offer(EPFD_RESTORE(p) → parent_id)
                delay += DELTA   # false suspicion
            queue.offer(PL_SEND(EPFD_INTERNAL_HEARTBEAT_REQUEST) to p)
        alive.clear()
        schedule_timeout()

    def handle_heartbeat_request(system_id):
        queue.offer(PL_SEND(EPFD_INTERNAL_HEARTBEAT_REPLY))
        # ⚠️ SET DESTINATION to the sender! Do not leave destination unset.

    def handle_heartbeat_reply(sender):
        alive[key(sender)] = sender
```

**Critical**: the heartbeat reply must set `PlSend.destination` to the process that sent the request. The Java implementation has a bug here — do not replicate it.

### Step 9: EventualLeaderDetector

```python
class ELD:
    suspected = set()
    current_leader = None

    def __init__(...):
        elect_new_leader()   # emit initial ELD_TRUST immediately

    def handle_suspect(p):
        suspected.add(key(p))
        elect_new_leader()

    def handle_restore(p):
        suspected.discard(key(p))
        elect_new_leader()

    def elect_new_leader():
        candidates = [p for p in processes if key(p) not in suspected]
        new_leader = max(candidates, key=lambda p: p.rank, default=None)
        if new_leader and key(new_leader) != key(current_leader):
            current_leader = new_leader
            queue.offer(ELD_TRUST(new_leader) → parent_id)
```

### Step 10: EpochChange

```python
class EpochChange:
    trusted = max(processes, key=lambda p: p.rank)
    ts = self_process.rank
    last_ts = 0

    def handle_eld_trust(p, system_id):
        trusted = p
        handle_self_trust(system_id)

    def handle_self_trust(system_id):
        if key(self) == key(trusted):
            ts = last_ts + len(processes)
            queue.offer(BEB_BROADCAST(EC_INTERNAL_NEW_EPOCH(ts)) → ec.beb)

    def handle_beb_deliver(inner, sender, system_id):
        if inner.type == EC_INTERNAL_NEW_EPOCH:
            new_ts = inner.ec_internal_new_epoch.timestamp
            if key(sender) == key(trusted) and new_ts > last_ts:
                last_ts = new_ts
                queue.offer(EC_START_EPOCH(new_ts, sender) → parent_id)
            else:
                queue.offer(PL_SEND(EC_INTERNAL_NACK) to sender)

    def handle_pl_deliver(inner, system_id):
        if inner.type == EC_INTERNAL_NACK:
            handle_self_trust(system_id)
```

### Step 11: EpochConsensus

```python
class EpochConsensus:
    aborted = False
    state = initial_state       # EpState(valueTimestamp, value)
    tmp_val = Value(defined=False)
    states = {}                 # "owner:index" → EpState
    accepted = 0

    def handle(msg):
        if aborted: return      # ⚠️ CRITICAL: latch, ignore all messages after abort

        if msg.type == EP_ABORT:
            queue.offer(EP_ABORTED(ets, state.ets, state.value) → parent_id)
            aborted = True

        elif msg.type == EP_PROPOSE:
            tmp_val = msg.ep_propose.value
            queue.offer(BEB_BROADCAST(EP_INTERNAL_READ) → beb)

        elif msg.type == BEB_DELIVER:
            handle_beb(msg)

        elif msg.type == PL_DELIVER:
            handle_pl(msg)

    def handle_beb(msg):
        inner = msg.beb_deliver.message
        sender = msg.beb_deliver.sender
        if inner.type == EP_INTERNAL_READ:
            queue.offer(PL_SEND(EP_INTERNAL_STATE(state.ets, state.value)) to sender)
        elif inner.type == EP_INTERNAL_WRITE:
            state = EpState(ets, inner.value)
            queue.offer(PL_SEND(EP_INTERNAL_ACCEPT) to sender)
        elif inner.type == EP_INTERNAL_DECIDED:
            queue.offer(EP_DECIDE(ets, state.value) → parent_id)

    def handle_pl(msg):
        inner = msg.pl_deliver.message
        sender = msg.pl_deliver.sender
        if inner.type == EP_INTERNAL_STATE:
            states[key(sender)] = EpState(inner.value_timestamp, inner.value)
            if len(states) > len(processes) // 2:
                highest = max(states.values(), key=lambda s: s.ets)
                if highest.value.defined:
                    tmp_val = highest.value
                states.clear()
                queue.offer(BEB_BROADCAST(EP_INTERNAL_WRITE(tmp_val)) → beb)
        elif inner.type == EP_INTERNAL_ACCEPT:
            accepted += 1
            if accepted > len(processes) // 2:
                accepted = 0
                queue.offer(BEB_BROADCAST(EP_INTERNAL_DECIDED(tmp_val)) → beb)
```

### Step 12: UniformConsensus

```python
class UniformConsensus:
    val = Value(defined=False)
    proposed = False
    decided = False
    ets = 0
    l = max(processes, key=lambda p: p.rank)
    new_ts = None
    new_l = None

    def __init__(...):
        add_ep_abstractions(EpState(0, Value(defined=False)))

    def handle(msg):
        if msg.type == UC_PROPOSE:
            val = msg.uc_propose.value
        elif msg.type == EC_START_EPOCH:
            if msg.new_timestamp > ets:
                new_ts = msg.new_timestamp
                new_l = msg.new_leader
                queue.offer(EP_ABORT → ep[ets])
        elif msg.type == EP_ABORTED:
            if msg.ets == ets and new_ts is not None:
                ets = new_ts; l = new_l; proposed = False
                add_ep_abstractions(EpState(msg.value_timestamp, msg.value))
        elif msg.type == EP_DECIDE:
            if msg.ets == ets and not decided:
                decided = True
                queue.offer(UC_DECIDE(msg.value) → "app")
        update_leader(msg.system_id)

    def update_leader(system_id):
        if (key(l) == key(self)
                and val.defined
                and not proposed
                and not decided):
            proposed = True
            queue.offer(EP_PROPOSE(val) → ep[ets])

    def add_ep_abstractions(initial_state):
        ep_root = abstraction_id + ".ep[" + str(ets) + "]"
        abstractions[ep_root] = EpochConsensus(abstraction_id, ep_root, ...)
        abstractions[ep_root + ".beb"] = BestEffortBroadcast(...)
        abstractions[ep_root + ".pl"] = pl.copy(parent=ep_root)
        abstractions[ep_root + ".beb.pl"] = pl.copy(parent=ep_root + ".beb")
```

---

## Hub Interaction Contract

| Trigger | Process must send |
|---|---|
| Startup | `PROC_REGISTRATION` wrapped in `NETWORK_MESSAGE` |
| `APP_BROADCAST` received | Nothing directly — but when `BEB_DELIVER` fires, send `APP_VALUE` |
| `APP_READ` completes | `APP_READ_RETURN { register, value }` |
| `APP_WRITE` completes | `APP_WRITE_RETURN { register }` |
| `APP_PROPOSE` completes | `APP_DECIDE { value }` |

All messages to hub go via `PL_SEND` with no destination (routes to hub automatically in PL).

`systemId` must be preserved: always copy `system_id` from incoming messages to outgoing replies.

---

## Base Abstraction Registration

Register these immediately after `PROC_INITIALIZE_SYSTEM`:

```python
pl = PerfectLink(queue, processes, host, port, hub_host, hub_port, system_id)
abstractions["app"]       = App(queue)
abstractions["app.pl"]    = pl.copy(parent="app")
abstractions["app.beb"]   = BestEffortBroadcast(queue, processes, "app.beb")
abstractions["app.beb.pl"] = pl.copy(parent="app.beb")
```

Register NNAR and UC lazily (on first message targeting them).

---

## NNAR Abstraction Registration

When first message targets `app.nnar[x]`:
```python
pl = PerfectLink(...)
abstractions["app.nnar[x]"]       = NNAtomicRegister(queue, key="x", rank=my_rank, N=len(processes))
abstractions["app.nnar[x].pl"]    = pl.copy(parent="app.nnar[x]")
abstractions["app.nnar[x].beb"]   = BestEffortBroadcast(queue, processes, "app.nnar[x].beb")
abstractions["app.nnar[x].beb.pl"] = pl.copy(parent="app.nnar[x].beb")
```

## UC Abstraction Registration

When first message targets `app.uc[t]`:
```python
uc_id = "app.uc[t]"
abstractions[uc_id]               = UniformConsensus(uc_id, queue, abstractions, ...)
abstractions[uc_id + ".ec"]       = EpochChange(uc_id, uc_id+".ec", self_process, queue, processes)
abstractions[uc_id + ".ec.pl"]    = pl.copy(parent=uc_id+".ec")
abstractions[uc_id + ".ec.beb"]   = BestEffortBroadcast(queue, processes, uc_id+".ec.beb")
abstractions[uc_id + ".ec.beb.pl"] = pl.copy(parent=uc_id+".ec.beb")
abstractions[uc_id + ".ec.eld"]   = EventualLeaderDetector(queue, uc_id+".ec", uc_id+".ec.eld", processes, system_id)
abstractions[uc_id + ".ec.eld.epfd"] = EPFD(uc_id+".ec.eld", uc_id+".ec.eld.epfd", queue, processes)
abstractions[uc_id + ".ec.eld.epfd.pl"] = pl.copy(parent=uc_id+".ec.eld.epfd")
```

Note: EP epoch abstractions are registered dynamically inside UC (`add_ep_abstractions`), not here.

---

## Common Pitfalls

| Pitfall | Correct behavior |
|---|---|
| Processing `PROC_INITIALIZE_SYSTEM` from the queue | Handle it directly on receipt, before starting queue processor |
| Not preserving `system_id` | Copy `system_id` from incoming to outgoing messages |
| EP continues after `EP_ABORT` | Set `aborted = True` latch, return immediately on all future messages |
| EPFD heartbeat reply has no destination | Set `destination = sender` from the `PL_DELIVER` that carried the request |
| Not resetting `proposed` on epoch change | Set `proposed = False` in `EP_ABORTED` handler |
| Not sending `UC_DECIDE` only once | Use `decided` latch: `if not decided: decided = True; emit` |
| Mutating algorithm state from timer thread | Timer only calls `queue.offer()`; state mutation happens on queue thread only |
| Missing `readId` matching | Use unique monotonic `read_id` per operation; discard responses with unknown `read_id` |
| Not writing back on NNAR read | NNAR read **must** do a write-back phase for linearizability |
| `ELD_TRUST` emitted on every timeout | Emit only when leader **changes** (`new_leader != current_leader`) |
| EC: not bumping `ts` on NACK retry | `ts = last_ts + N` each retry, not a fixed increment |

---

## Verification

Run your implementation against the reference hub:

```bash
# Start hub (teacher binary)
./dalgs-reference-binaries/dalgs-linux-amd64

# Start your 3 nodes (adjust ports and hub address)
<your-command> <hub_host> <hub_port> <node_host> 6001 6002 6003
```

Expected hub log in `dalgs-ref.log`: all `APP_READ_RETURN`, `APP_WRITE_RETURN`, and `APP_DECIDE` messages arrive with correct values and in valid order.

Check for correctness:
- NNAR: reads return the most recently written value (or undefined if never written)
- Consensus: all processes decide the same value for each topic
- No timeouts or stalled operations

---

## Language-Specific Notes

| Concept | Java (this impl) | Python | Go | Rust |
|---|---|---|---|---|
| Message queue | `LinkedBlockingQueue` | `queue.Queue` | `chan Message` | `mpsc::channel` |
| Atomic counter | `AtomicInteger` | `threading.Lock` + int | `atomic.Int32` | `AtomicU32` |
| Scheduled timer | `ScheduledExecutorService` | `threading.Timer` | `time.AfterFunc` | `tokio::time::sleep` |
| Protobuf | `protobuf-java` | `protobuf` (pip) | `google.golang.org/protobuf` | `prost` |
| Record/struct | `record EpState(...)` | `@dataclass` or `namedtuple` | struct | struct |
| Concurrent map | `ConcurrentHashMap` | `dict` (GIL) or `threading.Lock` | `sync.Map` | `DashMap` |

In Python, the GIL provides sufficient protection for the single-threaded queue pattern. In Go and Rust, use proper synchronization if sharing data across goroutines/tasks.
