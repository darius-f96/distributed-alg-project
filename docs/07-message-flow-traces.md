# Message Flow Traces

ASCII sequence diagrams for the five main flows. Participants: Hub, Process A (rank 3, leader), Process B (rank 2), Process C (rank 1). "→" = message sent, "⊕" = internal queue event.

---

## 1. AppBroadcast (BEB Demo)

Hub sends a value; all processes receive it via BEB; each forwards to hub.

```
Hub          Process A          Process B          Process C
 │                │                  │                  │
 │──APP_BROADCAST─►│                  │                  │
 │  (via PL)      │                  │                  │
 │                │⊕ BEB_BROADCAST   │                  │
 │                │   (APP_VALUE)     │                  │
 │                │──PL_SEND─────────►│                  │
 │                │──PL_SEND────────────────────────────►│
 │                │──PL_SEND─────────►│  (also self)     │
 │                │                  │                  │
 │  ◄─APP_VALUE───│  (BEB_DELIVER)    │                  │
 │  (via app.pl)  │                  │                  │
 │                │◄─────────────────│  (BEB_DELIVER)   │
 │◄───────────────────APP_VALUE───────│                  │
 │                │                  │◄─────────────────│
 │◄─────────────────────────────APP_VALUE───────────────│
```

Note: `BEB_BROADCAST` sends to all including self — Process A receives its own broadcast as a `BEB_DELIVER` too.

---

## 2. NNAR Write

Hub writes value `42` to register `"x"`. Process A is the writer (initiator). All 3 processes act as storage nodes.

```
Hub          Proc A (writer)    Proc B             Proc C
 │                │                  │                  │
 │──APP_WRITE─────►│                  │                  │
 │  (register=x,  │                  │                  │
 │   value=42)    │⊕ NNAR_WRITE      │                  │
 │                │⊕ BEB_BROADCAST   │                  │
 │                │   NNAR_INTERNAL_READ(readId=1)       │
 │                │──PL_SEND─────────►│                  │
 │                │──PL_SEND─────────────────────────────►│
 │                │──PL_SEND (self)   │                  │
 │                │                  │                  │
 │                │◄─PL──NNAR_INTERNAL_VALUE(readId=1, ts=0, rank=2, v=undefined)
 │                │◄─PL─────────────────NNAR_INTERNAL_VALUE(readId=1, ts=0, rank=1, v=undefined)
 │                │◄─(self)─NNAR_INTERNAL_VALUE(readId=1, ts=0, rank=3, v=undefined)
 │                │                  │                  │
 │                │  (3 acks > 3/2=1 → majority reached) │
 │                │  highestTs=0, newTimestamp=1         │
 │                │⊕ BEB_BROADCAST   │                  │
 │                │   NNAR_INTERNAL_WRITE(readId=1, ts=1, rank=3, v=42)
 │                │──PL_SEND─────────►│                  │
 │                │──PL_SEND─────────────────────────────►│
 │                │──PL_SEND (self)   │                  │
 │                │                  │                  │
 │                │  (B,C write locally: registerValue=42, timestamp=1)
 │                │◄─PL──NNAR_INTERNAL_ACK(readId=1)     │
 │                │◄─PL─────────────────NNAR_INTERNAL_ACK(readId=1)
 │                │◄─(self)─NNAR_INTERNAL_ACK(readId=1)  │
 │                │                  │                  │
 │                │  (3 acks > 1 → quorum)               │
 │                │⊕ NNAR_WRITE_RETURN                   │
 │                │⊕ PL_SEND APP_WRITE_RETURN            │
 │◄──APP_WRITE────│                  │                  │
 │   RETURN       │                  │                  │
```

---

## 3. NNAR Read (with Write-Back)

Hub reads register `"x"` (previously written to 42 with timestamp=1).

```
Hub          Proc A (reader)    Proc B             Proc C
 │                │                  │                  │
 │──APP_READ──────►│                  │                  │
 │  (register=x)  │⊕ NNAR_READ       │                  │
 │                │⊕ BEB_BROADCAST   │                  │
 │                │   NNAR_INTERNAL_READ(readId=2)       │
 │                │──PL_SEND─────────►│                  │
 │                │──PL_SEND─────────────────────────────►│
 │                │──PL_SEND (self)   │                  │
 │                │                  │                  │
 │                │◄─PL──NNAR_INTERNAL_VALUE(readId=2, ts=1, rank=2, v=42)
 │                │◄─PL─────────────────NNAR_INTERNAL_VALUE(readId=2, ts=1, rank=1, v=42)
 │                │◄─(self)─NNAR_INTERNAL_VALUE(readId=2, ts=1, rank=3, v=42)
 │                │                  │                  │
 │                │  highestTs=1, highestRank=3, highestValue=42
 │                │  (writing=false → write-back with same ts)
 │                │⊕ BEB_BROADCAST   │                  │
 │                │   NNAR_INTERNAL_WRITE(readId=2, ts=1, rank=3, v=42)
 │                │──PL_SEND─────────►│                  │
 │                │──PL_SEND─────────────────────────────►│
 │                │──PL_SEND (self)   │                  │
 │                │                  │                  │
 │                │◄─PL──NNAR_INTERNAL_ACK(readId=2)     │
 │                │◄─PL─────────────────NNAR_INTERNAL_ACK(readId=2)
 │                │◄─(self)─NNAR_INTERNAL_ACK(readId=2)  │
 │                │                  │                  │
 │                │  quorum reached → NNAR_READ_RETURN   │
 │◄──APP_READ─────│                  │                  │
 │   RETURN(42)   │                  │                  │
```

---

## 4. Uniform Consensus — Single Epoch (No Leader Change)

Hub proposes value `7` on topic `"t"`. Process A (rank 3) is leader. No failures.

```
Hub      Proc A (leader)    Proc B             Proc C
 │            │                  │                  │
 │─APP_PROPOSE►│                  │                  │
 │ (topic=t,  │⊕ UC_PROPOSE(7)   │                  │
 │  value=7)  │  val=7           │                  │
 │            │  updateLeader(): self==l, proposed=false
 │            │⊕ EP_PROPOSE(7) → ep[0]              │
 │            │                  │                  │
 │            │⊕ BEB_BROADCAST EP_INTERNAL_READ (ep[0])
 │            │──PL_SEND─────────►│                  │
 │            │──PL_SEND─────────────────────────────►│
 │            │──PL_SEND (self)   │                  │
 │            │                  │                  │
 │            │◄─PL──EP_INTERNAL_STATE(ets=0, value=undefined)
 │            │◄─PL─────────────────EP_INTERNAL_STATE(ets=0, value=undefined)
 │            │◄─(self)─EP_INTERNAL_STATE(ets=0, value=undefined)
 │            │                  │                  │
 │            │  majority states: all undefined → tmpVal stays 7
 │            │⊕ BEB_BROADCAST EP_INTERNAL_WRITE(value=7)
 │            │──PL_SEND─────────►│                  │
 │            │──PL_SEND─────────────────────────────►│
 │            │──PL_SEND (self)   │                  │
 │            │                  │                  │
 │            │  (B,C update state: EpState(ets=0, value=7))
 │            │◄─PL──EP_INTERNAL_ACCEPT               │
 │            │◄─PL─────────────────EP_INTERNAL_ACCEPT│
 │            │◄─(self)─EP_INTERNAL_ACCEPT            │
 │            │                  │                  │
 │            │  3 accepts > 1 → decided             │
 │            │⊕ BEB_BROADCAST EP_INTERNAL_DECIDED   │
 │            │──PL_SEND─────────►│                  │
 │            │──PL_SEND─────────────────────────────►│
 │            │──PL_SEND (self)   │                  │
 │            │                  │                  │
 │            │  (all processes receive DECIDED)      │
 │            │⊕ EP_DECIDE(ets=0, value=7) → UC      │
 │            │  decided=true                        │
 │            │⊕ UC_DECIDE(7) → app                  │
 │◄─APP_DECIDE─│                  │                  │
 │  (value=7) │                  │                  │
```

---

## 5. Uniform Consensus — Leader Change Mid-Flight

Process A (rank 3) crashes after starting EP. Process B (rank 2) becomes new leader.

```
 Proc A (crashed)  Proc B (new leader)  Proc C       EPFD/ELD
        │                 │                 │              │
        │── EP_PROPOSE(7) ─►│ (A proposes)    │              │
        │⊕ BEB_BROADCAST EP_INTERNAL_READ    │              │
        │──────────PL to B──►│                │              │
        │  [A CRASHES]        │                │              │
        X                    │                │              │
                             │         EPFD timeout, no reply from A
                             │                │◄─EPFD_SUSPECT(A)
                             │◄───────────────────ELD_TRUST(B)
                             │⊕ EC: self==trusted, ts=lastTs+3
                             │⊕ BEB_BROADCAST EC_INTERNAL_NEW_EPOCH(ts=N)
                             │──────────────────────────────►│
                             │                │              │
                             │◄─────────────────EC_INTERNAL_NEW_EPOCH ack
                             │  lastTs=N → EC_START_EPOCH(N, B) to UC
                             │⊕ UC: EC_START_EPOCH received
                             │  newTs=N, newL=B
                             │⊕ EP_ABORT → ep[0]
                             │⊕ EP_ABORTED(ets=0, state=EpState(0,undefined)) → UC
                             │  [ep[0] partially ran: B may have accepted value 7 from A's BEB]
                             │                │
                             │  ets=N, l=B, proposed=false
                             │  addEpAbstractions(initialState)  ← ep[N] created
                             │  updateLeader(): self==l → proposed=true
                             │⊕ EP_PROPOSE(7) → ep[N]
                             │                │
                             │⊕ BEB_BROADCAST EP_INTERNAL_READ (ep[N])
                             │──────────────────────────────►│
                             │◄─PL──────────────EP_INTERNAL_STATE(ets=0, value=7)
                             │◄─(self)─EP_INTERNAL_STATE(ets=0, value=undefined)
                             │                │
                             │  highest state has value=7 (from C which accepted A's write)
                             │  tmpVal = 7   ← inherits previous partial progress!
                             │⊕ BEB_BROADCAST EP_INTERNAL_WRITE(value=7)
                             │ ... (normal acceptance flow) ...
                             │⊕ UC_DECIDE(7) → app
```

Key insight: even though A crashed, B's read phase discovered that C had already accepted value `7`. B is forced to propose `7` — this is why UC is uniform (the decided value is preserved across leader changes).
