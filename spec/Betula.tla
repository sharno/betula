---------------------------- MODULE Betula ----------------------------
(***************************************************************************)
(* The betula multi-writer protocol, for one key.                          *)
(*                                                                         *)
(* Writers stamp writes (time, writer id), keep them in flight, and flush  *)
(* them as batches to their own append-only log. A writer's lease holds a  *)
(* floor under every timestamp it may still make durable. Writers can      *)
(* start late and can self-fence (lease expiry), losing in-flight writes,  *)
(* which were never acknowledged.                                          *)
(*                                                                         *)
(* Appliers checkpoint in three steps: read every lease (the tombstone     *)
(* floor), pull log entries beyond the manifest's frontier, and publish a  *)
(* new tree with put-if-absent on the next manifest version (modelled as   *)
(* compare-and-set on mver). Two appliers race; a stale one fails.         *)
(*                                                                         *)
(* The tree is a buffer level over a leaf level. Versions merge            *)
(* newest-wins. A tombstone older than the floor is dropped when the leaf  *)
(* is rewritten.                                                           *)
(*                                                                         *)
(* Safety (Correct): what a reader sees (tree plus unapplied log entries)  *)
(* is the newest durable write.                                            *)
(*                                                                         *)
(* Three switches reproduce bugs: FixDiscovery = FALSE pulls only writers  *)
(* the applier already knew instead of every writer whose lease it read;   *)
(* CarryDown = FALSE rewrites a leaf without carrying down the buffered    *)
(* versions above it; FencedPublish = FALSE publishes without              *)
(* put-if-absent, so a stale applier can overwrite a newer manifest.       *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

Max(a, b) == IF a > b THEN a ELSE b

CONSTANTS Writers, Appliers, MaxTime, MaxWrites, MaxVer, FixDiscovery, CarryDown, FencedPublish

Ops == {"put", "del"}
NONE == [t |-> 0, w |-> 0, op |-> "none"]

VARIABLES
    now,        \* global clock (stands in for the hybrid clock)
    started,    \* writer has opened
    alive,      \* writer may still flush (not self-fenced)
    last,       \* last timestamp time per writer (hybrid clocks are strictly monotonic)
    inflight,   \* stamped, not yet durable
    wlog,       \* per writer: sequence of durable batches (sets of entries)
    lease,      \* per writer: [floor, live]
    writes,     \* number of writes issued (bound)
    buf, leaf,  \* the tree for the one key: buffered version over leaf version
    frontier,   \* per writer: batches applied into the tree
    mver,       \* manifest version
    gcd,        \* per writer: batches deleted by GC (a prefix of the log)
    pc, floor, known, base, snapBuf, snapLeaf, snapFrontier, pulled

vars == <<now, started, alive, last, inflight, wlog, lease, writes, buf, leaf,
          frontier, mver, gcd, pc, floor, known, base, snapBuf, snapLeaf,
          snapFrontier, pulled>>

\* Lexicographic (time, writer): unique per write.
Newer(a, b) == a.t > b.t \/ (a.t = b.t /\ a.w > b.w)

Max2(a, b) == IF a = NONE THEN b ELSE IF b = NONE THEN a
              ELSE IF Newer(a, b) THEN a ELSE b

RECURSIVE NewestOf(_)
NewestOf(S) == IF S = {} THEN NONE
               ELSE LET e == CHOOSE x \in S : TRUE IN Max2(e, NewestOf(S \ {e}))

\* What a reader returns: nothing, or the put itself (each write is unique
\* by timestamp, so comparing entries is stricter than comparing values).
Value(e) == IF e.op \in {"none", "del"} THEN NONE ELSE e

Durable == UNION {UNION {wlog[w][i] : i \in 1..Len(wlog[w])} : w \in Writers}

\* Log entries a reader can still read past frontier f (GC'd ones are gone).
Unapplied(f) == UNION {UNION {wlog[w][i] : i \in (Max(f[w], gcd[w]) + 1)..Len(wlog[w])} : w \in Writers}

Visible == NewestOf({buf, leaf} \cup Unapplied(frontier))

\* A leaf rewrite drops tombstones older than the floor.
Drop(e, fl) == IF e.op = "del" /\ e.t < fl THEN NONE ELSE e

Init ==
    /\ now = 1
    /\ started = [w \in Writers |-> FALSE]
    /\ alive = [w \in Writers |-> FALSE]
    /\ last = [w \in Writers |-> 0]
    /\ inflight = [w \in Writers |-> {}]
    /\ wlog = [w \in Writers |-> <<>>]
    /\ lease = [w \in Writers |-> [floor |-> 0, live |-> FALSE]]
    /\ writes = 0
    /\ buf = NONE /\ leaf = NONE
    /\ frontier = [w \in Writers |-> 0]
    /\ mver = 0
    /\ gcd = [w \in Writers |-> 0]
    /\ pc = [p \in Appliers |-> "idle"]
    /\ floor = [p \in Appliers |-> 0]
    /\ known = [p \in Appliers |-> {}]
    /\ base = [p \in Appliers |-> 0]
    /\ snapBuf = [p \in Appliers |-> NONE]
    /\ snapLeaf = [p \in Appliers |-> NONE]
    /\ snapFrontier = [p \in Appliers |-> [w \in Writers |-> 0]]
    /\ pulled = [p \in Appliers |-> {}]

Writing == <<started, alive, last, inflight, wlog, lease, writes>>
Applying == <<buf, leaf, frontier, mver, gcd, pc, floor, known, base,
              snapBuf, snapLeaf, snapFrontier, pulled>>

Tick == now < MaxTime /\ now' = now + 1 /\ UNCHANGED <<Writing, Applying>>

\* Opening writes the lease before any write.
Start(w) ==
    /\ ~started[w]
    /\ started' = [started EXCEPT ![w] = TRUE]
    /\ alive' = [alive EXCEPT ![w] = TRUE]
    /\ lease' = [lease EXCEPT ![w] = [floor |-> now, live |-> TRUE]]
    /\ UNCHANGED <<now, last, inflight, wlog, writes, Applying>>

Write(w, op) ==
    /\ alive[w] /\ writes < MaxWrites /\ now > last[w]
    /\ inflight' = [inflight EXCEPT ![w] = @ \cup {[t |-> now, w |-> w, op |-> op]}]
    /\ last' = [last EXCEPT ![w] = now]
    /\ writes' = writes + 1
    /\ UNCHANGED <<now, started, alive, wlog, lease, Applying>>

Flush(w) ==
    /\ alive[w] /\ inflight[w] # {}
    /\ wlog' = [wlog EXCEPT ![w] = Append(@, inflight[w])]
    /\ inflight' = [inflight EXCEPT ![w] = {}]
    /\ UNCHANGED <<now, started, alive, last, lease, writes, Applying>>

\* Floor: oldest in-flight timestamp, else the clock (later writes are >= now).
Renew(w) ==
    /\ alive[w]
    /\ lease' = [lease EXCEPT ![w].floor =
            IF inflight[w] = {} THEN now
            ELSE CHOOSE t \in {e.t : e \in inflight[w]} :
                    \A u \in {e.t : e \in inflight[w]} : t <= u]
    /\ UNCHANGED <<now, started, alive, last, inflight, wlog, writes, Applying>>

\* Lease expiry: the writer stops flushing; unflushed writes are lost
\* (never acknowledged).
Expire(w) ==
    /\ alive[w]
    /\ alive' = [alive EXCEPT ![w] = FALSE]
    /\ lease' = [lease EXCEPT ![w].live = FALSE]
    /\ inflight' = [inflight EXCEPT ![w] = {}]
    /\ UNCHANGED <<now, started, last, wlog, writes, Applying>>

\* Background discovery of writers (LIST).
Discover(p) ==
    /\ known[p] # {w \in Writers : started[w]}
    /\ known' = [known EXCEPT ![p] = {w \in Writers : started[w]}]
    /\ UNCHANGED <<now, Writing, buf, leaf, frontier, mver, gcd, pc, floor,
                   base, snapBuf, snapLeaf, snapFrontier, pulled>>

\* Step 1: read every listed writer's lease.
ReadLeases(p) ==
    /\ pc[p] = "idle"
    /\ LET ls == {w \in Writers : started[w]}
           live == {w \in ls : lease[w].live}
       IN /\ floor' = [floor EXCEPT ![p] =
                 CHOOSE f \in {lease[w].floor : w \in live} \cup {now} :
                    \A g \in {lease[w].floor : w \in live} \cup {now} : f <= g]
          /\ known' = IF FixDiscovery THEN [known EXCEPT ![p] = @ \cup ls] ELSE known
    /\ pc' = [pc EXCEPT ![p] = "leased"]
    /\ UNCHANGED <<now, Writing, buf, leaf, frontier, mver, gcd, base, snapBuf,
                   snapLeaf, snapFrontier, pulled>>

\* Step 2: load the manifest and pull known writers' entries past its frontier.
Pull(p) ==
    /\ pc[p] = "leased"
    /\ base' = [base EXCEPT ![p] = mver]
    /\ snapBuf' = [snapBuf EXCEPT ![p] = buf]
    /\ snapLeaf' = [snapLeaf EXCEPT ![p] = leaf]
    /\ snapFrontier' = [snapFrontier EXCEPT ![p] =
          [w \in Writers |-> IF w \in known[p] THEN Len(wlog[w]) ELSE frontier[w]]]
    /\ pulled' = [pulled EXCEPT ![p] =
          UNION {UNION {wlog[w][i] : i \in (Max(frontier[w], gcd[w]) + 1)..Len(wlog[w])} : w \in known[p]}]
    /\ pc' = [pc EXCEPT ![p] = "pulled"]
    /\ UNCHANGED <<now, Writing, buf, leaf, frontier, mver, gcd, floor, known>>

\* Step 3: put-if-absent on the next manifest version. The new versions land
\* in the buffer. Then, optionally, the leaf is rewritten, dropping tombstones
\* older than the floor:
\*   "flush": the buffer overflowed and is carried down into the leaf;
\*   "purge": the leaf is rewritten for tombstones or relocation. With
\*            CarryDown the buffered versions above it come along (the fix);
\*            without, they stay above the rewritten leaf.
Publish(p, mode) ==
    /\ pc[p] = "pulled"
    /\ pc' = [pc EXCEPT ![p] = "idle"]
    /\ IF (mver = base[p] \/ ~FencedPublish) /\ mver < MaxVer
       THEN LET b == Max2(snapBuf[p], NewestOf(pulled[p]))
                carried == Drop(Max2(snapLeaf[p], b), floor[p])
            IN /\ mver' = mver + 1
               /\ frontier' = snapFrontier[p]
               /\ CASE mode = "none"  -> buf' = b /\ leaf' = snapLeaf[p]
                    [] mode = "flush" -> buf' = NONE /\ leaf' = carried
                    [] mode = "purge" /\ CarryDown -> buf' = NONE /\ leaf' = carried
                    [] mode = "purge" -> buf' = b /\ leaf' = Drop(snapLeaf[p], floor[p])
       ELSE UNCHANGED <<buf, leaf, frontier, mver>>
    \* Back to idle: clear the scratch state (keeps the state space small).
    /\ floor' = [floor EXCEPT ![p] = 0]
    /\ base' = [base EXCEPT ![p] = 0]
    /\ snapBuf' = [snapBuf EXCEPT ![p] = NONE]
    /\ snapLeaf' = [snapLeaf EXCEPT ![p] = NONE]
    /\ snapFrontier' = [snapFrontier EXCEPT ![p] = [w \in Writers |-> 0]]
    /\ pulled' = [pulled EXCEPT ![p] = {}]
    /\ UNCHANGED <<now, Writing, known, gcd>>

\* GC deletes log entries the current manifest already holds. (The real GC
\* also waits out a retention period, which only protects readers of older
\* manifests; an applier stalled past it is exactly the stale one modelled.)
GcLog(w) ==
    /\ gcd[w] < frontier[w]
    /\ gcd' = [gcd EXCEPT ![w] = frontier[w]]
    /\ UNCHANGED <<now, Writing, buf, leaf, frontier, mver, pc, floor, known,
                   base, snapBuf, snapLeaf, snapFrontier, pulled>>

Next ==
    \/ Tick
    \/ \E w \in Writers : Start(w) \/ Flush(w) \/ Renew(w) \/ Expire(w) \/ GcLog(w)
    \/ \E w \in Writers, op \in Ops : Write(w, op)
    \/ \E p \in Appliers : Discover(p) \/ ReadLeases(p) \/ Pull(p)
                         \/ \E m \in {"none", "flush", "purge"} : Publish(p, m)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
TypeOK ==
    /\ now \in 1..MaxTime
    /\ mver \in 0..MaxVer
    /\ \A w \in Writers : frontier[w] <= Len(wlog[w])

\* Readers see the newest durable write: no lost and no resurrected writes.
Correct == Value(Visible) = Value(NewestOf(Durable))

\* The tree never holds a version newer than anything durable (no invention).
TreeFromLogs == \A e \in {buf, leaf} : e = NONE \/ e \in Durable
=============================================================================
