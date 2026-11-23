---------------------------- MODULE Sidra ----------------------------
(***************************************************************************)
(* The sidra multi-writer protocol, for one key.                          *)
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
(* Clocks are unconstrained: every action that reads a clock may read any *)
(* value, so skew is unbounded. Timestamps come from hybrid clocks (max of *)
(* the clock reading and the last timestamp plus one), which keeps each    *)
(* writer's own timestamps increasing.                                     *)
(*                                                                         *)
(* Four switches reproduce bugs: FixDiscovery = FALSE pulls only writers  *)
(* the applier already knew instead of every writer whose lease it read;   *)
(* CarryDown = FALSE rewrites a leaf without carrying down the buffered    *)
(* versions above it; FencedPublish = FALSE publishes without              *)
(* put-if-absent, so a stale applier can overwrite a newer manifest;       *)
(* FixBound = FALSE drops tombstones below the lease floor alone, without  *)
(* the manifest's tombstone bound that new writers move their clocks past  *)
(* (a writer opening after a drop, with a lagging clock, then writes a     *)
(* version older than the dropped tombstone).                              *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

Max(a, b) == IF a > b THEN a ELSE b

CONSTANTS Writers, Appliers, MaxTime, MaxWrites, MaxVer, FixDiscovery, CarryDown, FencedPublish,
          FixBound

Ops == {"put", "del"}
NONE == [t |-> 0, w |-> 0, op |-> "none"]

VARIABLES
    started,    \* writer has opened
    ready,      \* writer has read the manifest's tombstone bound (after its lease)
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
    tbound,     \* manifest: tombstone bound
    pc, floor, known, base, snapBuf, snapLeaf, snapFrontier, snapBound, pulled

vars == <<started, ready, alive, last, inflight, wlog, lease, writes,
          buf, leaf, frontier, mver, gcd, tbound, pc, floor, known, base, snapBuf,
          snapLeaf, snapFrontier, snapBound, pulled>>

Min(a, b) == IF a < b THEN a ELSE b

\* The hybrid clock's next timestamp for writer w, reading clock value c.
Hlc(w, c) == Max(c, last[w] + 1)

\* Clock readings, and the range hybrid timestamps may reach.
Readings == 1..MaxTime
Stamp == 1..(MaxTime + 2)

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
    /\ started = [w \in Writers |-> FALSE]
    /\ ready = [w \in Writers |-> FALSE]
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
    /\ tbound = 0
    /\ pc = [p \in Appliers |-> "idle"]
    /\ floor = [p \in Appliers |-> 0]
    /\ known = [p \in Appliers |-> {}]
    /\ base = [p \in Appliers |-> 0]
    /\ snapBuf = [p \in Appliers |-> NONE]
    /\ snapLeaf = [p \in Appliers |-> NONE]
    /\ snapFrontier = [p \in Appliers |-> [w \in Writers |-> 0]]
    /\ snapBound = [p \in Appliers |-> 0]
    /\ pulled = [p \in Appliers |-> {}]

Writing == <<started, ready, alive, last, inflight, wlog, lease, writes>>
Applying == <<buf, leaf, frontier, mver, gcd, tbound, pc, floor, known, base,
              snapBuf, snapLeaf, snapFrontier, snapBound, pulled>>

\* Opening writes the lease before any write.
Start(w, c) ==
    /\ ~started[w]
    /\ started' = [started EXCEPT ![w] = TRUE]
    /\ ready' = [ready EXCEPT ![w] = ~FixBound]
    /\ alive' = [alive EXCEPT ![w] = TRUE]
    /\ last' = [last EXCEPT ![w] = Hlc(w, c)]
    /\ lease' = [lease EXCEPT ![w] = [floor |-> Hlc(w, c), live |-> TRUE]]
    /\ UNCHANGED <<inflight, wlog, writes, Applying>>

\* After its lease, the writer reads the latest manifest and moves its
\* clock past the tombstone bound.
Observe(w) ==
    /\ started[w] /\ alive[w] /\ ~ready[w]
    /\ ready' = [ready EXCEPT ![w] = TRUE]
    /\ last' = [last EXCEPT ![w] = Max(@, tbound)]
    /\ UNCHANGED <<started, alive, inflight, wlog, lease, writes, Applying>>

Write(w, op, c) ==
    /\ alive[w] /\ ready[w] /\ writes < MaxWrites /\ Hlc(w, c) \in Stamp
    /\ inflight' = [inflight EXCEPT ![w] = @ \cup {[t |-> Hlc(w, c), w |-> w, op |-> op]}]
    /\ last' = [last EXCEPT ![w] = Hlc(w, c)]
    /\ writes' = writes + 1
    /\ UNCHANGED <<started, ready, alive, wlog, lease, Applying>>

Flush(w) ==
    /\ alive[w] /\ inflight[w] # {}
    /\ wlog' = [wlog EXCEPT ![w] = Append(@, inflight[w])]
    /\ inflight' = [inflight EXCEPT ![w] = {}]
    /\ UNCHANGED <<started, ready, alive, last, lease, writes, Applying>>

\* Floor: oldest in-flight timestamp, else the next timestamp (every later
\* write is above it). A clock that jumped ahead shows up through writes,
\* which may read any value.
Renew(w) ==
    /\ alive[w]
    /\ lease' = [lease EXCEPT ![w].floor =
            IF inflight[w] = {} THEN last[w] + 1
            ELSE CHOOSE t \in {e.t : e \in inflight[w]} :
                    \A u \in {e.t : e \in inflight[w]} : t <= u]
    /\ UNCHANGED <<started, ready, alive, last, inflight, wlog, writes, Applying>>

\* Lease expiry: the writer stops flushing; unflushed writes are lost
\* (never acknowledged).
Expire(w) ==
    /\ alive[w]
    /\ alive' = [alive EXCEPT ![w] = FALSE]
    /\ lease' = [lease EXCEPT ![w].live = FALSE]
    /\ inflight' = [inflight EXCEPT ![w] = {}]
    /\ UNCHANGED <<started, ready, last, wlog, writes, Applying>>

\* Background discovery of writers (LIST).
Discover(p) ==
    /\ known[p] # {w \in Writers : started[w]}
    /\ known' = [known EXCEPT ![p] = {w \in Writers : started[w]}]
    /\ UNCHANGED <<Writing, buf, leaf, frontier, mver, gcd, tbound, pc, floor,
                   base, snapBuf, snapLeaf, snapFrontier, snapBound, pulled>>

\* Step 1: read every listed writer's lease. `c` is the applier's own clock
\* reading, unconstrained per scan: any skew against the writers.
ReadLeases(p, c) ==
    /\ pc[p] = "idle"
    /\ LET ls == {w \in Writers : started[w]}
           live == {w \in ls : lease[w].live}
       IN /\ floor' = [floor EXCEPT ![p] =
                 CHOOSE f \in {lease[w].floor : w \in live} \cup {c} :
                    \A g \in {lease[w].floor : w \in live} \cup {c} : f <= g]
          /\ known' = IF FixDiscovery THEN [known EXCEPT ![p] = @ \cup ls] ELSE known
    /\ pc' = [pc EXCEPT ![p] = "leased"]
    /\ UNCHANGED <<Writing, buf, leaf, frontier, mver, gcd, tbound, base, snapBuf,
                   snapLeaf, snapFrontier, snapBound, pulled>>

\* Step 2: load the manifest and pull known writers' entries past its frontier.
Pull(p) ==
    /\ pc[p] = "leased"
    /\ base' = [base EXCEPT ![p] = mver]
    /\ snapBuf' = [snapBuf EXCEPT ![p] = buf]
    /\ snapLeaf' = [snapLeaf EXCEPT ![p] = leaf]
    /\ snapBound' = [snapBound EXCEPT ![p] = tbound]
    /\ snapFrontier' = [snapFrontier EXCEPT ![p] =
          [w \in Writers |-> IF w \in known[p] THEN Len(wlog[w]) ELSE frontier[w]]]
    /\ pulled' = [pulled EXCEPT ![p] =
          UNION {UNION {wlog[w][i] : i \in (Max(frontier[w], gcd[w]) + 1)..Len(wlog[w])} : w \in known[p]}]
    /\ pc' = [pc EXCEPT ![p] = "pulled"]
    /\ UNCHANGED <<Writing, buf, leaf, frontier, mver, gcd, tbound, floor, known>>

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
                \* Only below the bound the previous manifest published: a
                \* writer this lease scan missed moved its clock past it.
                fl == IF FixBound THEN Min(floor[p], snapBound[p]) ELSE floor[p]
                carried == Drop(Max2(snapLeaf[p], b), fl)
            IN /\ mver' = mver + 1
               /\ frontier' = snapFrontier[p]
               /\ tbound' = Max(snapBound[p], floor[p])
               /\ CASE mode = "none"  -> buf' = b /\ leaf' = snapLeaf[p]
                    [] mode = "flush" -> buf' = NONE /\ leaf' = carried
                    [] mode = "purge" /\ CarryDown -> buf' = NONE /\ leaf' = carried
                    [] mode = "purge" -> buf' = b /\ leaf' = Drop(snapLeaf[p], fl)
       ELSE UNCHANGED <<buf, leaf, frontier, mver, tbound>>
    \* Back to idle: clear the scratch state (keeps the state space small).
    /\ floor' = [floor EXCEPT ![p] = 0]
    /\ base' = [base EXCEPT ![p] = 0]
    /\ snapBuf' = [snapBuf EXCEPT ![p] = NONE]
    /\ snapLeaf' = [snapLeaf EXCEPT ![p] = NONE]
    /\ snapFrontier' = [snapFrontier EXCEPT ![p] = [w \in Writers |-> 0]]
    /\ snapBound' = [snapBound EXCEPT ![p] = 0]
    /\ pulled' = [pulled EXCEPT ![p] = {}]
    /\ UNCHANGED <<Writing, known, gcd>>

\* GC deletes log entries the current manifest already holds. (The real GC
\* also waits out a retention period, which only protects readers of older
\* manifests; an applier stalled past it is exactly the stale one modelled.)
GcLog(w) ==
    /\ gcd[w] < frontier[w]
    /\ gcd' = [gcd EXCEPT ![w] = frontier[w]]
    /\ UNCHANGED <<Writing, buf, leaf, frontier, mver, tbound, pc, floor, known,
                   base, snapBuf, snapLeaf, snapFrontier, snapBound, pulled>>

Next ==
    \/ \E w \in Writers : Observe(w) \/ Flush(w) \/ Renew(w) \/ Expire(w) \/ GcLog(w)
    \/ \E w \in Writers, c \in Readings : Start(w, c)
    \/ \E w \in Writers, op \in Ops, c \in Readings : Write(w, op, c)
    \/ \E p \in Appliers, c \in 1..MaxTime : ReadLeases(p, c)
    \/ \E p \in Appliers : Discover(p) \/ Pull(p)
                         \/ \E m \in {"none", "flush", "purge"} : Publish(p, m)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
TypeOK ==
    /\ mver \in 0..MaxVer
    /\ \A w \in Writers : frontier[w] <= Len(wlog[w])

\* Readers see the newest durable write: no lost and no resurrected writes.
Correct == Value(Visible) = Value(NewestOf(Durable))

\* The tree never holds a version newer than anything durable (no invention).
TreeFromLogs == \A e \in {buf, leaf} : e = NONE \/ e \in Durable
=============================================================================
