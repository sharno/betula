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
(* A checkpoint applies the oldest pulled batches, across writers, up to a  *)
(* cap that the model leaves nondeterministic: any oldest-first prefix of   *)
(* at least one batch. A batch's age is its oldest timestamp raised to its  *)
(* predecessors' in the same log, so every writer's taken batches are a     *)
(* prefix of its log. Ties break on (writer, seq).                          *)
(*                                                                         *)
(* Five switches reproduce bugs: FixDiscovery = FALSE pulls only writers  *)
(* the applier already knew instead of every writer whose lease it read;   *)
(* CarryDown = FALSE rewrites a leaf without carrying down the buffered    *)
(* versions above it; FencedPublish = FALSE publishes without              *)
(* put-if-absent, so a stale applier can overwrite a newer manifest;       *)
(* FixBound = FALSE drops tombstones below the lease floor alone, without  *)
(* the manifest's tombstone bound that new writers move their clocks past  *)
(* (a writer opening after a drop, with a lagging clock, then writes a     *)
(* version older than the dropped tombstone); CapRest = FALSE drops        *)
(* tombstones without capping at the oldest timestamp among the batches a   *)
(* partial checkpoint left out (a left-out older put then resurrects the    *)
(* key once applied).                                                       *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

Max(a, b) == IF a > b THEN a ELSE b

CONSTANTS Writers, Appliers, MaxTime, MaxWrites, MaxVer, FixDiscovery, CarryDown, FencedPublish,
          FixBound, CapRest

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
    pc, floor, known, base, snapBuf, snapLeaf, snapFrontier, snapBound, pulled,
    snapRest    \* oldest timestamp among batches the pull left out (Inf if none)

vars == <<started, ready, alive, last, inflight, wlog, lease, writes,
          buf, leaf, frontier, mver, gcd, tbound, pc, floor, known, base, snapBuf,
          snapLeaf, snapFrontier, snapBound, pulled, snapRest>>

Min(a, b) == IF a < b THEN a ELSE b

\* The hybrid clock's next timestamp for writer w, reading clock value c.
Hlc(w, c) == Max(c, last[w] + 1)

\* "No timestamp": above every real one.
Inf == MaxTime + 3

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

\* Partial checkpoints. A pulled batch is <<writer, seq>> with its age: the
\* oldest timestamp in it, raised to its predecessors' still held in the log.
MinT(b) == CHOOSE t \in {e.t : e \in b} : \A u \in {e.t : e \in b} : t <= u

Held(w) == (Max(frontier[w], gcd[w]) + 1)..Len(wlog[w])

Pend(p) ==
    UNION {{[w |-> w, i |-> i,
             age |-> LET ts == {MinT(wlog[w][j]) : j \in (Max(frontier[w], gcd[w]) + 1)..i}
                     IN CHOOSE a \in ts : \A u \in ts : u <= a]
            : i \in Held(w)} : w \in known[p]}

\* b is no later than c in the checkpoint order (age, writer, seq).
Upto(b, c) == b.age < c.age
              \/ (b.age = c.age /\ (b.w < c.w \/ (b.w = c.w /\ b.i <= c.i)))

\* Every oldest-first prefix of at least one batch (none if nothing pulled).
TakeChoices(p) ==
    IF Pend(p) = {} THEN {{}}
    ELSE {{b \in Pend(p) : Upto(b, c)} : c \in Pend(p)}

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
    /\ snapRest = [p \in Appliers |-> Inf]

Writing == <<started, ready, alive, last, inflight, wlog, lease, writes>>
Applying == <<buf, leaf, frontier, mver, gcd, tbound, pc, floor, known, base,
              snapBuf, snapLeaf, snapFrontier, snapBound, pulled, snapRest>>

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
                   base, snapBuf, snapLeaf, snapFrontier, snapBound, pulled, snapRest>>

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
                   snapLeaf, snapFrontier, snapBound, pulled, snapRest>>

\* Step 2: load the manifest and pull known writers' entries past its frontier,
\* then take the oldest of them (a checkpoint's byte cap, any prefix here). The
\* frontier advances only past the batches taken.
Pull(p) ==
    /\ pc[p] = "leased"
    /\ \E taken \in TakeChoices(p) :
        LET rest == Pend(p) \ taken
        IN /\ snapFrontier' = [snapFrontier EXCEPT ![p] =
                  [w \in Writers |-> IF \E b \in taken : b.w = w
                                     THEN CHOOSE i \in {b.i : b \in {c \in taken : c.w = w}} :
                                              \A j \in {b.i : b \in {c \in taken : c.w = w}} : j <= i
                                     ELSE frontier[w]]]
           /\ pulled' = [pulled EXCEPT ![p] =
                  UNION {wlog[b.w][b.i] : b \in taken}]
           /\ snapRest' = [snapRest EXCEPT ![p] =
                  IF rest = {} THEN Inf
                  ELSE LET ts == {e.t : e \in UNION {wlog[b.w][b.i] : b \in rest}}
                       IN CHOOSE t \in ts : \A u \in ts : t <= u]
    /\ base' = [base EXCEPT ![p] = mver]
    /\ snapBuf' = [snapBuf EXCEPT ![p] = buf]
    /\ snapLeaf' = [snapLeaf EXCEPT ![p] = leaf]
    /\ snapBound' = [snapBound EXCEPT ![p] = tbound]
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
                fb == IF FixBound THEN Min(floor[p], snapBound[p]) ELSE floor[p]
                \* A batch left out may hold a put older than a tombstone dropped
                \* now; it would come back once applied.
                fl == IF CapRest THEN Min(fb, snapRest[p]) ELSE fb
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
    /\ snapRest' = [snapRest EXCEPT ![p] = Inf]
    /\ UNCHANGED <<Writing, known, gcd>>

\* GC deletes log entries the current manifest already holds. (The real GC
\* also waits out a retention period, which only protects readers of older
\* manifests; an applier stalled past it is exactly the stale one modelled.)
GcLog(w) ==
    /\ gcd[w] < frontier[w]
    /\ gcd' = [gcd EXCEPT ![w] = frontier[w]]
    /\ UNCHANGED <<Writing, buf, leaf, frontier, mver, tbound, pc, floor, known,
                   base, snapBuf, snapLeaf, snapFrontier, snapBound, pulled, snapRest>>

Next ==
    \/ \E w \in Writers : Observe(w) \/ Flush(w) \/ Renew(w) \/ Expire(w) \/ GcLog(w)
    \/ \E w \in Writers, c \in Readings : Start(w, c)
    \/ \E w \in Writers, op \in Ops, c \in Readings : Write(w, op, c)
    \/ \E p \in Appliers, c \in 1..MaxTime : ReadLeases(p, c)
    \/ \E p \in Appliers : Discover(p) \/ Pull(p)
                         \/ \E m \in {"none", "flush", "purge"} : Publish(p, m)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
\* State constraint for BugPartial.cfg, which keeps its search to seconds: at
\* most two writes per writer, writer 1 only puts, timestamps up to 3, no
\* lease expiry, no log GC. Enough for the counterexample.
Small == \A w \in Writers :
    LET es == inflight[w] \cup UNION {wlog[w][i] : i \in 1..Len(wlog[w])}
    IN /\ Cardinality(es) <= 2
       /\ \A e \in es : e.t <= 3 /\ (w = 1 => e.op = "put")
       /\ (alive[w] \/ ~started[w])
       /\ gcd[w] = 0

TypeOK ==
    /\ mver \in 0..MaxVer
    /\ \A w \in Writers : frontier[w] <= Len(wlog[w])

\* Readers see the newest durable write: no lost and no resurrected writes.
Correct == Value(Visible) = Value(NewestOf(Durable))

\* The tree never holds a version newer than anything durable (no invention).
TreeFromLogs == \A e \in {buf, leaf} : e = NONE \/ e \in Durable
=============================================================================
