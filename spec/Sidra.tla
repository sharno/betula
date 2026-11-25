---------------------------- MODULE Sidra ----------------------------
(***************************************************************************)
(* The sidra single-writer protocol, for one key.                          *)
(*                                                                         *)
(* Storage: manifests (put-if-absent by id), one WAL of put-if-absent      *)
(* slots, and a tree. A manifest holds writer_epoch, tree, last_ts,        *)
(* wal_applied (slots up to it are in the tree) and fences (fence slots at *)
(* or below wal_applied; GC never deletes them).                           *)
(*                                                                         *)
(* Opening a writer (Claim, Read, Place):                                  *)
(*   claim:  publish the next manifest with epoch + 1 (last opener wins;   *)
(*           a conflict just retries, so the model takes the step whole).  *)
(*   fence:  read the WAL forward from wal_applied + 1 collecting batches,  *)
(*           put Fence(epoch) in the first empty slot. A slot taken since   *)
(*           the read (batch or older fence) is read on from. A newer      *)
(*           fence means this writer was superseded. Then check: a      *)
(*           manifest id + 1 means a newer writer claimed meanwhile.       *)
(*   clock:  the hybrid clock observes replayed timestamps and last_ts.     *)
(* Its writes start at fence + 1.                                          *)
(*                                                                         *)
(* A writer stamps one batch, then puts it at its next slot (Stamp, Land). *)
(* The two steps are apart, so a put can land arbitrarily late: after a    *)
(* newer writer fenced and after GC. A put on a taken slot means fenced:   *)
(* not acknowledged, the writer stops.                                     *)
(*                                                                         *)
(* Checkpoint (Plan, Publish): take a prefix of the unapplied tail, apply  *)
(* it to the tree (a tombstone is dropped at the leaf), publish manifest   *)
(* id + 1 put-if-absent with wal_applied = end of the prefix, fences plus  *)
(* the fences in it, last_ts = max. A conflict means fenced.               *)
(*                                                                         *)
(* GC deletes WAL slots <= wal_applied except fences.                      *)
(*                                                                         *)
(* A reader loads the latest manifest and reads the WAL from wal_applied+1 *)
(* to the first empty slot, skipping fences.                               *)
(*                                                                         *)
(* Clocks are unconstrained: every reading may be any value.               *)
(*                                                                         *)
(* Switches reproduce mistakes: KeepFences = FALSE (GC deletes fences),    *)
(* FenceOnOpen = FALSE (the new writer opens without a fence),             *)
(* TakeMode = "overshoot" (a partial checkpoint sets wal_applied past what  *)
(* it applied), UseLastTs = FALSE (the new writer ignores last_ts),        *)
(* PutIfAbsent = FALSE (a checkpoint overwrites the manifest blindly),     *)
(* CheckAfterFence = FALSE (the opener never rechecks the manifest).      *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Writers, MaxSlot, MaxWrites, MaxClock, MaxMver,
          KeepFences, FenceOnOpen, TakeMode, UseLastTs, PutIfAbsent, CheckAfterFence

Ops == {"put", "del"}

\* An entry is one write: timestamp, writer, op. NONE is "no version".
NONE == [t |-> 0, w |-> 0, op |-> "none"]

NoRec == [k |-> "none"]
Batch(e) == [k |-> "batch", e |-> e]
Fence(ep) == [k |-> "fence", ep |-> ep]

NoPend == [s |-> 0]
NoCk == [upto |-> 0]

Max(a, b) == IF a > b THEN a ELSE b

\* Lexicographic (time, writer): unique per write.
Newer(a, b) == a.t > b.t \/ (a.t = b.t /\ a.w > b.w)

Newest(S) == IF S = {} THEN NONE
             ELSE CHOOSE e \in S : \A x \in S : ~Newer(x, e)

\* What a reader returns: nothing, or the put itself.
Value(e) == IF e.op \in {"none", "del"} THEN NONE ELSE e

VARIABLES
    wal,        \* slot -> record (NoRec: never written, or deleted by GC)
    mver, man,  \* latest manifest id and contents
    ph,         \* writer phase: idle, reading, placing, open, dead
    ep,         \* writer epoch
    cur,        \* slot being read, then the next slot to write
    wlast,      \* hybrid clock: last timestamp observed or stamped
    wman,       \* the manifest the writer last published (its view of the tree)
    wmid,       \* ... and its id
    tail,       \* records {s, e} past wman.wal_applied that the writer holds
    wfen,       \* fence slots past wman.wal_applied that the writer knows
    pend,       \* stamped batch awaiting its put: {s, e}
    ck,         \* planned checkpoint: {take, upto}
    writes,     \* writes stamped (bound)
    lastAck,    \* ghost: the last acknowledged entry
    mono,       \* ghost: every ack had a higher timestamp than the previous
    placed,     \* ghost: fences ever placed, as {slot, epoch}
    zombie      \* ghost: a put landed at or after a newer writer's fence

vars == <<wal, mver, man, ph, ep, cur, wlast, wman, wmid, tail, wfen, pend, ck,
          writes, lastAck, mono, placed, zombie>>

Init ==
    /\ wal = [s \in 1..MaxSlot |-> NoRec]
    /\ mver = 0
    /\ man = [epoch |-> 0, tree |-> NONE, last |-> 0, applied |-> 0, fences |-> {}]
    /\ ph = [w \in Writers |-> "idle"]
    /\ ep = [w \in Writers |-> 0]
    /\ cur = [w \in Writers |-> 0]
    /\ wlast = [w \in Writers |-> 0]
    /\ wman = [w \in Writers |-> man]
    /\ wmid = [w \in Writers |-> 0]
    /\ tail = [w \in Writers |-> {}]
    /\ wfen = [w \in Writers |-> {}]
    /\ pend = [w \in Writers |-> NoPend]
    /\ ck = [w \in Writers |-> NoCk]
    /\ writes = 0
    /\ lastAck = NONE
    /\ mono = TRUE
    /\ placed = {}
    /\ zombie = FALSE

\* The hybrid clock's next timestamp, reading clock value c.
Hlc(w, c) == Max(c, wlast[w] + 1)

-----------------------------------------------------------------------------
\* Opening

\* Typestate: idle -> reading.
Claim(w) ==
    /\ ph[w] = "idle" /\ mver < MaxMver
    /\ mver' = mver + 1
    /\ man' = [man EXCEPT !.epoch = @ + 1]
    /\ ph' = [ph EXCEPT ![w] = "reading"]
    /\ ep' = [ep EXCEPT ![w] = man.epoch + 1]
    /\ cur' = [cur EXCEPT ![w] = man.applied + 1]
    /\ wlast' = [wlast EXCEPT ![w] = IF UseLastTs THEN man.last ELSE 0]
    /\ wman' = [wman EXCEPT ![w] = [man EXCEPT !.epoch = @ + 1]]
    /\ wmid' = [wmid EXCEPT ![w] = mver + 1]
    /\ UNCHANGED <<wal, tail, wfen, pend, ck, writes, lastAck, mono, placed, zombie>>

\* reading -> reading (slot taken) | placing (slot empty) | dead (superseded).
\* Without FenceOnOpen the writer opens at the empty slot instead.
Read(w) ==
    /\ ph[w] = "reading" /\ cur[w] <= MaxSlot
    /\ LET r == wal[cur[w]] IN
       \/ /\ r = NoRec
          /\ ph' = [ph EXCEPT ![w] = IF FenceOnOpen THEN "placing" ELSE "open"]
          /\ UNCHANGED <<cur, wlast, tail, wfen>>
       \/ /\ r.k = "batch"
          /\ tail' = [tail EXCEPT ![w] = @ \cup {[s |-> cur[w], e |-> r.e]}]
          /\ wlast' = [wlast EXCEPT ![w] = Max(@, r.e.t)]
          /\ cur' = [cur EXCEPT ![w] = @ + 1]
          /\ UNCHANGED <<ph, wfen>>
       \/ /\ r.k = "fence" /\ r.ep < ep[w]
          /\ wfen' = [wfen EXCEPT ![w] = @ \cup {cur[w]}]
          /\ cur' = [cur EXCEPT ![w] = @ + 1]
          /\ UNCHANGED <<ph, wlast, tail>>
       \/ /\ r.k = "fence" /\ r.ep > ep[w]
          /\ ph' = [ph EXCEPT ![w] = "dead"]
          /\ UNCHANGED <<cur, wlast, tail, wfen>>
    /\ UNCHANGED <<wal, mver, man, ep, wman, wmid, pend, ck, writes, lastAck, mono,
                   placed, zombie>>

\* placing -> open (fence in place) | reading (the slot is taken now).
\* The put comes later than the read: the slot may have been written and
\* deleted in between (a stall past GC retention). Then the fence lands in a
\* deleted slot, below a newer writer's fence, and this writer's later puts
\* find deleted slots too. With CheckAfterFence the opener looks for a newer
\* manifest after its fence and stops if there is one.
Place(w) ==
    /\ ph[w] = "placing"
    /\ IF wal[cur[w]] = NoRec
       THEN /\ wal' = [wal EXCEPT ![cur[w]] = Fence(ep[w])]
            /\ placed' = placed \cup {[s |-> cur[w], ep |-> ep[w]]}
            /\ wfen' = [wfen EXCEPT ![w] = @ \cup {cur[w]}]
            /\ cur' = [cur EXCEPT ![w] = @ + 1]
            /\ ph' = [ph EXCEPT ![w] =
                    IF CheckAfterFence /\ mver # wmid[w] THEN "dead" ELSE "open"]
       ELSE /\ ph' = [ph EXCEPT ![w] = "reading"]
            /\ UNCHANGED <<wal, placed, wfen, cur>>
    /\ UNCHANGED <<mver, man, ep, wlast, wman, wmid, tail, pend, ck, writes,
                   lastAck, mono, zombie>>

-----------------------------------------------------------------------------
\* Writing

Stamp(w, op, c) ==
    /\ ph[w] = "open" /\ pend[w] = NoPend /\ writes < MaxWrites
    /\ cur[w] <= MaxSlot
    /\ LET e == [t |-> Hlc(w, c), w |-> w, op |-> op] IN
       /\ pend' = [pend EXCEPT ![w] = [s |-> cur[w], e |-> e]]
       /\ wlast' = [wlast EXCEPT ![w] = e.t]
    /\ writes' = writes + 1
    /\ UNCHANGED <<wal, mver, man, ph, ep, cur, wman, wmid, tail, wfen, ck,
                   lastAck, mono, placed, zombie>>

\* The put-if-absent: acknowledged if the slot is free, fenced otherwise.
Land(w) ==
    /\ ph[w] = "open" /\ pend[w] # NoPend
    /\ LET p == pend[w] IN
       IF wal[p.s] = NoRec
       THEN /\ wal' = [wal EXCEPT ![p.s] = Batch(p.e)]
            /\ tail' = [tail EXCEPT ![w] = @ \cup {p}]
            /\ cur' = [cur EXCEPT ![w] = p.s + 1]
            /\ lastAck' = p.e
            /\ mono' = (mono /\ p.e.t > lastAck.t)
            /\ zombie' = (zombie \/ \E f \in placed : f.ep > ep[w] /\ f.s <= p.s)
            /\ UNCHANGED ph
       ELSE /\ ph' = [ph EXCEPT ![w] = "dead"]
            /\ UNCHANGED <<wal, tail, cur, lastAck, mono, zombie>>
    /\ pend' = [pend EXCEPT ![w] = NoPend]
    /\ UNCHANGED <<mver, man, ep, wlast, wman, wmid, wfen, ck, writes, placed>>

-----------------------------------------------------------------------------
\* Checkpoints

Slots(T) == {r.s : r \in T}
MaxSlotOf(T) == CHOOSE s \in Slots(T) : \A u \in Slots(T) : u <= s

\* A prefix of the tail in slot order, ending at a batch (at least one batch).
Prefixes(w) ==
    { [take |-> {r \in tail[w] : r.s <= u},
       upto |-> IF TakeMode = "overshoot" THEN MaxSlotOf(tail[w]) ELSE u]
      : u \in Slots(tail[w]) }

Plan(w) ==
    /\ ph[w] = "open" /\ ck[w] = NoCk /\ tail[w] # {}
    /\ \E c \in Prefixes(w) : ck' = [ck EXCEPT ![w] = c]
    /\ UNCHANGED <<wal, mver, man, ph, ep, cur, wlast, wman, wmid, tail, wfen,
                   pend, writes, lastAck, mono, placed, zombie>>

\* Leaves drop every tombstone they apply.
ApplyToTree(tree, T) ==
    LET n == Newest({tree} \cup {r.e : r \in T})
    IN IF n.op = "del" THEN NONE ELSE n

Next(w, mf, upto, take) ==
    [epoch |-> ep[w],
     tree |-> ApplyToTree(mf.tree, take),
     last |-> Max(mf.last, Newest({r.e : r \in take}).t),
     applied |-> upto,
     fences |-> mf.fences \cup {f \in wfen[w] : f <= upto}]

\* Put-if-absent on id + 1: a conflict means fenced. Without PutIfAbsent the
\* manifest is overwritten blindly.
Publish(w) ==
    /\ ph[w] = "open" /\ ck[w] # NoCk
    /\ ck' = [ck EXCEPT ![w] = NoCk]
    /\ IF (mver = wmid[w] \/ ~PutIfAbsent) /\ mver < MaxMver
       THEN LET m == Next(w, wman[w], ck[w].upto, ck[w].take) IN
            /\ mver' = mver + 1
            /\ man' = m
            /\ wman' = [wman EXCEPT ![w] = m]
            /\ wmid' = [wmid EXCEPT ![w] = mver + 1]
            /\ tail' = [tail EXCEPT ![w] = @ \ ck[w].take]
            /\ wfen' = [wfen EXCEPT ![w] = {f \in @ : f > ck[w].upto}]
            /\ UNCHANGED ph
       ELSE /\ mver # wmid[w] \/ mver >= MaxMver
            /\ ph' = [ph EXCEPT ![w] = "dead"]
            /\ UNCHANGED <<mver, man, wman, wmid, tail, wfen>>
    /\ UNCHANGED <<wal, ep, cur, wlast, pend, writes, lastAck, mono, placed, zombie>>

-----------------------------------------------------------------------------
\* GC deletes applied WAL slots, except fences.
Gc ==
    LET dead(s) == s <= man.applied /\ (~KeepFences \/ s \notin man.fences)
        wal2 == [s \in 1..MaxSlot |-> IF dead(s) THEN NoRec ELSE wal[s]]
    IN /\ wal2 # wal
       /\ wal' = wal2
       /\ UNCHANGED <<mver, man, ph, ep, cur, wlast, wman, wmid, tail, wfen, pend,
                      ck, writes, lastAck, mono, placed, zombie>>

Step ==
    \/ \E w \in Writers : Claim(w) \/ Read(w) \/ Place(w) \/ Land(w)
                          \/ Plan(w) \/ Publish(w)
    \/ \E w \in Writers, op \in Ops, c \in 1..MaxClock : Stamp(w, op, c)
    \/ Gc

Spec == Init /\ [][Step]_vars

-----------------------------------------------------------------------------
\* What a reader sees: the tree plus the WAL up to the first empty slot.
ReadEnd == CHOOSE s \in (man.applied + 1)..(MaxSlot + 1) :
    /\ (s = MaxSlot + 1 \/ wal[s] = NoRec)
    /\ \A u \in (man.applied + 1)..(s - 1) : wal[u] # NoRec

Visible ==
    Newest({man.tree} \cup {wal[s].e : s \in {u \in (man.applied + 1)..(ReadEnd - 1) :
                                               wal[u].k = "batch"}})

TypeOK ==
    /\ mver \in 0..MaxMver
    /\ man.applied \in 0..MaxSlot

\* Acknowledged durability: a reader sees the last acknowledged write, so
\* none is lost (also after GC) and none is resurrected.
Correct == Value(Visible) = Value(lastAck)

\* A writer that is current has every acknowledged write in its view.
WriterSees == \A w \in Writers :
    (ph[w] = "open" /\ ep[w] = man.epoch) =>
        Value(Newest({wman[w].tree} \cup {r.e : r \in tail[w]})) = Value(lastAck)

\* No put lands at or after a newer writer's fence (so none is acknowledged).
NoZombieAck == ~zombie

\* Timestamps strictly increase across acknowledgements, so across writers.
Monotone == mono
=============================================================================
