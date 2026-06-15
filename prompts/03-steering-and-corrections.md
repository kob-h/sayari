# Prompt 3 — Steering, corrections, and approvals

Mid-flight prompts that changed direction or kept the agent accountable (verbatim).

### Reconsidering the transport → Redis Streams
> on second thought, using redis for the queue messaging might be a better choice,
> because it's relatively easy to run it as part of docker compose and it shows a
> better implementation and understanding of a real life production code. apply the
> needed changes

**Effect:** The plan and architecture were revised from "Postgres-as-queue" to
**Redis Streams** (consumer groups, acks, PEL crash redelivery), with Postgres
kept as the single source of truth. This introduced the at-least-once consistency
model and the reconciler ([ADR-005](../docs/adr/ADR-005-consistency-model.md)).

### Keeping the plan trackable + confirming the diagram
> i want to access this plan, just make sure to keep a copy of your plan markdown
> file so i can track your changes, and also just confirming the mermaid diagram is
> going to be included in the deliverables as well.

**Effect:** The approved plan was committed to the repo at
[`docs/PLAN.md`](../docs/PLAN.md), and the Mermaid architecture diagram is included
in [`docs/architecture.md`](../docs/architecture.md).

### Plan approval
> I confirm your plan with auto-edits

### Identity correction
> i'm currently logged in with gh. the github user is kob-h

**Effect:** The Go module path and repository identity use `github.com/kob-h/…`.

---

## Bugs the agent caught by actually running the system

Deliberate verification (running the stack + tests, not just writing code) surfaced
and fixed real issues:

1. **Dockerfile `ENTRYPOINT` vs `command`.** `ENTRYPOINT ["api"]` plus a compose
   `command: ["extractor"]` ran `api extractor` — so every service ran the API
   binary. Fixed by switching to `CMD`.
2. **Concurrent migration race.** All three services running `CREATE TABLE IF NOT
   EXISTS` on boot raced on Postgres' implicit row type. Fixed with a
   `pg_advisory_lock` around migrations.
3. **Mock extractor false positives.** Entities spanned headings/newlines (e.g.
   `"New Leadership\n\nOn"`). Fixed by splitting on line breaks and filtering
   non-name/role words.
4. **Demo timing for partial rerun.** The classifier finished before the kill;
   fixed by polling until classification is underway, with tuned latency/recovery
   windows, so the resume is reliably demonstrated.
