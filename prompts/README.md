# AI Proficiency — How this project was built with AI

This solution was built collaboratively with an AI coding agent (Claude Code),
used **deliberately and accountably**: I drove the architectural decisions, the
agent did research/scaffolding/implementation under a plan I approved, and I
reviewed every step. This file documents how AI was used; the actual prompts are
captured in the numbered files alongside it.

## Workflow

1. **Plan-first.** The agent read the assignment, explored the available skills,
   and produced a written plan ([`docs/PLAN.md`](../docs/PLAN.md)) **before writing
   any code**. I reviewed and approved it explicitly.
2. **Decisions stayed with me.** Key choices were made via direct questions, not
   assumed. I chose Go, chose the messaging approach, and chose the LLM strategy
   (see `02-key-decisions.md`). When I reconsidered the transport mid-plan
   (Postgres-queue → **Redis Streams**), the agent revised the plan and the
   architecture docs accordingly (`03-steering-and-corrections.md`).
3. **Verification, not vibes.** The agent ran the real stack via Docker Compose,
   executed the demo, and ran the full unit + integration test suite (with the
   race detector) — fixing real bugs it found along the way (e.g. a Dockerfile
   `ENTRYPOINT`/`command` interaction and a concurrent-migration race).
4. **Accountability.** Nothing was merged blind: the plan was approved, the diffs
   were reviewed, and the behaviour was demonstrated end-to-end.

## What AI did well here
- Fast, idiomatic scaffolding of a multi-service Go codebase.
- Catching and fixing its own integration bugs by actually running the system.
- Keeping the design docs and the code in sync as decisions changed.

## What I kept ownership of
- The architecture and every technology trade-off.
- The decision to switch to Redis Streams for production realism.
- Reviewing correctness of the concurrency/rerun model.

## Files
- `01-initial-task.md` — the original task prompt.
- `02-key-decisions.md` — the decision points and my answers.
- `03-steering-and-corrections.md` — mid-flight steering, corrections, and approvals.
