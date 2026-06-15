# Prompt 2 — Key decisions

Before finalizing the plan, the agent asked three questions that shape the whole
architecture. My answers (verbatim):

### Language
**Go** — matches the `golang-pro` skill and the concurrent-worker model.

### Inter-component communication
> go with your recommended approach of the postgres work-queue, but later indicate
> that the more production realistic approach is the dedicated broker

*(This was later revised — see `03-steering-and-corrections.md`.)*

### NLP / LLM
> mock + ollama option. i do have an api key for ollama, so you don't need to run
> any ollama process locally.

### How it was used
These answers became the "Confirmed decisions" section of the plan. The Ollama
answer led to a real `Classifier` implementation against the hosted Ollama endpoint
(`https://ollama.com`) selectable via `LLM_PROVIDER=ollama`, with the deterministic
mock as the key-free default. Because the provider is Ollama (not Claude), the
agent deliberately did **not** pull in the Claude API skill.
