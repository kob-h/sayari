# Prompt 1 — Initial task (planning)

The session began in **plan mode** (no code until the plan is approved). Verbatim
prompt:

> i have this home assignment described in the document
> `@Senior_SWE_Home_Assignment.pdf`.
> As you'll see, it contains all the information you need to solve it successfully,
> the best and cleanest way possible. take careful look at the examples,
> requirements, 3 main parts with subsections in each, deliverable and evaluation
> criteria. take all into consideration to help me get the best solution possible
> for this assignment.
> note that you can help the following skills here `@.claude/skills`.
> Take your time to carefully plan each part of the assignment, think step by step
> and adhere to the requirements of the home assignment as best as you can and come
> up with a thorough plan which you'll implement later.

### How it was used
The agent extracted the PDF text, read the two available skills (`golang-pro`,
`architecture-designer`), and produced a structured plan covering all three parts
(architecture design, trade-offs/edge-cases, working POC) and every deliverable.
No implementation happened until the plan was approved.
