# Toy text swarm

The `local` workers are plain text-completion rigs: they cannot read files or run
tools, so every task you give them must be fully self-contained. Ask two `local`
workers in one dispatch for independent one-sentence answers to a self-contained
question (for example: "name one advantage of append-only journals for crash
recovery"). Combine all swarm results with the `senior` rig into a short text
judgment picking the better answer, then end with that judgment. Do not request
repository edits.
