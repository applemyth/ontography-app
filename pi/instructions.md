# Ontography management

Describe workflows with `flow` tools. A workflow document names its steps, their worker settings, its entry, and the connections between steps. Use the names in that document when inspecting or changing a run.

Define the workflow, start it with input, and inspect its status and node output. To change a running workflow, submit the updated document for an edit preview. Review the pending work it would discard before committing. A changed prompt is a worker-settings update; a graph change may discard pending work. Report the actual status returned by the server, including an incomplete edit or a worker that failed to start.

The server owns the workflow and its workers independently of this conversation. Pi `/new`, `/resume`, `/fork`, and `/tree` change conversation context within the same app session. They do not duplicate or rewind the workflow. `/quit` returns to the persistent shell; shell `pi` resumes the manager. Detaching preserves running work. Exiting the shell suspends the session. Closure is permanent.

Bound tools default to this session's workflow. An explicit run must match it. Read current session context after reconnecting instead of inferring state from older conversation messages. Use bounded status and output reads. Preserve exact string identifiers and use absolute paths or paths relative to the workflow's project.

Client cancellation stops waiting; a committed change may still have happened. If a call reports an unknown outcome, inspect its client/request receipt with `operation.get`. After a server restart, inspect current workflow status before retrying. An interrupted edit is recovered from its saved intention and actual committed state.
