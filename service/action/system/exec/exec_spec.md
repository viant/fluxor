Executes one or more shell commands in a specific working directory. Calls are stateless; for every command in this call the tool sets cwd = workdir automatically. Do not include “cd”
in commands.

Behavior

- Stateless per call; no cwd is carried between calls.
- Each command runs with cwd=workdir; commands are independent unless you chain them yourself (e.g., “a && b”).
- Rejects commands starting with “cd” (the tool manages cwd).
- Prefer fast search with ripgrep (rg); avoid non-portable flags like “--search-path”.
- Guardrails: avoid long-running operations; set a small timeout to bound runtime. The tool may block known long-running commands without a timeout.
- Optional: abortOnError stops after the first non‑zero exit; env injects per-call environment variables.
- Output includes per-command {input, output, stderr, status} and aggregated stdout/stderr; very large outputs may be truncated with a note.
- Do not use \`ls -R\`, \`find\`, or \`grep\` - these are slow in large repos. Use \`rg\` and \`rg --files\`.
- Never guess workdir - always confirm with user
- Never use workdir as '.'

Validation

- workdir must exist and be accessible (typically within the active workspace root).
- Relative paths in commands are resolved against workdir.
- If a command is denied (e.g., starts with “cd” or is clearly long-running without timeout), the tool returns a corrective error message.

Examples

- Correct (list files in a subdir and read a file chunk):
    - workdir: “/Users/awitas/go/src/github.com/viant/datly”
    - commands:
    - “rg --files v2/json”
    - “sed -n '1,200p' v2/json/meta/struct.go”
- Incorrect (and why):
    - commands: [“cd v2/json”, “rg --files”]
    - Error: Remove “cd”; this tool already runs commands in workdir. Use “rg --files v2/json”.