# beadsd - Simple Beads Orchestration Daemon

A lightweight Go daemon that monitors a workspace for ready beads issues and spawns Claude workers.

## Features

- **Scoped to workspace**: One daemon per mono-repo copy, only manages local issues
- **Epic-aware**: Only spawns workers for issues that are children of in-progress epics
- **Crash recovery**: Detects dead workers and resumes them using stored session IDs
- **Idle detection**: Nudges workers that have been idle too long
- **State persistence**: Survives daemon restarts

## Installation

```bash
cd daemon
make build
# Binary at bin/beadsd

# Or install to GOPATH
make install
```

## Usage

```bash
# Run in a workspace
beadsd --workspace /path/to/monorepo

# With options
beadsd \
  --workspace /path/to/monorepo \
  --max-workers 3 \
  --poll-interval 30s \
  --idle-timeout 10m
```

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--workspace` | `.` | Workspace root directory (must have .beads/) |
| `--max-workers` | `3` | Maximum concurrent Claude workers |
| `--poll-interval` | `30s` | How often to check for ready issues |
| `--idle-timeout` | `10m` | Time before nudging an idle worker |

## How It Works

### Patrol Loop

Every poll interval, the daemon:

1. **Checks worker health** - Detects dead/idle sessions
2. **Finds ready issues** - Runs `bd ready --unassigned`
3. **Filters by epic** - Only issues with in-progress epic parents
4. **Spawns workers** - Up to max-workers limit

### Worker Lifecycle

```
Ready Issue Found
       │
       ▼
┌─────────────────┐
│ Claim Issue     │  bd update <id> --assignee --status in_progress
│ Store Session ID│  bd update <id> --external-ref session:<uuid>
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Spawn tmux      │  tmux new-session -d -s beadsd-<id>
│ Run Claude      │  claude --session-id <uuid> -p '/beads-issue <id>'
└────────┬────────┘
         │
    ┌────┴────┐
    │ Working │
    └────┬────┘
         │
    ┌────┴─────┐        ┌────────────────┐
    │          │        │                │
    ▼          ▼        ▼                │
 Completes   Dies    Goes Idle           │
    │          │        │                │
    ▼          ▼        ▼                │
 bd close   Resume   Nudge ──────────────┘
    │          │
    ▼          │
 Cleanup ◄─────┘
```

### Crash Recovery

When a worker session dies unexpectedly:

1. Daemon detects missing tmux session
2. Checks if issue was closed (success case)
3. If not closed, resumes with `claude --resume <session-id>`
4. Full conversation context is restored

### State File

The daemon persists worker state to `.beads/daemon-state.json`:

```json
{
  "po-abc123": {
    "issue_id": "po-abc123",
    "session_name": "beadsd-po-abc12",
    "session_id": "e0a1966f-e266-4230-b625-72c86f783d25",
    "started_at": "2024-01-24T10:00:00Z",
    "last_active": "2024-01-24T10:30:00Z"
  }
}
```

## Integration with Beads

### Starting Work

To have the daemon pick up work:

1. Create an epic and set it to in-progress:
   ```bash
   bd create --type epic --title "Feature X"
   bd update <epic-id> --status in_progress
   ```

2. Create child tasks:
   ```bash
   bd create --title "Implement component A" --parent <epic-id>
   bd create --title "Implement component B" --parent <epic-id>
   ```

3. The daemon will automatically spawn workers for ready children.

### Pausing Work

To pause the daemon from processing an epic:

```bash
# Move epic back to open (or any status other than in_progress)
bd update <epic-id> --status open
```

Workers for that epic's children won't be spawned until the epic is back to in_progress.

## Multiple Workspaces

Run separate daemons for each workspace:

```bash
# Terminal 1: Main development
beadsd --workspace ~/code/polymer

# Terminal 2: Experimental branch
beadsd --workspace ~/code/polymer-experiment --max-workers 1
```

Each daemon only manages issues in its workspace.

## Logs

The daemon logs to stdout:

```
2024/01/24 10:00:00 beadsd starting in /home/user/code/polymer (max workers: 3, poll: 30s)
2024/01/24 10:00:00 Patrol: checking for work...
2024/01/24 10:00:00 Found 2 ready issues under in-progress epics
2024/01/24 10:00:01 Spawned worker beadsd-po-abc12 for issue po-abc123: Implement feature
2024/01/24 10:00:01 Patrol complete: 1 active workers
```

## Tmux Session Organization

Workers are organized in tmux by epic:
- **Sessions**: One per epic (named `beadsd-<epic-id-prefix>`)
- **Windows**: One per worker/issue (named by issue ID prefix)

This allows easy navigation between workers on the same epic:
```bash
# List all epic sessions
tmux list-sessions | grep beadsd

# Attach to an epic's session
tmux attach -t beadsd-po-abc12

# Cycle between workers in an epic
# (use Ctrl-B n for next window, Ctrl-B p for previous)
```

## Advanced Nudging (Future Enhancement)

The current nudging is simple: send a text message after idle timeout. For more sophisticated nudging, consider:

### LLM-Based Nudging (Gastown Approach)
Instead of a static message, use an LLM to analyze:
1. **Worker's recent output**: Capture last N lines from tmux pane
2. **Issue context**: What the worker was supposed to accomplish
3. **Stuck patterns**: Detect repeated errors, loops, or lack of progress

The LLM could then generate targeted nudges:
- "I see you're getting a 404 error. Have you checked if the endpoint path changed?"
- "You've been modifying the same file for 20 minutes. Would it help to break this into smaller steps?"
- "The build is failing on line 45. Try checking the import statement."

### Escalation Levels
1. **Gentle reminder** (first nudge): "Status check, are you still working?"
2. **Context-aware prompt** (second nudge): LLM analyzes output and suggests next step
3. **Supervisor escalation** (third nudge): Alert a parent agent or human
4. **Auto-reassign** (final): Kill worker, clear assignee, let another worker pick it up

### Implementation Ideas
```go
// Future: LLM-based nudge generation
func (d *Daemon) generateSmartNudge(worker *Worker) string {
    // Capture recent pane output
    output := d.capturePaneOutput(worker, 50) // last 50 lines

    // Get issue context
    issue := d.getIssue(worker.IssueID)

    // Call LLM to analyze and generate nudge
    prompt := fmt.Sprintf(`
        Worker is processing issue: %s
        Description: %s

        Recent terminal output:
        %s

        Generate a helpful nudge message if the worker seems stuck.
        If progress looks normal, respond with "OK".
    `, issue.ID, issue.Title, output)

    return callLLM(prompt)
}
```

### Detecting Stuck States
- **Error loops**: Same error message repeated 3+ times
- **No output**: No new lines in pane for extended period
- **Build failures**: Detecting common failure patterns
- **Prompt waiting**: Claude waiting for input (shows `>` prompt)

## Troubleshooting

### Daemon not spawning workers

1. Check for in-progress epics: `bd list --type=epic --status=in_progress`
2. Check for ready issues: `bd ready --unassigned`
3. Verify issues have parent epic set: `bd show <issue-id>`

### Worker not resuming after crash

1. Check if session ID was stored: `bd show <issue-id>` should show `external-ref: session:<uuid>`
2. Try manual resume: `claude --resume <uuid>`

### Too many workers

Reduce max-workers:
```bash
beadsd --max-workers 1
```
