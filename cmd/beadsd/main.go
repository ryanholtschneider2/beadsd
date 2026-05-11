// beadsd - Simple beads orchestration daemon
//
// Monitors a workspace for ready beads issues and spawns Claude workers.
// Only processes issues that are children of in-progress epics.
// Detects idle/dead workers and resumes them using session IDs.
//
// Usage:
//
//	beadsd [--workspace /path/to/workspace] [--max-workers 3] [--poll-interval 30s]
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
)

// Config holds daemon configuration
type Config struct {
	Workspace         string
	MaxWorkers        int
	PollInterval      time.Duration
	IdleTimeout       time.Duration
	StateFile         string
	MaxFailedNudges   int           // rocks-project-w1f: kill+reset after N failed nudges in a row
	IdleKillThreshold time.Duration // rocks-project-w1f: hard timeout — kill regardless of nudge state
}

// Dependency represents a beads dependency
type Dependency struct {
	ID             string `json:"id"`
	DependencyType string `json:"dependency_type"`
	IssueType      string `json:"issue_type"`
}

// Issue represents a beads issue
type Issue struct {
	ID             string       `json:"id"`
	Title          string       `json:"title"`
	Status         string       `json:"status"`
	Priority       int          `json:"priority"`
	Assignee       string       `json:"assignee"`
	IssueType      string       `json:"issue_type"`
	ExternalRef    string       `json:"external_ref"`
	Labels         []string     `json:"labels"`
	Notes          string       `json:"notes"`
	Dependencies   []Dependency `json:"dependencies"`
	DependencyType string       `json:"dependency_type"` // "parent-child" or "blocks" when in dependents list
}

// GetEpicParent returns the ID of the epic this issue depends on (if any)
func (i *Issue) GetEpicParent() string {
	for _, dep := range i.Dependencies {
		if dep.IssueType == "epic" {
			return dep.ID
		}
	}
	return ""
}

// GetWorkflow returns the workflow type from the notes field ("software" or "general")
func (i *Issue) GetWorkflow() string {
	if strings.Contains(i.Notes, "workflow:general") {
		return "general"
	}
	return "software"
}

// SlashCommand returns the appropriate slash command for this workflow type
func (i *Issue) SlashCommand() string {
	if i.GetWorkflow() == "general" {
		return "/beads-task"
	}
	return "/beads-issue"
}

// Worker represents an active Claude worker
type Worker struct {
	IssueID          string    `json:"issue_id"`
	EpicID           string    `json:"epic_id"`
	SessionName      string    `json:"session_name"` // tmux session (epic-level)
	WindowName       string    `json:"window_name"`  // tmux window (worker-level)
	SessionID        string    `json:"session_id"`   // Claude session ID for resume
	StartedAt        time.Time `json:"started_at"`
	LastActive       time.Time `json:"last_active"`
	ResumeErrors     int       `json:"resume_errors"`      // consecutive resume failures
	FailedNudges     int       `json:"failed_nudges"`      // rocks-project-w1f: consecutive failed nudge attempts
	LastNudgeAt      time.Time `json:"last_nudge_at"`      // when we last sent a nudge (to discount nudge-induced window activity)
	PreNudgeActivity time.Time `json:"pre_nudge_activity"` // window activity snapshot BEFORE last nudge
}

// DispatchEntry is a queued request to spawn a worker on a specific issue at
// (or after) a scheduled time. Written by `beadsd dispatch` and drained by the
// daemon's patrol loop.
type DispatchEntry struct {
	IssueID      string    `json:"issue_id"`
	ScheduledFor time.Time `json:"scheduled_for"`
	Skill        string    `json:"skill"`
	QueuedAt     time.Time `json:"queued_at"`
}

// Daemon manages the orchestration loop
type Daemon struct {
	config        Config
	workers       map[string]*Worker // issueID -> Worker
	epicWorkflows map[string]string  // epicID -> workflow type ("software" or "general")
	mu            sync.RWMutex
	stop          chan struct{}
}

func main() {
	// Subcommand dispatch: `beadsd dispatch [--time MMDDYYYY-HH:MM] [--skill CMD] <issue-id>...`
	// Writes entries to .beads/dispatch-queue.json; the running daemon picks them up on patrol.
	if len(os.Args) > 1 && os.Args[1] == "dispatch" {
		runDispatchCommand(os.Args[2:])
		return
	}

	config := parseFlags()

	// Ensure workspace exists
	if _, err := os.Stat(config.Workspace); os.IsNotExist(err) {
		log.Fatalf("Workspace does not exist: %s", config.Workspace)
	}

	// Check for .beads directory
	beadsDir := filepath.Join(config.Workspace, ".beads")
	if _, err := os.Stat(beadsDir); os.IsNotExist(err) {
		log.Fatalf("No .beads directory found in workspace: %s", config.Workspace)
	}

	// Setup logging to both stdout and file
	logFile := filepath.Join(beadsDir, "daemon.log")
	f, err := os.OpenFile(logFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}
	defer f.Close()
	multiWriter := io.MultiWriter(os.Stdout, f)
	log.SetOutput(multiWriter)

	daemon := NewDaemon(config)

	// Handle signals for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutting down...")
		daemon.Stop()
	}()

	log.Printf("beadsd starting in %s (max workers: %d, poll: %s)",
		config.Workspace, config.MaxWorkers, config.PollInterval)

	daemon.Run()
}

func parseFlags() Config {
	workspace := flag.String("workspace", ".", "Workspace root directory")
	maxWorkers := flag.Int("max-workers", 3, "Maximum concurrent workers")
	pollInterval := flag.Duration("poll-interval", 30*time.Second, "Poll interval for checking ready issues")
	idleTimeout := flag.Duration("idle-timeout", 10*time.Minute, "Time before considering a worker idle")
	maxFailedNudges := flag.Int("max-failed-nudges", 3, "Kill+reset a worker after this many consecutive failed nudges (rocks-project-w1f)")
	idleKillThreshold := flag.Duration("idle-kill-threshold", 60*time.Minute, "Hard timeout — kill+reset any worker idle longer than this regardless of nudge state (rocks-project-w1f)")

	flag.Parse()

	// Resolve workspace to absolute path
	absWorkspace, err := filepath.Abs(*workspace)
	if err != nil {
		log.Fatalf("Failed to resolve workspace path: %v", err)
	}

	return Config{
		Workspace:         absWorkspace,
		MaxWorkers:        *maxWorkers,
		PollInterval:      *pollInterval,
		IdleTimeout:       *idleTimeout,
		MaxFailedNudges:   *maxFailedNudges,
		IdleKillThreshold: *idleKillThreshold,
		StateFile:         filepath.Join(absWorkspace, ".beads", "daemon-state.json"),
	}
}

func NewDaemon(config Config) *Daemon {
	d := &Daemon{
		config:        config,
		workers:       make(map[string]*Worker),
		epicWorkflows: make(map[string]string),
		stop:          make(chan struct{}),
	}
	d.loadState()
	return d
}

func (d *Daemon) Run() {
	ticker := time.NewTicker(d.config.PollInterval)
	defer ticker.Stop()

	// Initial run
	d.patrol()

	for {
		select {
		case <-ticker.C:
			d.patrol()
		case <-d.stop:
			d.saveState()
			return
		}
	}
}

func (d *Daemon) Stop() {
	close(d.stop)
}

// patrol is the main loop iteration
func (d *Daemon) patrol() {
	log.Println("Patrol: checking for work...")

	// 1. Check health of existing workers
	d.checkWorkerHealth()

	// 2. Check if any in-progress epics are complete (all children closed)
	d.checkEpicCompletion()

	// 3. Drain dispatch queue (scheduled / manual dispatches)
	d.processDispatchQueue()

	// 4. Find ready issues (children of in-progress epics)
	readyIssues := d.findReadyIssues()

	// 5. Spawn workers for unassigned ready issues
	d.spawnWorkers(readyIssues)

	// 6. Save state
	d.saveState()

	log.Printf("Patrol complete: %d active workers", len(d.workers))
}

// findReadyIssues returns issues that are ready and children of in-progress epics
func (d *Daemon) findReadyIssues() []Issue {
	// Get in-progress epics first
	inProgressEpics := d.getInProgressEpics()
	if len(inProgressEpics) == 0 {
		log.Println("No in-progress epics found")
		return nil
	}

	// For each in-progress epic, get ready issues under it
	var allReady []Issue
	for _, epic := range inProgressEpics {
		// Cache the epic's workflow type
		d.epicWorkflows[epic.ID] = epic.GetWorkflow()

		issues := d.getReadyIssuesForEpic(epic.ID)
		for i := range issues {
			// Store epic ID for tmux session grouping
			issues[i].Dependencies = []Dependency{{ID: epic.ID, IssueType: "epic"}}
		}
		allReady = append(allReady, issues...)
	}

	log.Printf("Found %d ready issues under in-progress epics", len(allReady))
	return allReady
}

func (d *Daemon) getInProgressEpics() []Issue {
	cmd := exec.Command("bd", "list", "--type=epic", "--status=in_progress", "--json")
	cmd.Dir = d.config.Workspace

	output, err := cmd.Output()
	if err != nil {
		log.Printf("Failed to list in-progress epics: %v", err)
		return nil
	}

	return parseIssuesJSON(output)
}

// checkEpicCompletion checks if any in-progress epics have all children closed
func (d *Daemon) checkEpicCompletion() {
	epics := d.getInProgressEpics()

	if len(epics) == 0 {
		// Only log this occasionally to avoid spam
		return
	}

	log.Printf("Checking completion for %d in-progress epics", len(epics))

	for _, epic := range epics {
		allDependents := d.getEpicChildren(epic.ID)

		// Filter to only parent-child relationships (exclude "blocks" which are downstream epics)
		var children []Issue
		for _, dep := range allDependents {
			if dep.DependencyType == "parent-child" {
				children = append(children, dep)
			}
		}

		if len(children) == 0 {
			log.Printf("Epic %s has no parent-child dependents (total dependents: %d)", epic.ID, len(allDependents))
			continue // No children yet
		}

		allClosed := true
		var openChildren []string
		var closedChildren []string
		for _, child := range children {
			if child.Status != "closed" {
				allClosed = false
				openChildren = append(openChildren, child.ID)
			} else {
				closedChildren = append(closedChildren, child.ID)
			}
		}

		if allClosed {
			log.Printf("Epic %s complete! All %d children closed: %v", epic.ID, len(children), closedChildren)
			d.closeEpic(epic)
			d.notifyEpicComplete(epic)
		} else {
			// Always log progress for in-progress epics
			log.Printf("Epic %s: %d/%d children closed (open: %v)", epic.ID, len(closedChildren), len(children), openChildren)
		}
	}
}

// getEpicChildren returns all children of an epic (any status)
// Uses bd show --json and extracts the dependents field
func (d *Daemon) getEpicChildren(epicID string) []Issue {
	cmd := exec.Command("bd", "show", epicID, "--json")
	cmd.Dir = d.config.Workspace

	output, err := cmd.Output()
	if err != nil {
		log.Printf("Failed to get epic %s: %v", epicID, err)
		return nil
	}

	// bd show --json returns an array with one element containing dependents
	var epics []struct {
		Dependents []Issue `json:"dependents"`
	}
	if err := json.Unmarshal(output, &epics); err != nil {
		log.Printf("Failed to parse epic JSON: %v", err)
		return nil
	}

	if len(epics) == 0 {
		return nil
	}

	return epics[0].Dependents
}

// closeEpic marks an epic as closed
func (d *Daemon) closeEpic(epic Issue) {
	cmd := exec.Command("bd", "close", epic.ID, "--reason", "All children completed")
	cmd.Dir = d.config.Workspace

	if err := cmd.Run(); err != nil {
		log.Printf("Failed to close epic %s: %v", epic.ID, err)
	} else {
		log.Printf("Closed epic %s: %s", epic.ID, epic.Title)
	}
}

// notifyEpicComplete sends a desktop notification
func (d *Daemon) notifyEpicComplete(epic Issue) {
	// Use notify-send for Ubuntu/GNOME notifications
	// --urgency=critical makes it persist until dismissed
	cmd := exec.Command("notify-send",
		"--urgency=critical",
		"--icon=emblem-default",
		"🎉 Epic Complete!",
		fmt.Sprintf("%s: %s", epic.ID, epic.Title))

	if err := cmd.Run(); err != nil {
		log.Printf("Failed to send notification: %v", err)
	}
}

func (d *Daemon) getReadyIssuesForEpic(epicID string) []Issue {
	cmd := exec.Command("bd", "ready", "--unassigned", "--parent", epicID, "--json")
	cmd.Dir = d.config.Workspace

	output, err := cmd.Output()
	if err != nil {
		log.Printf("Failed to get ready issues for epic %s: %v", epicID, err)
		return nil
	}

	// Exclude sub-epics — epics need decomposition into children, not a /beads-issue worker.
	all := parseIssuesJSON(output)
	filtered := make([]Issue, 0, len(all))
	for _, issue := range all {
		if issue.IssueType == "epic" {
			log.Printf("Skipping sub-epic %s under %s: epics are not implemented by workers", issue.ID, epicID)
			continue
		}
		filtered = append(filtered, issue)
	}
	return filtered
}

func parseIssuesJSON(data []byte) []Issue {
	// bd outputs JSONL (one JSON object per line)
	var issues []Issue
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		var issue Issue
		if err := json.Unmarshal([]byte(line), &issue); err != nil {
			// Try parsing as array
			var arr []Issue
			if err := json.Unmarshal(data, &arr); err == nil {
				return arr
			}
			log.Printf("Failed to parse issue JSON: %v", err)
			continue
		}
		issues = append(issues, issue)
	}
	return issues
}

// spawnWorkers creates workers for ready issues up to max capacity
func (d *Daemon) spawnWorkers(issues []Issue) {
	d.mu.Lock()
	defer d.mu.Unlock()

	activeCount := len(d.workers)

	for _, issue := range issues {
		if activeCount >= d.config.MaxWorkers {
			log.Printf("At max workers (%d), skipping remaining issues", d.config.MaxWorkers)
			break
		}

		// Skip if already have a worker for this issue
		if _, exists := d.workers[issue.ID]; exists {
			log.Printf("Skipping %s: already have a worker in map", issue.ID)
			continue
		}

		// Double-check issue status before spawning (prevents race with bd ready cache)
		if d.isIssueInProgress(issue.ID) {
			log.Printf("Skipping %s: already in_progress (claimed by another process)", issue.ID)
			continue
		}

		// Spawn worker
		worker, err := d.spawnWorker(issue)
		if err != nil {
			log.Printf("Failed to spawn worker for %s: %v", issue.ID, err)
			continue
		}

		d.workers[issue.ID] = worker
		activeCount++
		log.Printf("Spawned worker %s for issue %s: %s", worker.SessionName, issue.ID, issue.Title)
	}
}

func (d *Daemon) spawnWorker(issue Issue) (*Worker, error) {
	sessionID := uuid.New().String()
	epicID := issue.GetEpicParent()

	// Use full IDs - tmux supports long names and truncation causes collisions
	windowName := issue.ID
	sessionName := fmt.Sprintf("beadsd-%s", epicID)

	// Claim the issue
	claimCmd := exec.Command("bd", "update", issue.ID,
		"--assignee", windowName,
		"--status", "in_progress",
		"--external-ref", fmt.Sprintf("session:%s", sessionID))
	claimCmd.Dir = d.config.Workspace
	if err := claimCmd.Run(); err != nil {
		return nil, fmt.Errorf("failed to claim issue: %w", err)
	}

	// Determine slash command based on epic's workflow type
	slashCmd := "/beads-issue"
	if workflow, ok := d.epicWorkflows[epicID]; ok && workflow == "general" {
		slashCmd = "/beads-task"
	}

	// Build the Claude command (interactive session with initial prompt)
	// --dangerously-skip-permissions bypasses trust prompt for automated workers
	// Use script -q to force PTY allocation for smooth streaming output
	claudeCmd := fmt.Sprintf("cd %s && script -q -c 'claude --dangerously-skip-permissions --session-id %s \"%s %s\"' /dev/null; echo 'Claude exited with code '$?; read -p 'Press enter to close...'",
		d.config.Workspace, sessionID, slashCmd, issue.ID)

	// Check if epic's tmux session exists
	if d.tmuxSessionExists(sessionName) {
		// Add as new window to existing epic session
		tmuxCmd := exec.Command("tmux", "new-window", "-t", sessionName, "-n", windowName, "bash", "-c", claudeCmd)
		if err := tmuxCmd.Run(); err != nil {
			return nil, fmt.Errorf("failed to add tmux window: %w", err)
		}
	} else {
		// Create new epic session with first worker
		tmuxCmd := exec.Command("tmux", "new-session", "-d", "-s", sessionName, "-n", windowName, "bash", "-c", claudeCmd)
		if err := tmuxCmd.Run(); err != nil {
			return nil, fmt.Errorf("failed to spawn tmux session: %w", err)
		}
	}

	return &Worker{
		IssueID:     issue.ID,
		EpicID:      epicID,
		SessionName: sessionName,
		WindowName:  windowName,
		SessionID:   sessionID,
		StartedAt:   time.Now(),
		LastActive:  time.Now(),
	}, nil
}

// checkWorkerHealth monitors existing workers and handles dead/idle ones
func (d *Daemon) checkWorkerHealth() {
	d.mu.Lock()
	defer d.mu.Unlock()

	for issueID, worker := range d.workers {
		// Check if tmux window exists (session:window format)
		windowTarget := fmt.Sprintf("%s:%s", worker.SessionName, worker.WindowName)

		windowExists := d.tmuxWindowExists(worker.SessionName, worker.WindowName)
		claudeRunning := windowExists && d.isClaudeRunning(worker.SessionName, worker.WindowName)

		// First check: Is the issue already closed?
		// If closed, remove worker regardless of whether Claude is still running
		// (user may have closed issue manually while session is open)
		if d.isIssueClosed(issueID) {
			log.Printf("Worker %s: issue %s is closed, removing from tracking", worker.WindowName, issueID)
			// Don't kill the window - let user keep it open for reference
			delete(d.workers, issueID)
			continue
		}

		if !windowExists || !claudeRunning {
			// Guard: don't act on workers that just started (allow 60s for Claude to boot)
			if time.Since(worker.StartedAt) < 60*time.Second {
				log.Printf("Worker %s for issue %s is still booting (started %s ago), skipping health check",
					worker.WindowName, issueID, time.Since(worker.StartedAt).Round(time.Second))
				continue
			}

			// Window died or Claude exited but issue not closed - resume
			reason := "window died"
			if windowExists && !claudeRunning {
				reason = "Claude exited"
				// Kill the lingering bash window before resuming
				d.killTmuxWindow(worker.SessionName, worker.WindowName)
			}
			log.Printf("Worker %s %s for issue %s, resuming...", worker.WindowName, reason, issueID)
			if err := d.resumeWorker(worker); err != nil {
				worker.ResumeErrors++
				log.Printf("Failed to resume worker (attempt %d/3): %v", worker.ResumeErrors, err)
				if worker.ResumeErrors >= 3 {
					log.Printf("Worker %s failed to resume 3 times, releasing issue %s", worker.WindowName, issueID)
					d.clearAssignee(issueID)
					delete(d.workers, issueID)
				} else {
					log.Printf("Keeping issue %s claimed to prevent duplicate spawn. Will retry next patrol.", issueID)
				}
			} else {
				// Resume succeeded — reset error count and update start time so boot guard applies
				worker.ResumeErrors = 0
				worker.StartedAt = time.Now()
				worker.LastActive = time.Now()
			}
			continue
		}

		// Check if window is idle (no output for too long).
		// rocks-project-w1f: layered escalation — soft path is nudge, hard path
		// is unconditional kill+reset after IdleKillThreshold so a stuck worker
		// can never permanently block its slot.
		lastActivity := d.getWindowLastActivity(windowTarget)
		// Discount nudge-induced activity. tmux's window_activity updates on any
		// pane output, including our own send-keys echo. If the most recent
		// activity timestamp falls inside the window where our last nudge could
		// have produced it (and nothing newer has happened since), fall back to
		// the pre-nudge snapshot. Without this the hard IdleKillThreshold can
		// never fire on a wedged worker because every patrol's nudge resets the
		// clock.
		if !worker.LastNudgeAt.IsZero() && !worker.PreNudgeActivity.IsZero() &&
			!lastActivity.After(worker.LastNudgeAt.Add(5*time.Second)) {
			lastActivity = worker.PreNudgeActivity
		}
		idleDuration := time.Since(lastActivity)

		// Hard timeout: kill+reset regardless of nudge state. This catches the
		// case where nudges are silently failing or the worker is wedged in a
		// way that ignores stdin.
		if idleDuration > d.config.IdleKillThreshold {
			d.killAndResetWorker(
				issueID,
				worker,
				fmt.Sprintf("hard idle timeout (%s > %s)",
					idleDuration.Round(time.Second), d.config.IdleKillThreshold),
			)
			continue
		}

		// Soft path: nudge once per idle window, escalate after N consecutive failures.
		if idleDuration > d.config.IdleTimeout {
			log.Printf("Worker %s idle for %s, nudging (failed nudges: %d/%d)...",
				worker.WindowName, idleDuration.Round(time.Second),
				worker.FailedNudges, d.config.MaxFailedNudges)
			if err := d.nudgeWorker(worker); err != nil {
				worker.FailedNudges++
				if worker.FailedNudges >= d.config.MaxFailedNudges {
					d.killAndResetWorker(
						issueID,
						worker,
						fmt.Sprintf("%d consecutive failed nudges", worker.FailedNudges),
					)
					continue
				}
			} else {
				// Successful nudge resets the counter. The worker may still
				// not respond, but the hard idle timeout will catch that.
				worker.FailedNudges = 0
			}
			worker.LastActive = time.Now() // Reset to avoid re-nudging next patrol
		}
	}
}

func (d *Daemon) tmuxSessionExists(name string) bool {
	cmd := exec.Command("tmux", "has-session", "-t", name)
	return cmd.Run() == nil
}

func (d *Daemon) killTmuxWindow(sessionName, windowName string) {
	target := fmt.Sprintf("%s:%s", sessionName, windowName)
	cmd := exec.Command("tmux", "kill-window", "-t", target)
	if err := cmd.Run(); err != nil {
		log.Printf("Failed to kill tmux window %s: %v", target, err)
	}
}

func (d *Daemon) tmuxWindowExists(sessionName, windowName string) bool {
	// List windows in the session and check if our window name exists
	cmd := exec.Command("tmux", "list-windows", "-t", sessionName, "-F", "#{window_name}")
	output, err := cmd.Output()
	if err != nil {
		return false
	}

	// Check each line for exact match
	for _, line := range strings.Split(strings.TrimSpace(string(output)), "\n") {
		if line == windowName {
			return true
		}
	}
	return false
}

// isClaudeRunning checks if Claude is actually running in the tmux window
// (not just that the window exists with bash after Claude exited)
func (d *Daemon) isClaudeRunning(sessionName, windowName string) bool {
	target := fmt.Sprintf("%s:%s", sessionName, windowName)

	// Check pane_current_command for known Claude process names
	cmd := exec.Command("tmux", "display-message", "-t", target, "-p", "#{pane_current_command}")
	output, err := cmd.Output()
	if err != nil {
		return false
	}
	proc := strings.TrimSpace(string(output))
	// "script" wraps Claude (PTY allocation), "node" is Claude's runtime,
	// "claude" is the direct process name, "bash" could be startup phase
	if proc == "script" || proc == "node" || proc == "claude" {
		return true
	}

	// Fallback: check if any claude/node process exists in the pane's process tree
	// This catches cases where pane_current_command reports a different name
	pidCmd := exec.Command("tmux", "display-message", "-t", target, "-p", "#{pane_pid}")
	pidOutput, err := pidCmd.Output()
	if err != nil {
		return false
	}
	pid := strings.TrimSpace(string(pidOutput))
	if pid == "" {
		return false
	}

	// Check if any descendant process is claude or node
	psCmd := exec.Command("bash", "-c", fmt.Sprintf("pstree -p %s 2>/dev/null | grep -qE 'claude|node'", pid))
	return psCmd.Run() == nil
}

func (d *Daemon) getWindowLastActivity(windowTarget string) time.Time {
	// Get last activity time from tmux window
	cmd := exec.Command("tmux", "display-message", "-t", windowTarget, "-p", "#{window_activity}")
	output, err := cmd.Output()
	if err != nil {
		return time.Now() // Assume active if we can't check
	}

	timestamp := strings.TrimSpace(string(output))
	if timestamp == "" {
		return time.Now()
	}

	// tmux returns Unix timestamp
	var unixTime int64
	if _, err := fmt.Sscanf(timestamp, "%d", &unixTime); err != nil {
		return time.Now()
	}

	return time.Unix(unixTime, 0)
}

func (d *Daemon) getSessionLastActivity(name string) time.Time {
	// Get last activity time from tmux
	cmd := exec.Command("tmux", "display-message", "-t", name, "-p", "#{session_activity}")
	output, err := cmd.Output()
	if err != nil {
		return time.Now() // Assume active if we can't check
	}

	timestamp := strings.TrimSpace(string(output))
	if timestamp == "" {
		return time.Now()
	}

	// tmux returns Unix timestamp
	var unixTime int64
	if _, err := fmt.Sscanf(timestamp, "%d", &unixTime); err != nil {
		return time.Now()
	}

	return time.Unix(unixTime, 0)
}

func (d *Daemon) isIssueClosed(issueID string) bool {
	cmd := exec.Command("bd", "show", issueID, "--json")
	cmd.Dir = d.config.Workspace

	output, err := cmd.Output()
	if err != nil {
		return false
	}

	// bd show --json returns an array
	var issues []Issue
	if err := json.Unmarshal(output, &issues); err != nil {
		return false
	}

	if len(issues) == 0 {
		return false
	}

	return issues[0].Status == "closed"
}

// isIssueInProgress checks if an issue is already claimed/in_progress
func (d *Daemon) isIssueInProgress(issueID string) bool {
	cmd := exec.Command("bd", "show", issueID, "--json")
	cmd.Dir = d.config.Workspace

	output, err := cmd.Output()
	if err != nil {
		return false
	}

	var issues []Issue
	if err := json.Unmarshal(output, &issues); err != nil {
		return false
	}

	if len(issues) == 0 {
		return false
	}

	return issues[0].Status == "in_progress"
}

func (d *Daemon) resumeWorker(worker *Worker) error {
	// Resume using the stored session ID
	// --dangerously-skip-permissions bypasses trust prompt for automated workers
	// Use script -q to force PTY allocation for smooth streaming output
	claudeCmd := fmt.Sprintf("cd %s && script -q -c 'claude --dangerously-skip-permissions --resume %s' /dev/null",
		d.config.Workspace, worker.SessionID)

	// Check if epic's tmux session still exists
	if d.tmuxSessionExists(worker.SessionName) {
		// Add as new window to existing epic session
		tmuxCmd := exec.Command("tmux", "new-window", "-t", worker.SessionName, "-n", worker.WindowName, "bash", "-c", claudeCmd)
		return tmuxCmd.Run()
	}

	// Create new epic session with this worker
	tmuxCmd := exec.Command("tmux", "new-session", "-d", "-s", worker.SessionName, "-n", worker.WindowName, "bash", "-c", claudeCmd)
	return tmuxCmd.Run()
}

// resolveWindowID looks up the tmux @window-id for a (session, window-name) pair.
// Targeting by @id avoids the dot-as-pane-separator parse in "session:window.pane"
// syntax for window names that contain periods.
func (d *Daemon) resolveWindowID(session, windowName string) (string, error) {
	out, err := exec.Command("tmux", "list-windows", "-t", session,
		"-F", "#{window_id} #{window_name}").Output()
	if err != nil {
		return "", fmt.Errorf("list windows in %s: %w", session, err)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		parts := strings.SplitN(line, " ", 2)
		if len(parts) == 2 && parts[1] == windowName {
			return parts[0], nil
		}
	}
	return "", fmt.Errorf("window %q not found in session %s", windowName, session)
}

// nudgeWorker sends a status-check prompt to the worker's tmux window.
// Returns nil on success, or an error if either send-keys command failed.
// Callers track the error for escalation (rocks-project-w1f).
func (d *Daemon) nudgeWorker(worker *Worker) error {
	// Resolve window name to its tmux @window-id. Window names containing "."
	// (e.g. "pd-n44.1") break the default session:window target syntax because
	// tmux parses the dot as a window.pane separator. @window-id is dot-safe.
	windowTarget, err := d.resolveWindowID(worker.SessionName, worker.WindowName)
	if err != nil {
		log.Printf("Failed to nudge worker %s: %v", worker.WindowName, err)
		return err
	}

	// Modal gate: if the pane is blocked on a modal prompt (resume-session
	// dialog, numbered-select, etc.), send-keys text will just pile into the
	// input buffer without being processed by Claude. Return an error so the
	// caller escalates straight to kill+reset instead of sending yet another
	// status-check line.
	if paneContent, capErr := d.capturePane(windowTarget); capErr == nil {
		if modal, blocked := isPaneBlockedByModal(paneContent); blocked {
			log.Printf("Worker %s blocked by modal %q — skipping nudge, escalating",
				worker.WindowName, modal)
			return fmt.Errorf("pane blocked by modal: %s", modal)
		}
	}

	// Snapshot pre-nudge activity so checkWorkerHealth can discount the
	// activity our own send-keys will trigger on the next patrol.
	worker.PreNudgeActivity = d.getWindowLastActivity(windowTarget)
	worker.LastNudgeAt = time.Now()

	message := fmt.Sprintf("Status check: Are you still working on %s? Use 'bd close %s' when done.",
		worker.IssueID, worker.IssueID)

	cmd := exec.Command("tmux", "send-keys", "-t", windowTarget, "-l", message)
	if err := cmd.Run(); err != nil {
		log.Printf("Failed to nudge worker %s: %v", worker.WindowName, err)
		return fmt.Errorf("send message to %s: %w", windowTarget, err)
	}
	// Send Enter separately since -l flag treats everything as literal text
	cmd = exec.Command("tmux", "send-keys", "-t", windowTarget, "Enter")
	if err := cmd.Run(); err != nil {
		log.Printf("Failed to nudge worker %s: %v", worker.WindowName, err)
		return fmt.Errorf("send Enter to %s: %w", windowTarget, err)
	}
	return nil
}

// killAndResetWorker kills the worker's tmux window, releases its claim on the
// beads issue, and removes it from tracking. Used when escalation thresholds
// are crossed (rocks-project-w1f) so a stuck worker frees up its slot for the
// dispatch queue. Caller must hold d.mu.
func (d *Daemon) killAndResetWorker(issueID string, worker *Worker, reason string) {
	log.Printf("ESCALATING worker %s (issue %s): %s — killing tmux window and releasing assignee",
		worker.WindowName, issueID, reason)
	d.killTmuxWindow(worker.SessionName, worker.WindowName)
	d.clearAssignee(issueID)
	delete(d.workers, issueID)
}

func (d *Daemon) clearAssignee(issueID string) {
	cmd := exec.Command("bd", "update", issueID, "--assignee", "", "--status", "open")
	cmd.Dir = d.config.Workspace
	cmd.Run() // Ignore errors
}

// State persistence
func (d *Daemon) loadState() {
	data, err := os.ReadFile(d.config.StateFile)
	if err != nil {
		return // No state file yet
	}

	var workers map[string]*Worker
	if err := json.Unmarshal(data, &workers); err != nil {
		log.Printf("Failed to load state: %v", err)
		return
	}

	d.workers = workers
	log.Printf("Loaded state: %d workers", len(workers))
}

func (d *Daemon) saveState() {
	d.mu.RLock()
	defer d.mu.RUnlock()

	data, err := json.MarshalIndent(d.workers, "", "  ")
	if err != nil {
		log.Printf("Failed to marshal state: %v", err)
		return
	}

	if err := os.WriteFile(d.config.StateFile, data, 0644); err != nil {
		log.Printf("Failed to save state: %v", err)
	}
}

// dispatchTimeLayout is the expected format for --time on `beadsd dispatch`.
// MMDDYYYY-HH:MM, 24-hour. Example: "04212026-15:30".
const dispatchTimeLayout = "01022006-15:04"

// runDispatchCommand implements the `beadsd dispatch` CLI subcommand.
func runDispatchCommand(args []string) {
	fs := flag.NewFlagSet("dispatch", flag.ExitOnError)
	timeFlag := fs.String("time", "", "Schedule dispatch for MMDDYYYY-HH:MM in EST (24-hour). Default: now.")
	skillFlag := fs.String("skill", "", "Slash command to invoke (default: /beads-issue, or /beads-task for general workflow issues)")
	workspaceFlag := fs.String("workspace", ".", "Workspace root directory (must contain .beads/)")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: beadsd dispatch [--time MMDDYYYY-HH:MM] [--skill CMD] [--workspace DIR] <issue-id>...\n\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		os.Exit(2)
	}
	issueIDs := fs.Args()
	if len(issueIDs) == 0 {
		fs.Usage()
		os.Exit(2)
	}

	var scheduledFor time.Time
	if *timeFlag == "" {
		scheduledFor = time.Now()
	} else {
		est := time.FixedZone("EST", -5*3600)
		t, err := time.ParseInLocation(dispatchTimeLayout, *timeFlag, est)
		if err != nil {
			log.Fatalf("Invalid --time %q (expected MMDDYYYY-HH:MM, e.g. 04212026-15:30): %v", *timeFlag, err)
		}
		scheduledFor = t
	}

	absWorkspace, err := filepath.Abs(*workspaceFlag)
	if err != nil {
		log.Fatalf("Failed to resolve workspace path: %v", err)
	}
	beadsDir := filepath.Join(absWorkspace, ".beads")
	if _, err := os.Stat(beadsDir); os.IsNotExist(err) {
		log.Fatalf("No .beads directory in workspace: %s", absWorkspace)
	}
	queuePath := filepath.Join(beadsDir, "dispatch-queue.json")

	entries := loadDispatchQueue(queuePath)
	now := time.Now()
	for _, id := range issueIDs {
		entries = append(entries, DispatchEntry{
			IssueID:      id,
			ScheduledFor: scheduledFor,
			Skill:        *skillFlag,
			QueuedAt:     now,
		})
	}
	if err := saveDispatchQueue(queuePath, entries); err != nil {
		log.Fatalf("Failed to write dispatch queue: %v", err)
	}

	skillLabel := *skillFlag
	if skillLabel == "" {
		skillLabel = "(auto)"
	}
	for _, id := range issueIDs {
		fmt.Printf("Queued dispatch: %s at %s (skill: %s)\n",
			id, scheduledFor.Format(time.RFC3339), skillLabel)
	}
}

func loadDispatchQueue(path string) []DispatchEntry {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var entries []DispatchEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		log.Printf("Failed to parse dispatch queue %s: %v", path, err)
		return nil
	}
	return entries
}

func saveDispatchQueue(path string, entries []DispatchEntry) error {
	if len(entries) == 0 {
		// Remove the file entirely so an empty queue has no lingering state.
		if _, err := os.Stat(path); os.IsNotExist(err) {
			return nil
		}
		return os.Remove(path)
	}
	data, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

// processDispatchQueue drains due entries from .beads/dispatch-queue.json and
// spawns workers for them. Unspawnable entries (at max capacity, spawn error)
// are left in the queue for a later patrol.
func (d *Daemon) processDispatchQueue() {
	queuePath := filepath.Join(d.config.Workspace, ".beads", "dispatch-queue.json")
	entries := loadDispatchQueue(queuePath)
	if len(entries) == 0 {
		return
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	now := time.Now()
	var remaining []DispatchEntry
	for _, entry := range entries {
		if entry.ScheduledFor.After(now) {
			remaining = append(remaining, entry)
			continue
		}
		if len(d.workers) >= d.config.MaxWorkers {
			log.Printf("Dispatch queue: at max workers, deferring %s", entry.IssueID)
			remaining = append(remaining, entry)
			continue
		}
		if _, exists := d.workers[entry.IssueID]; exists {
			log.Printf("Dispatch queue: %s already has a worker, dropping entry", entry.IssueID)
			continue
		}
		if d.isIssueClosed(entry.IssueID) {
			log.Printf("Dispatch queue: %s is closed, dropping entry", entry.IssueID)
			continue
		}
		if d.isIssueInProgress(entry.IssueID) {
			log.Printf("Dispatch queue: %s already in_progress elsewhere, dropping entry", entry.IssueID)
			continue
		}
		worker, err := d.spawnDispatchedWorker(entry.IssueID, entry.Skill)
		if err != nil {
			log.Printf("Dispatch queue: failed to spawn %s: %v (will retry)", entry.IssueID, err)
			remaining = append(remaining, entry)
			continue
		}
		d.workers[entry.IssueID] = worker
		log.Printf("Dispatch queue: spawned worker for %s (scheduled %s)",
			entry.IssueID, entry.ScheduledFor.Format(time.RFC3339))
	}

	if err := saveDispatchQueue(queuePath, remaining); err != nil {
		log.Printf("Failed to save dispatch queue: %v", err)
	}
}

// spawnDispatchedWorker spawns a worker for an ad-hoc dispatched issue. Unlike
// spawnWorker it does not require the issue's parent epic to be in_progress —
// standalone issues go under the "beadsd-dispatch" tmux session; issues that
// do have an epic parent still group under beadsd-<epicID>.
func (d *Daemon) spawnDispatchedWorker(issueID, skill string) (*Worker, error) {
	cmd := exec.Command("bd", "show", issueID, "--json")
	cmd.Dir = d.config.Workspace
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("bd show %s: %w", issueID, err)
	}
	var issues []Issue
	if err := json.Unmarshal(output, &issues); err != nil {
		return nil, fmt.Errorf("parse bd show output: %w", err)
	}
	if len(issues) == 0 {
		return nil, fmt.Errorf("issue %s not found", issueID)
	}
	issue := issues[0]

	sessionID := uuid.New().String()
	epicID := issue.GetEpicParent()
	windowName := issue.ID
	sessionName := "beadsd-dispatch"
	if epicID != "" {
		sessionName = fmt.Sprintf("beadsd-%s", epicID)
	}

	slashCmd := skill
	if slashCmd == "" {
		slashCmd = issue.SlashCommand()
	}

	claimCmd := exec.Command("bd", "update", issueID,
		"--assignee", windowName,
		"--status", "in_progress",
		"--external-ref", fmt.Sprintf("session:%s", sessionID))
	claimCmd.Dir = d.config.Workspace
	if err := claimCmd.Run(); err != nil {
		return nil, fmt.Errorf("claim issue: %w", err)
	}

	claudeCmd := fmt.Sprintf("cd %s && script -q -c 'claude --dangerously-skip-permissions --session-id %s \"%s %s\"' /dev/null; echo 'Claude exited with code '$?; read -p 'Press enter to close...'",
		d.config.Workspace, sessionID, slashCmd, issueID)

	if d.tmuxSessionExists(sessionName) {
		tmuxCmd := exec.Command("tmux", "new-window", "-t", sessionName, "-n", windowName, "bash", "-c", claudeCmd)
		if err := tmuxCmd.Run(); err != nil {
			return nil, fmt.Errorf("add tmux window: %w", err)
		}
	} else {
		tmuxCmd := exec.Command("tmux", "new-session", "-d", "-s", sessionName, "-n", windowName, "bash", "-c", claudeCmd)
		if err := tmuxCmd.Run(); err != nil {
			return nil, fmt.Errorf("spawn tmux session: %w", err)
		}
	}

	return &Worker{
		IssueID:     issue.ID,
		EpicID:      epicID,
		SessionName: sessionName,
		WindowName:  windowName,
		SessionID:   sessionID,
		StartedAt:   time.Now(),
		LastActive:  time.Now(),
	}, nil
}

// capturePane returns the currently visible content of a tmux pane. Used to
// detect modal prompts that would swallow nudge keystrokes.
func (d *Daemon) capturePane(target string) (string, error) {
	out, err := exec.Command("tmux", "capture-pane", "-p", "-t", target).Output()
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// isPaneBlockedByModal scans visible pane content for known Claude Code modal
// prompts. When a modal is up, the composer/Enter is intercepted and any
// send-keys input just piles into the input buffer unprocessed — the caller
// should escalate straight to kill+reset instead of nudging again.
func isPaneBlockedByModal(content string) (string, bool) {
	modals := []string{
		"Resume from summary",
		"Resume full session as-is",
		"Enter to confirm · Esc to cancel",
	}
	for _, m := range modals {
		if strings.Contains(content, m) {
			return m, true
		}
	}
	return "", false
}
