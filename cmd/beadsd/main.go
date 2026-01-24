// beadsd - Simple beads orchestration daemon
//
// Monitors a workspace for ready beads issues and spawns Claude workers.
// Only processes issues that are children of in-progress epics.
// Detects idle/dead workers and resumes them using session IDs.
//
// Usage:
//   beadsd [--workspace /path/to/workspace] [--max-workers 3] [--poll-interval 30s]
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
	Workspace    string
	MaxWorkers   int
	PollInterval time.Duration
	IdleTimeout  time.Duration
	StateFile    string
}

// Dependency represents a beads dependency
type Dependency struct {
	ID             string `json:"id"`
	DependencyType string `json:"dependency_type"`
	IssueType      string `json:"issue_type"`
}

// Issue represents a beads issue
type Issue struct {
	ID           string       `json:"id"`
	Title        string       `json:"title"`
	Status       string       `json:"status"`
	Priority     int          `json:"priority"`
	Assignee     string       `json:"assignee"`
	IssueType    string       `json:"issue_type"`
	ExternalRef  string       `json:"external_ref"`
	Labels       []string     `json:"labels"`
	Dependencies []Dependency `json:"dependencies"`
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

// Worker represents an active Claude worker
type Worker struct {
	IssueID     string    `json:"issue_id"`
	EpicID      string    `json:"epic_id"`
	SessionName string    `json:"session_name"` // tmux session (epic-level)
	WindowName  string    `json:"window_name"`  // tmux window (worker-level)
	SessionID   string    `json:"session_id"`   // Claude session ID for resume
	StartedAt   time.Time `json:"started_at"`
	LastActive  time.Time `json:"last_active"`
}

// Daemon manages the orchestration loop
type Daemon struct {
	config  Config
	workers map[string]*Worker // issueID -> Worker
	mu      sync.RWMutex
	stop    chan struct{}
}

func main() {
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

	flag.Parse()

	// Resolve workspace to absolute path
	absWorkspace, err := filepath.Abs(*workspace)
	if err != nil {
		log.Fatalf("Failed to resolve workspace path: %v", err)
	}

	return Config{
		Workspace:    absWorkspace,
		MaxWorkers:   *maxWorkers,
		PollInterval: *pollInterval,
		IdleTimeout:  *idleTimeout,
		StateFile:    filepath.Join(absWorkspace, ".beads", "daemon-state.json"),
	}
}

func NewDaemon(config Config) *Daemon {
	d := &Daemon{
		config:  config,
		workers: make(map[string]*Worker),
		stop:    make(chan struct{}),
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

	// 3. Find ready issues (children of in-progress epics)
	readyIssues := d.findReadyIssues()

	// 4. Spawn workers for unassigned ready issues
	d.spawnWorkers(readyIssues)

	// 5. Save state
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

	for _, epic := range epics {
		children := d.getEpicChildren(epic.ID)
		if len(children) == 0 {
			continue // No children yet
		}

		allClosed := true
		for _, child := range children {
			if child.Status != "closed" {
				allClosed = false
				break
			}
		}

		if allClosed {
			log.Printf("Epic %s complete! All %d children closed.", epic.ID, len(children))
			d.closeEpic(epic)
			d.notifyEpicComplete(epic)
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
	cmd := exec.Command("notify-send",
		"--urgency=normal",
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

	return parseIssuesJSON(output)
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

	// Use full ID if shorter than limit, otherwise truncate
	epicSuffix := epicID
	if len(epicID) > 8 {
		epicSuffix = epicID[:8]
	}
	windowName := issue.ID
	if len(issue.ID) > 12 {
		windowName = issue.ID[:12]
	}
	sessionName := fmt.Sprintf("beadsd-%s", epicSuffix)  // tmux session per epic

	// Claim the issue
	claimCmd := exec.Command("bd", "update", issue.ID,
		"--assignee", windowName,
		"--status", "in_progress",
		"--external-ref", fmt.Sprintf("session:%s", sessionID))
	claimCmd.Dir = d.config.Workspace
	if err := claimCmd.Run(); err != nil {
		return nil, fmt.Errorf("failed to claim issue: %w", err)
	}

	// Build the Claude command (interactive session with initial prompt)
	// --dangerously-skip-permissions bypasses trust prompt for automated workers
	// Use script -q to force PTY allocation for smooth streaming output
	claudeCmd := fmt.Sprintf("cd %s && script -q -c 'claude --dangerously-skip-permissions --session-id %s \"/beads-issue %s\"' /dev/null; echo 'Claude exited with code '$?; read -p 'Press enter to close...'",
		d.config.Workspace, sessionID, issue.ID)

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

		if !d.tmuxWindowExists(worker.SessionName, worker.WindowName) {
			// Window died - check if issue was closed
			if d.isIssueClosed(issueID) {
				log.Printf("Worker %s completed issue %s", worker.WindowName, issueID)
				delete(d.workers, issueID)
				continue
			}

			// Window died but issue not closed - resume
			log.Printf("Worker %s died for issue %s, resuming...", worker.WindowName, issueID)
			if err := d.resumeWorker(worker); err != nil {
				log.Printf("Failed to resume worker: %v", err)
				// Clear assignee so it can be picked up again
				d.clearAssignee(issueID)
				delete(d.workers, issueID)
			}
			continue
		}

		// Check if window is idle (no output for too long)
		lastActivity := d.getWindowLastActivity(windowTarget)
		if time.Since(lastActivity) > d.config.IdleTimeout {
			log.Printf("Worker %s idle for %s, nudging...", worker.WindowName, time.Since(lastActivity))
			d.nudgeWorker(worker)
			worker.LastActive = time.Now() // Reset to avoid repeated nudges
		}
	}
}

func (d *Daemon) tmuxSessionExists(name string) bool {
	cmd := exec.Command("tmux", "has-session", "-t", name)
	return cmd.Run() == nil
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

func (d *Daemon) nudgeWorker(worker *Worker) {
	// Send a message to the specific tmux window
	windowTarget := fmt.Sprintf("%s:%s", worker.SessionName, worker.WindowName)
	message := fmt.Sprintf("Status check: Are you still working on %s? Use 'bd close %s' when done.",
		worker.IssueID, worker.IssueID)

	cmd := exec.Command("tmux", "send-keys", "-t", windowTarget, message, "Enter")
	if err := cmd.Run(); err != nil {
		log.Printf("Failed to nudge worker %s: %v", worker.WindowName, err)
	}
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
