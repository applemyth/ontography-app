//! Claude in the node's terminal. Its hooks report what it is doing, and graph
//! work is typed into its prompt only while it is idle and nobody at the
//! terminal is typing.
//!
//! A delivery reads as a person's message: a short typed request, the
//! recorded envelope as a bracketed paste, then Enter. Claude quotes a long
//! paste as content and follows instructions in it only where the typed
//! message asks, so the request asks it to handle the pasted work. The
//! envelope is marked sent once Claude's UserPromptSubmit hook shows it.

use super::{Started, Status, attempt_id, error, turn_failed};
use crate::{
    Result,
    node_hooks::HookEvent,
    node_tool::{NodeToolContext, Reply},
    terminal::{ClientInput, Terminal},
};
use serde_json::Value;
use std::time::Duration;
use tokio::{
    sync::mpsc,
    time::{Instant, MissedTickBehavior},
};

/// Ctrl+S: Claude stashes the draft in its input box, or restores its one
/// stashed draft into an empty box.
const STASH: &[u8] = b"\x13";
/// Shown beside the input box while a draft is stashed.
const STASHED: &str = "\u{203a} stashed";
/// Typed ahead of the pasted envelope. Plain words: a leading '/', '!', '#'
/// or '@' would make Claude read the line as a command or reference.
const REQUEST: &str = "Handle the graph work pasted here: ";
const PASTE_START: &[u8] = b"\x1b[200~";
const PASTE_END: &[u8] = b"\x1b[201~";

/// The terminal Claude runs in, as delivery uses it.
pub(crate) trait Console {
    /// Writes input on the server's behalf, which is not counted as typing.
    fn send(&self, bytes: &[u8]) -> Result<()>;
    /// Typing received from people attached to the terminal.
    fn typing(&self) -> ClientInput;
    /// The visible screen as plain text.
    fn screen(&self) -> String;
}

impl Console for Terminal {
    fn send(&self, bytes: &[u8]) -> Result<()> {
        self.send_input(bytes.to_vec())
    }

    fn typing(&self) -> ClientInput {
        self.client_input()
    }

    fn screen(&self) -> String {
        self.screen_text()
    }
}

/// How long delivery waits for Claude and for people.
#[derive(Clone, Copy, Debug)]
pub(super) struct Timing {
    /// Without SessionStart by then, the terminal is likely asking whether
    /// to trust the folder.
    pub trust: Duration,
    /// Delivery waits until nobody has typed for this long.
    pub quiet: Duration,
    /// Claude must report a delivered prompt within this time.
    pub accept: Duration,
    /// Lets Claude act on the stash key before more input arrives.
    pub keystroke: Duration,
    /// Lets Claude take in the paste before Enter submits it.
    pub paste: Duration,
    /// How often delivery looks for work and checks on the terminal.
    pub poll: Duration,
}

impl Default for Timing {
    fn default() -> Self {
        Self {
            trust: Duration::from_secs(10),
            quiet: Duration::from_secs(10),
            accept: Duration::from_secs(20),
            keystroke: Duration::from_millis(100),
            paste: Duration::from_millis(150),
            poll: Duration::from_millis(250),
        }
    }
}

/// Delivers graph work to Claude in `console` as `events` report its state,
/// until the hook channel closes or the console refuses input. `report`
/// hears every change of status.
pub(crate) async fn deliver(
    console: &impl Console,
    events: mpsc::Receiver<HookEvent>,
    tools: &NodeToolContext,
    started: Started,
    report: impl FnMut(Status),
) -> Result<()> {
    run(console, events, tools, started, report, Timing::default()).await
}

pub(super) async fn run(
    console: &impl Console,
    mut events: mpsc::Receiver<HookEvent>,
    tools: &NodeToolContext,
    started: Started,
    mut report: impl FnMut(Status),
    timing: Timing,
) -> Result<()> {
    let mut delivery = Delivery {
        console,
        tools,
        started,
        marked: false,
        timing,
        launched: Instant::now(),
        phase: Phase::Starting,
        attended: false,
        pending: None,
        turn: Vec::new(),
        typed: 0,
        restored: false,
        stashed: false,
    };
    let mut poll = tokio::time::interval(timing.poll);
    poll.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut reported = None;
    loop {
        let status = delivery.status();
        if reported != Some(status) {
            reported = Some(status);
            report(status);
        }
        tokio::select! {
            event = events.recv() => {
                let event = event.ok_or_else(|| error("Claude's hook channel closed"))?;
                delivery.observe(event).await?;
            }
            _ = poll.tick() => delivery.poll().await?,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// Until SessionStart. Before a folder is trusted, no hook runs.
    Starting,
    Idle,
    Working,
    AwaitingApproval,
    /// After SessionEnd, until Enter in the terminal resumes Claude.
    Exited,
}

/// Input typed into Claude, awaiting its UserPromptSubmit hook.
struct Pending {
    attempt: String,
    reply: Reply,
    deadline: Instant,
}

struct Delivery<'a, C> {
    console: &'a C,
    tools: &'a NodeToolContext,
    started: Started,
    marked: bool,
    timing: Timing,
    launched: Instant,
    phase: Phase,
    /// Someone at the terminal may be using the input box.
    attended: bool,
    pending: Option<Pending>,
    /// Attempts delivered in Claude's current turn.
    turn: Vec<String>,
    /// Client typing counted at the last submitted prompt.
    typed: u64,
    /// Whether the input box may hold a draft nobody typed since the last
    /// submitted prompt: submitting restores a stashed draft into the box,
    /// and input Claude never accepted may remain there.
    restored: bool,
    /// Whether delivery stashed a draft since the last submitted prompt.
    stashed: bool,
}

impl<C: Console> Delivery<'_, C> {
    async fn observe(&mut self, event: HookEvent) -> Result<()> {
        match event.name.as_str() {
            // Compaction starts a session too, even in the middle of a turn.
            "SessionStart" if event.input["source"] != "compact" => {
                self.phase = Phase::Idle;
                self.turn.clear();
                self.abandon("Claude restarted before accepting the delivered input")
                    .await?;
            }
            "SessionEnd" => {
                self.phase = Phase::Exited;
                self.turn.clear();
                self.abandon("Claude exited before accepting the delivered input")
                    .await?;
            }
            "UserPromptSubmit" => self.submitted(&event).await?,
            "PermissionRequest" if self.phase == Phase::Working => {
                self.phase = Phase::AwaitingApproval;
            }
            "Stop" => self.idle(),
            // An API error ends the turn without Stop, failing its attempts.
            "StopFailure" => {
                let reason = turn_failed(failure(&event.input));
                for attempt in std::mem::take(&mut self.turn) {
                    self.tools.message_failed(&attempt, &reason).await?;
                }
                self.idle();
            }
            // Claude notifies a minute after it goes idle. After an interrupt,
            // which Stop does not report, this is the only sign.
            "Notification" if event.input["notification_type"] == "idle_prompt" => self.idle(),
            _ => {}
        }
        Ok(())
    }

    /// Claude took a prompt, from delivery or from a person. It fires on
    /// Enter, even while Claude is busy and queues the prompt.
    async fn submitted(&mut self, event: &HookEvent) -> Result<()> {
        self.phase = Phase::Working;
        // Submitting restores a stashed draft into the input box. Only typing
        // or delivery's stash key can have stashed one since the last prompt.
        let typed = self.console.typing().count;
        self.restored = self.stashed || typed != self.typed;
        self.typed = typed;
        self.stashed = false;
        if !self.marked {
            self.started.set()?;
            self.marked = true;
        }
        let accepted = self.pending.take_if(|pending| {
            event
                .prompt()
                .is_some_and(|prompt| prompt.contains(pending.attempt.as_str()))
        });
        if let Some(Pending { attempt, reply, .. }) = accepted {
            reply.sent().await?;
            self.turn.push(attempt);
        }
        Ok(())
    }

    fn idle(&mut self) {
        if matches!(self.phase, Phase::Working | Phase::AwaitingApproval) {
            self.phase = Phase::Idle;
        }
        self.turn.clear();
    }

    async fn poll(&mut self) -> Result<()> {
        if let Some(pending) = self
            .pending
            .take_if(|pending| pending.deadline <= Instant::now())
        {
            // Claude's state is unknown, and the input may remain in its box:
            // wait until Claude reports idle, then stash whatever is there.
            self.phase = Phase::Working;
            self.restored = true;
            self.fail(pending, "Claude did not accept the delivered input")
                .await?;
        }
        let typing = self.console.typing();
        let stash = typing.count != self.typed || self.restored;
        // Claude keeps one stash: the stash key would replace a stashed
        // draft, or restore it into an empty box.
        self.attended = typing
            .last
            .is_some_and(|last| last.elapsed() < self.timing.quiet)
            || (stash && self.console.screen().contains(STASHED));
        if self.phase != Phase::Idle || self.pending.is_some() || self.attended {
            return Ok(());
        }
        if let Some(reply) = self.tools.next_message().await? {
            self.type_in(reply, stash).await?;
        }
        Ok(())
    }

    /// Types `reply` into Claude's prompt and submits it, stashing any draft
    /// in the input box first. Claude restores that draft after submitting.
    async fn type_in(&mut self, reply: Reply, stash: bool) -> Result<()> {
        let attempt = attempt_id(&reply)?;
        if stash {
            self.console.send(STASH)?;
            self.stashed = true;
            tokio::time::sleep(self.timing.keystroke).await;
        }
        self.console
            .send(&[REQUEST.as_bytes(), PASTE_START, reply.bytes(), PASTE_END].concat())?;
        tokio::time::sleep(self.timing.paste).await;
        self.console.send(b"\r")?;
        self.pending = Some(Pending {
            attempt,
            reply,
            deadline: Instant::now() + self.timing.accept,
        });
        Ok(())
    }

    async fn abandon(&mut self, reason: &str) -> Result<()> {
        match self.pending.take() {
            Some(pending) => self.fail(pending, reason).await,
            None => Ok(()),
        }
    }

    async fn fail(&self, pending: Pending, reason: &str) -> Result<()> {
        let Pending { attempt, reply, .. } = pending;
        // Ending the attempt would first wait for this unsent reply.
        drop(reply);
        self.tools.message_failed(&attempt, reason).await
    }

    fn status(&self) -> Status {
        if self.pending.is_some() {
            return Status::Delivering;
        }
        match self.phase {
            Phase::Starting if self.launched.elapsed() >= self.timing.trust => {
                Status::AwaitingTrust
            }
            Phase::Starting => Status::Starting,
            Phase::Idle if self.attended => Status::Attended,
            Phase::Idle => Status::Idle,
            Phase::Working => Status::Working,
            Phase::AwaitingApproval => Status::AwaitingApproval,
            Phase::Exited => Status::Exited,
        }
    }
}

/// What a StopFailure hook says went wrong, most readable first.
fn failure(input: &Value) -> &str {
    ["last_assistant_message", "error_details", "error"]
        .into_iter()
        .find_map(|key| input[key].as_str().filter(|text| !text.is_empty()))
        .unwrap_or("unknown error")
}
