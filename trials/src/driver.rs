//! One session's part in the sessions trial: a seeded script of what a person
//! does with a session's terminal, and the driver that plays it against the
//! server, checking as it goes what docs/SESSIONS.md says each action does.
//! The driver keeps a model of the session (its state, whether its terminal
//! runs, whether a client controls it, and whether Pi or the shell has the
//! screen) and compares it with `session.list` and `terminal.status` after
//! every step.
//!
//! A step whose request fails, or whose attachment ends, while chaos crashes
//! the server was interrupted, not refused: the driver waits for the new
//! server, learns what became of any lifecycle change whose reply was lost,
//! and attaches again, which resumes the session. Anything else the
//! documentation rules out is a problem. After a problem that leaves the
//! model in doubt, the driver stops playing its session.

use crate::attach::{Attachment, End, Opened, View};
use crate::game::Fate;
use crate::history::History;
use crate::server::{Reply, send};
use crate::sessions::{Role, Stage, When, rc_files};
use ontography_app::client::Client;
use ontography_app::environment::Environment;
use ontography_app::terminal::{AttachRequest, HistoryAction, VERSION};
use rand::seq::{IndexedRandom, SliceRandom};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

/// How long a new Pi may take to show itself: the server first checks its
/// version, which macOS can hold up for a new build.
const PI_START: Duration = Duration::from_secs(20);
/// How long anything else may take to be seen.
const SEEN: Duration = Duration::from_secs(10);
/// How long a stopped terminal's processes may take to exit.
const GRACE: Duration = Duration::from_secs(2);
/// Pi's own commands, as the fake Pi knows them; everything else it echoes.
const COMMANDS: [&str; 5] = ["/quit", "/size", "/child", "/child-hup", "/graph"];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    Active,
    Suspended,
    Closed,
}

impl Status {
    pub fn name(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Suspended => "suspended",
            Self::Closed => "closed",
        }
    }
}

/// What has the session's screen: Pi, or the shell Pi returns to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Program {
    Pi,
    Shell,
}

impl Program {
    fn name(self) -> &'static str {
        match self {
            Self::Pi => "pi",
            Self::Shell => "shell",
        }
    }
}

/// How a client lets go of a terminal.
#[derive(Clone, Copy, Debug)]
pub enum Leave {
    /// It sends `detach`, as Ctrl-B D does.
    Detach,
    /// It closes its connection, as a closed terminal window does.
    Close,
    /// Another terminal detaches it (`ontography detach`).
    Takeover,
}

#[derive(Clone, Copy, Debug)]
pub enum Step {
    /// Attach as `ontography attach` does; it resumes an inactive session.
    Attach,
    /// Type lines at Pi, each waiting for its echo.
    Type(usize),
    /// Type lines at Pi in one input frame.
    Burst(usize),
    /// Resize the terminal, now and then from the history view.
    Resize {
        from_history: bool,
    },
    /// Have Pi start a program in its own process group.
    Child {
        ignores_hangup: bool,
    },
    Leave(Leave),
    /// A second client tries to attach while one controls the terminal.
    Contend,
    /// Pi's `/graph`: the client is asked to show the graph.
    Graph,
    /// Browse history while Pi keeps running.
    History,
    /// Pi's `/quit`, back to the shell.
    Quit,
    /// `pi` typed at the shell.
    Relaunch,
    /// `exit` typed at the shell, with the client attached or not.
    Exit {
        detached: bool,
    },
    Suspend,
    Resume,
    Close,
    /// The first scoped `flow.start`, which binds the session's graph.
    Start,
    /// A move at the graph's external source.
    Submit,
}

/// How a session's script ends: what is left of it for the orderly stop.
#[derive(Clone, Copy, Debug)]
pub enum Ending {
    Close,
    Exit,
    Suspend,
    /// Pi and its children still run.
    Live,
}

/// A lifecycle change whose reply may be lost.
#[derive(Clone, Copy, Debug)]
enum Lifecycle {
    Resume,
    Suspend,
    Exit,
    Close,
}

/// A line typed at Pi, and whether the client saw its echo.
#[derive(Clone, Debug)]
pub struct Typed {
    pub token: String,
    pub seen: bool,
}

/// A terminal as `terminal.ensure` described it.
struct Terminal {
    id: String,
    pid: Option<u64>,
    socket: PathBuf,
}

pub enum Failure {
    /// A crash interrupted the step.
    Crash,
    Problem(String),
}

type Played = std::result::Result<(), Failure>;

/// A seeded script. Every round shuffles blocks of steps, each of which
/// leaves Pi running with a client attached, and may end by suspending the
/// session one way or another and resuming it.
pub fn script(rng: &mut impl Rng, graph: bool, rounds: usize, ending: Ending) -> Vec<Step> {
    let mut steps = vec![Step::Attach];
    if graph {
        steps.push(Step::Start);
    }
    steps.push(Step::Type(rng.random_range(1..=2)));
    let leave = |rng: &mut dyn rand::RngCore| {
        Step::Leave(
            *[Leave::Detach, Leave::Close, Leave::Takeover]
                .choose(rng)
                .expect("a way"),
        )
    };
    for _ in 0..rounds {
        let mut blocks: Vec<Vec<Step>> = vec![
            vec![Step::Type(rng.random_range(1..=3))],
            vec![Step::Burst(rng.random_range(2..=4))],
            vec![Step::Resize {
                from_history: rng.random_bool(0.3),
            }],
            vec![Step::Child {
                ignores_hangup: rng.random_bool(0.5),
            }],
            vec![leave(rng), Step::Attach],
            vec![Step::Contend],
            vec![Step::Graph],
            vec![Step::History],
            if rng.random_bool(0.5) {
                vec![Step::Quit, Step::Relaunch]
            } else {
                // Detach and reattach at the shell, then contend there.
                vec![
                    Step::Quit,
                    leave(rng),
                    Step::Attach,
                    Step::Contend,
                    Step::Relaunch,
                ]
            },
        ];
        if graph {
            blocks.push(vec![Step::Submit]);
            blocks.push(vec![Step::Submit]);
        }
        blocks.shuffle(rng);
        let keep = rng.random_range(blocks.len() / 2..=blocks.len());
        steps.extend(blocks.into_iter().take(keep).flatten());
        match rng.random_range(0..3) {
            0 => steps.extend([
                Step::Exit {
                    detached: rng.random_bool(0.4),
                },
                Step::Resume,
            ]),
            1 => steps.extend([Step::Suspend, Step::Resume]),
            _ => {}
        }
    }
    // Whatever is left runs a child of each kind.
    steps.extend([
        Step::Child {
            ignores_hangup: true,
        },
        Step::Child {
            ignores_hangup: false,
        },
    ]);
    if graph {
        steps.push(Step::Submit);
    }
    match ending {
        Ending::Close => steps.push(Step::Close),
        Ending::Exit => steps.push(Step::Exit {
            detached: rng.random_bool(0.4),
        }),
        Ending::Suspend => steps.push(Step::Suspend),
        Ending::Live => {
            if rng.random_bool(0.5) {
                steps.push(Step::Leave(Leave::Detach));
            }
        }
    }
    steps
}

pub struct Driver {
    pub name: String,
    pub id: String,
    /// The conversation the session was created with; it never changes here.
    pub conversation: String,
    stage: Arc<Stage>,
    rng: ChaCha8Rng,
    pub status: Status,
    running: bool,
    attached: bool,
    program: Program,
    terminal: Option<Terminal>,
    attachment: Option<Attachment>,
    size: (u16, u16),
    /// The activation the session took its environment from.
    marker: Option<String>,
    activations: usize,
    /// The Pi running now, and the lines typed at it, in order.
    pi: Option<u32>,
    last_pi: Option<u32>,
    here: Vec<String>,
    pub typed: Vec<Typed>,
    /// Children Pi reported starting.
    children: BTreeSet<u32>,
    /// Generations of the session's terminals, named by their rc files.
    pub generations: BTreeSet<String>,
    generation: Option<String>,
    /// The saved history the session records for its conversation.
    history: Option<PathBuf>,
    pub run: Option<String>,
    start_id: String,
    pub moves: Vec<(String, Fate)>,
    pending: Option<Lifecycle>,
    pub problems: Vec<String>,
    pub counts: BTreeMap<&'static str, usize>,
    /// The model is in doubt: the driver stopped playing.
    pub abandoned: bool,
    /// The server generation the model describes.
    seen: u64,
}

impl Driver {
    pub fn new(stage: Arc<Stage>, name: String, record: &Value, seed: u64) -> Self {
        Self {
            id: record["session_id"].as_str().unwrap_or_default().to_owned(),
            conversation: record["pi"]["active_conversation_id"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
            name,
            stage,
            rng: ChaCha8Rng::seed_from_u64(seed),
            status: Status::Active,
            running: false,
            attached: false,
            program: Program::Pi,
            terminal: None,
            attachment: None,
            size: (24, 80),
            marker: None,
            activations: 0,
            pi: None,
            last_pi: None,
            here: Vec::new(),
            typed: Vec::new(),
            children: BTreeSet::new(),
            generations: BTreeSet::new(),
            generation: None,
            history: None,
            run: None,
            start_id: uuid::Uuid::new_v4().to_string(),
            moves: Vec::new(),
            pending: None,
            problems: Vec::new(),
            counts: BTreeMap::new(),
            abandoned: false,
            seen: 0,
        }
    }

    fn count(&mut self, name: &'static str) {
        *self.counts.entry(name).or_default() += 1;
    }

    fn note(&self, text: &str) {
        self.stage.note(&self.name, text);
    }

    pub async fn play(mut self, script: Vec<Step>) -> Self {
        for step in script {
            if self.abandoned {
                break;
            }
            // A crash may have struck between steps, or while the driver
            // recovered from another.
            if self.stage.generation() != self.seen {
                self.note("  the server restarted");
                self.recover(self.seen).await;
                if self.abandoned {
                    break;
                }
            }
            let mut since = self.seen;
            loop {
                self.note(&format!("{step:?}"));
                let played = match self.step(step, since).await {
                    Ok(()) => self.verify(since).await,
                    failed => failed,
                };
                match played {
                    Ok(()) => break,
                    Err(Failure::Problem(problem)) => {
                        self.note(&format!("  problem: {problem}"));
                        self.problems
                            .push(format!("{} at {step:?}: {problem}", self.name));
                        self.abandoned = true;
                        break;
                    }
                    Err(Failure::Crash) => {
                        self.note("  interrupted by a crash");
                        self.recover(since).await;
                        // A graph start is retried, as its start_id allows.
                        if !matches!(step, Step::Start) || self.abandoned {
                            break;
                        }
                        since = self.seen;
                    }
                }
            }
            self.count("steps");
            self.stage.advance();
            let pause = self.rng.random_range(0..40);
            tokio::time::sleep(Duration::from_millis(pause)).await;
        }
        self
    }

    async fn step(&mut self, step: Step, g: u64) -> Played {
        match step {
            Step::Attach => {
                if self.status != Status::Closed && !self.attached {
                    self.open(g).await?;
                }
                Ok(())
            }
            Step::Type(count) => self.type_lines(g, count, false).await,
            Step::Burst(count) => self.type_lines(g, count, true).await,
            Step::Resize { from_history } => self.resize(g, from_history).await,
            Step::Child { ignores_hangup } => self.child(g, ignores_hangup).await,
            Step::Leave(how) => self.leave(g, how).await,
            Step::Contend => self.contend(g).await,
            Step::Graph => self.graph_view(g).await,
            Step::History => self.history_view(g).await,
            Step::Quit => {
                if self.ready(g, Some(Program::Pi)).await? {
                    self.stop_pi(g).await?;
                }
                Ok(())
            }
            Step::Relaunch => {
                if self.ready(g, None).await? && self.program == Program::Shell {
                    self.start_pi(g).await?;
                }
                Ok(())
            }
            Step::Exit { detached } => self.exit(g, detached).await,
            Step::Suspend => self.suspend(g).await,
            Step::Resume => {
                if self.status != Status::Closed && !self.attached {
                    self.open(g).await?;
                }
                Ok(())
            }
            Step::Close => self.close(g).await,
            Step::Start => self.start(g).await,
            Step::Submit => self.submit(g).await,
        }
    }

    /// What a failed request or wait means: a crash, if one struck since the
    /// step began, or else a problem.
    fn fail(&self, g: u64, text: impl Into<String>) -> Failure {
        if self.stage.interrupted(g) {
            Failure::Crash
        } else {
            Failure::Problem(text.into())
        }
    }

    fn done(&self, g: u64, operation: &str, reply: Reply) -> Result<Value, Failure> {
        match reply {
            Reply::Done(value) => Ok(value),
            other => Err(self.fail(g, format!("{operation}: {other}"))),
        }
    }

    /// A request as the CLI makes it: scoped to the session or naming it,
    /// and, when it activates the session, carrying the environment of the
    /// terminal that typed it, marked with the activation.
    async fn call(
        &self,
        operation: &str,
        args: Value,
        scoped: bool,
        marker: Option<&str>,
    ) -> Reply {
        let mut client = self.stage.client().await;
        if scoped {
            client = client.for_session(&self.id);
        }
        if let Some(marker) = marker {
            client =
                client.with_environment(Environment::current().with("TRIALS_ACTIVATION", marker));
        }
        send(&client, operation, args).await
    }

    async fn inspect(&self, g: u64) -> Result<Value, Failure> {
        let reply = self
            .call(
                "session.inspect",
                json!({"session_id": self.id}),
                false,
                None,
            )
            .await;
        self.done(g, "session.inspect", reply)
    }

    async fn terminal_status(&self, g: u64) -> Result<Value, Failure> {
        let reply = self.call("terminal.status", json!({}), true, None).await;
        self.done(g, "terminal.status", reply)
    }

    /// Polls `terminal.status` until `done` holds of it.
    async fn until(
        &self,
        g: u64,
        what: &str,
        done: impl Fn(&Value) -> bool,
    ) -> Result<Value, Failure> {
        let deadline = tokio::time::Instant::now() + SEEN;
        loop {
            let status = self.terminal_status(g).await?;
            if done(&status) {
                return Ok(status);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(self.fail(
                    g,
                    format!(
                        "{what}: not within {}s; the terminal's status is {status}",
                        SEEN.as_secs()
                    ),
                ));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Polls `session.inspect` until the session is in `status`.
    async fn until_status(&self, g: u64, status: &str) -> Result<Value, Failure> {
        let deadline = tokio::time::Instant::now() + SEEN;
        loop {
            let record = self.inspect(g).await?;
            if record["status"] == status {
                return Ok(record);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(self.fail(
                    g,
                    format!(
                        "the session is still {} {}s later, not {status}",
                        record["status"],
                        SEEN.as_secs()
                    ),
                ));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Waits until the client has seen what `done` asks for.
    async fn screen(
        &mut self,
        g: u64,
        what: &str,
        limit: Duration,
        done: impl Fn(&View) -> bool,
    ) -> Result<View, Failure> {
        let waited = match self.attachment.as_mut() {
            Some(attachment) => attachment.wait(limit, done).await,
            None => return Err(Failure::Problem(format!("no client to see {what}"))),
        };
        waited.map_err(|view| {
            let how = match &view.end {
                Some(end) => format!("the attachment ended ({end:?})"),
                None => format!("not within {}s", limit.as_secs()),
            };
            self.fail(g, format!("{what}: {how}; the screen ends {}", tail(&view)))
        })
    }

    fn view(&self) -> View {
        self.attachment
            .as_ref()
            .map(Attachment::view)
            .unwrap_or_default()
    }

    async fn input(&mut self, g: u64, bytes: &[u8]) -> Played {
        let sent = match self.attachment.as_mut() {
            Some(attachment) => attachment.input(bytes).await,
            None => return Err(Failure::Problem("typing with no client attached".into())),
        };
        sent.map_err(|error| self.fail(g, format!("typing failed: {error:#}")))
    }

    async fn type_text(&mut self, g: u64, text: &str) -> Played {
        self.input(g, format!("{text}\r").as_bytes()).await
    }

    fn token(&mut self) -> String {
        let token = format!("{}.{}", self.name, self.typed.len() + 1);
        self.typed.push(Typed {
            token: token.clone(),
            seen: false,
        });
        self.here.push(token.clone());
        token
    }

    fn rc_files(&self) -> BTreeSet<String> {
        rc_files(&self.stage.data, &self.id).into_keys().collect()
    }

    /// Brings the session to a client attached with `program` on screen.
    /// False when the session is not active: the step is skipped.
    async fn ready(&mut self, g: u64, program: Option<Program>) -> Result<bool, Failure> {
        if self.status != Status::Active {
            return Ok(false);
        }
        if !self.attached {
            self.open(g).await?;
        }
        match (program, self.program) {
            (Some(Program::Pi), Program::Shell) => self.start_pi(g).await?,
            (Some(Program::Shell), Program::Pi) => self.stop_pi(g).await?,
            _ => {}
        }
        Ok(true)
    }

    /// Attaches as `ontography attach` does: resumes the session with this
    /// client's environment, ensures its terminal, and connects as its
    /// controller. A new terminal starts Pi; an existing one is reused as it
    /// is, showing its current screen.
    async fn open(&mut self, g: u64) -> Played {
        self.activations += 1;
        let marker = format!("{}#{}", self.name, self.activations);
        self.pending = Some(Lifecycle::Resume);
        let reply = self
            .call(
                "session.resume",
                json!({"session_id": self.id}),
                false,
                Some(&marker),
            )
            .await;
        let record = self.done(g, "session.resume", reply)?;
        self.pending = None;
        if record["status"] != "active" {
            return Err(Failure::Problem(format!(
                "session.resume left the session {}",
                record["status"]
            )));
        }
        let resumed = self.status != Status::Active;
        self.status = Status::Active;
        // The first activation since the session last stopped gives it its
        // environment; later ones do not change it.
        self.marker.get_or_insert(marker.clone());
        if resumed && self.run.is_some() {
            self.check_run(g, "active").await?;
        }
        let before = self.rc_files();
        let reply = self
            .call(
                "terminal.ensure",
                json!({"pi": self.stage.pi, "rows": self.size.0, "cols": self.size.1}),
                true,
                Some(&marker),
            )
            .await;
        let status = self.done(g, "terminal.ensure", reply)?;
        let id = status["terminal_id"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        let pid = status["pid"].as_u64();
        let fresh = match &self.terminal {
            Some(terminal) if terminal.id == id => {
                if terminal.pid != pid {
                    return Err(Failure::Problem(format!(
                        "reattaching found another shell in the same terminal: {:?} became {pid:?}",
                        terminal.pid
                    )));
                }
                if self.program == Program::Pi
                    && status["manager_pid"].as_u64() != self.pi.map(u64::from)
                {
                    return Err(Failure::Problem(format!(
                        "reattaching restarted Pi: {:?} became {}",
                        self.pi, status["manager_pid"]
                    )));
                }
                false
            }
            Some(terminal) if self.running => {
                return Err(Failure::Problem(format!(
                    "terminal.ensure replaced the running terminal {} with {id}",
                    terminal.id
                )));
            }
            _ => true,
        };
        self.terminal = Some(Terminal {
            id,
            pid,
            socket: PathBuf::from(status["socket"].as_str().unwrap_or_default()),
        });
        if fresh {
            self.note(&format!("  new terminal, shell {pid:?}"));
            self.count("terminals");
            self.running = true;
            self.program = Program::Pi;
            self.pi = None;
            self.here.clear();
            let after = self.rc_files();
            self.generation = after.difference(&before).next().cloned();
            self.generations.extend(after);
        }
        // Now and then the client reattaches from a terminal of another
        // size: the controller's dimensions are the terminal's.
        let resized = !fresh && self.rng.random_bool(0.3);
        if resized {
            self.size = self.other_size();
            self.note(&format!("  reattaching at {:?}", self.size));
        }
        self.connect(g).await?;
        if fresh {
            return self.await_pi(g).await;
        }
        // Reattaching shows the terminal as it is.
        let size = self.size;
        let report = format!("{} {}", size.0, size.1);
        let view = if resized && self.program == Program::Pi {
            self.screen(g, &format!("Pi's report of the size {report}"), SEEN, |v| {
                v.size == size && v.lines("SIZE ").last() == Some(&report)
            })
            .await?
        } else {
            self.screen(g, "the first snapshot", SEEN, |v| v.snapshots > 0)
                .await?
        };
        if resized {
            self.until(g, "the terminal to take the client's size", |s| {
                s["rows"] == size.0 && s["cols"] == size.1
            })
            .await?;
        }
        if self.program == Program::Pi {
            self.check_echoes(&view)?;
            if let Some(last) = self.here.last()
                && !view.shows(&format!("ECHO {last}"))
            {
                return Err(Failure::Problem(format!(
                    "the screen on reattaching lacks the latest echo, of {last}; it ends {}",
                    tail(&view)
                )));
            }
        }
        Ok(())
    }

    async fn connect(&mut self, g: u64) -> Played {
        let Some(terminal) = &self.terminal else {
            return Err(Failure::Problem("no terminal to attach to".into()));
        };
        let request = AttachRequest {
            version: VERSION,
            server_id: self.stage.client().await.server_id().to_owned(),
            session_id: self.id.clone(),
            terminal_id: terminal.id.clone(),
            rows: self.size.0,
            cols: self.size.1,
        };
        match Attachment::open(&terminal.socket, request).await {
            Ok(Opened::Attached(attachment)) => {
                self.attachment = Some(attachment);
                self.attached = true;
                self.count("attaches");
                Ok(())
            }
            Ok(Opened::Refused(error)) => {
                Err(self.fail(g, format!("the terminal refused its only client: {error}")))
            }
            Err(error) => Err(self.fail(g, format!("attaching failed: {error:#}"))),
        }
    }

    /// Waits for a new Pi on screen and in the server's status, and checks
    /// how it was launched.
    async fn await_pi(&mut self, g: u64) -> Played {
        let before = self.last_pi;
        let view = match self
            .screen(g, "Pi's READY", PI_START, |v| {
                ready_pid(v).is_some_and(|pid| Some(pid) != before)
            })
            .await
        {
            Ok(view) => view,
            Err(Failure::Problem(problem)) => {
                // What the server says of its manager tells why.
                let status = self.terminal_status(g).await.map_or(String::new(), |s| {
                    format!(
                        "; the server has {} with error {}",
                        s["manager_mode"], s["manager_error"]
                    )
                });
                return Err(Failure::Problem(format!("{problem}{status}")));
            }
            Err(crash) => return Err(crash),
        };
        let pid = ready_pid(&view).expect("seen");
        self.until(g, "the server to see Pi running", |s| {
            s["manager_mode"] == "pi" && s["manager_pid"].as_u64() == Some(u64::from(pid))
        })
        .await?;
        self.note(&format!("  Pi {pid}"));
        self.pi = Some(pid);
        self.last_pi = Some(pid);
        self.program = Program::Pi;
        self.here.clear();
        self.count("Pi launches");
        let problems = self.check_launch(pid);
        self.problems.extend(problems);
        // The saved history, once Pi registered one, is what the next Pi
        // must resume.
        let record = self.inspect(g).await?;
        let conversation = &record["pi"]["conversations"][&self.conversation];
        if conversation["materialized"] == true
            && let Some(path) = conversation["path"].as_str()
        {
            self.history = Some(PathBuf::from(path));
        }
        Ok(())
    }

    /// How a Pi was launched, from its witness record: the session's
    /// conversation, resumed from its saved history once there is one, and
    /// the environment of the activation the session took it from.
    fn check_launch(&self, pid: u32) -> Vec<String> {
        let events = self.stage.witness(&self.id);
        let Some(start) = events
            .iter()
            .find(|e| e["event"] == "start" && e["pid"].as_u64() == Some(u64::from(pid)))
        else {
            return vec![format!(
                "{}: Pi {pid} showed itself without a start record",
                self.name
            )];
        };
        let mut problems = Vec::new();
        let mut problem = |text: String| problems.push(format!("{}: Pi {pid} {text}", self.name));
        if start["conversation"] != self.conversation.as_str() {
            problem(format!(
                "started conversation {}, not the session's {}",
                start["conversation"], self.conversation
            ));
        }
        let environment = &start["environment"];
        if environment["ONTOGRAPHY_SESSION_ID"] != self.id.as_str() {
            problem(format!(
                "has ONTOGRAPHY_SESSION_ID {}",
                environment["ONTOGRAPHY_SESSION_ID"]
            ));
        }
        let data = environment["ONTOGRAPHY_DATA_DIR"].as_str().map(Path::new);
        if !data.is_some_and(|data| same_path(data, &self.stage.data)) {
            problem(format!(
                "has ONTOGRAPHY_DATA_DIR {}, not the server's store",
                environment["ONTOGRAPHY_DATA_DIR"]
            ));
        }
        if let Some(marker) = &self.marker
            && environment["TRIALS_ACTIVATION"] != marker.as_str()
        {
            problem(format!(
                "started with the environment of activation {}, but the session took its environment from activation {marker}",
                environment["TRIALS_ACTIVATION"]
            ));
        }
        if let Some(history) = &self.history {
            let resumed = start["resumed"].as_str().map(Path::new);
            if !resumed.is_some_and(|path| same_path(path, history)) {
                problem(format!(
                    "did not resume the saved conversation {}: it was given --session {} --session-id {}",
                    history.display(),
                    start["resumed"],
                    start["reserved"]
                ));
            }
        }
        if !start["registration_error"].is_null() {
            problem(format!(
                "could not register its conversation: {}",
                start["registration_error"]
            ));
        }
        problems
    }

    async fn type_lines(&mut self, g: u64, count: usize, burst: bool) -> Played {
        if !self.ready(g, Some(Program::Pi)).await? {
            return Ok(());
        }
        if burst {
            let tokens: Vec<String> = (0..count).map(|_| self.token()).collect();
            let text: String = tokens.iter().map(|token| format!("{token}\r")).collect();
            self.input(g, text.as_bytes()).await?;
            return self.echoed(g, &tokens).await;
        }
        for _ in 0..count {
            let token = self.token();
            let text = format!("{token}\r");
            // Now and then a line arrives in two input frames.
            if self.rng.random_bool(0.3) {
                let (first, second) = text.split_at(text.len() / 2);
                self.input(g, first.as_bytes()).await?;
                self.input(g, second.as_bytes()).await?;
            } else {
                self.input(g, text.as_bytes()).await?;
            }
            self.echoed(g, std::slice::from_ref(&token)).await?;
        }
        Ok(())
    }

    /// Waits for the echo of the last of `tokens` and checks the screen.
    async fn echoed(&mut self, g: u64, tokens: &[String]) -> Played {
        let Some(last) = tokens.last() else {
            return Ok(());
        };
        let echo = format!("ECHO {last}");
        let view = self
            .screen(g, &format!("the echo of {last}"), SEEN, |v| {
                v.shows(&echo) && !v.history
            })
            .await?;
        for typed in &mut self.typed {
            if tokens.contains(&typed.token) {
                typed.seen = true;
            }
        }
        self.check_echoes(&view)
    }

    /// Every line typed at this Pi shows on screen once, in the order typed;
    /// the oldest may have scrolled away.
    fn check_echoes(&self, view: &View) -> Played {
        let seen = view.lines("ECHO ");
        let from = self.here.len().saturating_sub(seen.len());
        if seen.as_slice() != &self.here[from..] {
            return Err(Failure::Problem(format!(
                "the screen shows the echoes {seen:?}, but the lines typed at this Pi were {:?}",
                self.here
            )));
        }
        Ok(())
    }

    async fn resize(&mut self, g: u64, from_history: bool) -> Played {
        if !self.ready(g, Some(Program::Pi)).await? {
            return Ok(());
        }
        let size = self.other_size();
        if from_history {
            self.enter_history(g).await?;
        }
        let resized = match self.attachment.as_mut() {
            Some(attachment) => attachment.resize(size.0, size.1).await,
            None => return Err(Failure::Problem("resizing with no client attached".into())),
        };
        resized.map_err(|error| self.fail(g, format!("resizing failed: {error:#}")))?;
        // Resizing returns to the live screen, where Pi reports its new size.
        let report = format!("{} {}", size.0, size.1);
        self.screen(
            g,
            &format!("Pi's report of its new size {report}"),
            SEEN,
            |v| v.size == size && !v.history && v.lines("SIZE ").last() == Some(&report),
        )
        .await?;
        self.size = size;
        self.until(g, "the terminal's new size", |s| {
            s["rows"] == size.0 && s["cols"] == size.1
        })
        .await?;
        let view = self.view();
        self.check_echoes(&view)?;
        // Asked, Pi reports the same size.
        let Some(pid) = self.pi else { return Ok(()) };
        let asked = self.sizes(pid).len();
        self.type_text(g, "/size").await?;
        let deadline = tokio::time::Instant::now() + SEEN;
        loop {
            if let Some(reported) = self.sizes(pid).get(asked) {
                if *reported != size {
                    return Err(Failure::Problem(format!(
                        "after a resize to {size:?}, Pi's /size reports {reported:?}"
                    )));
                }
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(self.fail(g, "Pi never answered /size"));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// A terminal size other than the current one.
    fn other_size(&mut self) -> (u16, u16) {
        loop {
            let size = (
                self.rng.random_range(18..=44),
                self.rng.random_range(60..=140),
            );
            if size != self.size {
                return size;
            }
        }
    }

    /// The sizes a Pi reported, in order, from its witness.
    fn sizes(&self, pid: u32) -> Vec<(u16, u16)> {
        self.stage
            .witness(&self.id)
            .iter()
            .filter(|e| e["event"] == "size" && e["pid"].as_u64() == Some(u64::from(pid)))
            .map(|e| {
                let field = |name: &str| e[name].as_u64().unwrap_or_default() as u16;
                (field("rows"), field("cols"))
            })
            .collect()
    }

    async fn enter_history(&mut self, g: u64) -> Played {
        let entered = match self.attachment.as_mut() {
            Some(attachment) => attachment.history(HistoryAction::Enter).await,
            None => return Err(Failure::Problem("no client to browse history".into())),
        };
        entered.map_err(|error| self.fail(g, format!("entering history failed: {error:#}")))?;
        self.screen(g, "the history view", SEEN, |v| v.history)
            .await
            .map(|_| ())
    }

    async fn child(&mut self, g: u64, ignores_hangup: bool) -> Played {
        if !self.ready(g, Some(Program::Pi)).await? {
            return Ok(());
        }
        let known = self.children.clone();
        self.type_text(
            g,
            if ignores_hangup {
                "/child-hup"
            } else {
                "/child"
            },
        )
        .await?;
        let new_child = move |v: &View| {
            v.lines("CHILD ")
                .iter()
                .filter_map(|line| line.split_whitespace().next()?.parse::<u32>().ok())
                .find(|pid| !known.contains(pid))
        };
        let view = self
            .screen(g, "Pi's report of its child", SEEN, |v| {
                new_child(v).is_some()
            })
            .await?;
        if let Some(pid) = new_child(&view) {
            self.note(&format!("  child {pid}"));
            self.children.insert(pid);
        }
        self.count("children");
        self.stage.observe();
        Ok(())
    }

    async fn leave(&mut self, g: u64, how: Leave) -> Played {
        if self.status != Status::Active || !self.attached {
            return Ok(());
        }
        let before = self.terminal_status(g).await?;
        match how {
            Leave::Detach => {
                let sent = match self.attachment.as_mut() {
                    Some(attachment) => attachment.detach().await,
                    None => Ok(()),
                };
                sent.map_err(|error| self.fail(g, format!("detaching failed: {error:#}")))?;
                self.screen(g, "the attachment to end after detaching", SEEN, |v| {
                    v.end.is_some()
                })
                .await?;
            }
            Leave::Close => {}
            Leave::Takeover => {
                // Another terminal: its own client and connection.
                let other = Client::connect(self.stage.client().await.socket())
                    .await
                    .map_err(|error| self.fail(g, format!("a second client: {error}")))?;
                let reply = send(&other, "terminal.detach", json!({"session_id": self.id})).await;
                self.done(g, "terminal.detach", reply)?;
                let view = self
                    .screen(g, "the detach notice", SEEN, |v| v.end.is_some())
                    .await?;
                if view.end != Some(End::Detached) {
                    return Err(Failure::Problem(format!(
                        "detached from another terminal, the client ended with {:?}, not a detach notice",
                        view.end
                    )));
                }
            }
        }
        self.attachment = None;
        self.attached = false;
        let after = self
            .until(g, "the server to release the controller", |s| {
                s["attached"] == false
            })
            .await?;
        // Detaching keeps the shell and Pi running.
        for field in ["terminal_id", "pid", "manager_pid", "running"] {
            if after[field] != before[field] {
                return Err(Failure::Problem(format!(
                    "detaching ({how:?}) changed the terminal's {field} from {} to {}",
                    before[field], after[field]
                )));
            }
        }
        Ok(())
    }

    async fn contend(&mut self, g: u64) -> Played {
        if !self.ready(g, None).await? {
            return Ok(());
        }
        let Some(terminal) = &self.terminal else {
            return Ok(());
        };
        // The second client asks for another size, which it must not get.
        let request = AttachRequest {
            version: VERSION,
            server_id: self.stage.client().await.server_id().to_owned(),
            session_id: self.id.clone(),
            terminal_id: terminal.id.clone(),
            rows: self.size.0 + 1,
            cols: self.size.1 + 1,
        };
        match Attachment::open(&terminal.socket, request).await {
            Ok(Opened::Refused(error)) if error.code == "terminal_attached" => {
                self.count("refused");
            }
            Ok(Opened::Refused(error)) => {
                return Err(self.fail(
                    g,
                    format!("a second controller was refused with {error}, not terminal_attached"),
                ));
            }
            Ok(Opened::Attached(_)) => {
                return Err(Failure::Problem(
                    "a second controller attached while another controlled the terminal".into(),
                ));
            }
            Err(error) => return Err(self.fail(g, format!("a second attach failed: {error:#}"))),
        }
        // The first client still controls the terminal, at its size.
        let status = self.terminal_status(g).await?;
        if status["attached"] != true
            || status["rows"] != self.size.0
            || status["cols"] != self.size.1
        {
            return Err(Failure::Problem(format!(
                "after a second controller was refused, the terminal is {status}"
            )));
        }
        if self.program == Program::Pi {
            let token = self.token();
            self.type_text(g, &token).await?;
            self.echoed(g, &[token]).await?;
        }
        Ok(())
    }

    async fn graph_view(&mut self, g: u64) -> Played {
        if !self.ready(g, Some(Program::Pi)).await? {
            return Ok(());
        }
        let Some(pid) = self.pi else { return Ok(()) };
        let before = self.view().graphs;
        let asked = self.graph_outcomes(pid).len();
        self.type_text(g, "/graph").await?;
        self.screen(g, "the graph notice", SEEN, |v| v.graphs > before)
            .await?;
        let deadline = tokio::time::Instant::now() + SEEN;
        let outcome = loop {
            if let Some(outcome) = self.graph_outcomes(pid).get(asked) {
                break outcome.clone();
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(self.fail(g, "Pi's /graph never finished"));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        if outcome != "ok" {
            return Err(self.fail(g, format!("Pi's /graph failed: {outcome}")));
        }
        Ok(())
    }

    fn graph_outcomes(&self, pid: u32) -> Vec<String> {
        self.stage
            .witness(&self.id)
            .iter()
            .filter(|e| e["event"] == "graph" && e["pid"].as_u64() == Some(u64::from(pid)))
            .map(|e| e["outcome"].as_str().unwrap_or_default().to_owned())
            .collect()
    }

    /// History is a frozen view: Pi keeps running and receiving input while
    /// it is open, and its new output shows only on return to live.
    async fn history_view(&mut self, g: u64) -> Played {
        if !self.ready(g, Some(Program::Pi)).await? {
            return Ok(());
        }
        let Some(pid) = self.pi else { return Ok(()) };
        self.enter_history(g).await?;
        let token = self.token();
        self.type_text(g, &token).await?;
        let deadline = tokio::time::Instant::now() + SEEN;
        while !self.stage.witness(&self.id).iter().any(|e| {
            e["event"] == "input"
                && e["line"] == token.as_str()
                && e["pid"].as_u64() == Some(u64::from(pid))
        }) {
            if tokio::time::Instant::now() >= deadline {
                return Err(self.fail(
                    g,
                    format!("Pi never received {token} while the client browsed history"),
                ));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        let view = self.view();
        if !view.history {
            return Err(Failure::Problem(
                "the history view returned to live by itself".into(),
            ));
        }
        if view.shows(&format!("ECHO {token}")) {
            return Err(Failure::Problem(format!(
                "Pi's new output, the echo of {token}, showed in the frozen history view"
            )));
        }
        let left = match self.attachment.as_mut() {
            Some(attachment) => attachment.history(HistoryAction::Exit).await,
            None => return Err(Failure::Problem("no client to leave history".into())),
        };
        left.map_err(|error| self.fail(g, format!("leaving history failed: {error:#}")))?;
        self.echoed(g, &[token]).await
    }

    /// Pi's `/quit`: Pi exits, the same shell stays, and the session and its
    /// graph keep running.
    async fn stop_pi(&mut self, g: u64) -> Played {
        let (Some(pid), Some(terminal)) = (self.pi, &self.terminal) else {
            return Ok(());
        };
        let (id, shell) = (terminal.id.clone(), terminal.pid);
        self.type_text(g, "/quit").await?;
        let status = self
            .until(g, "the shell to return after /quit", |s| {
                s["manager_mode"] == "shell" && s["manager_pid"].is_null()
            })
            .await?;
        if status["terminal_id"] != id.as_str()
            || status["pid"].as_u64() != shell
            || status["running"] != true
        {
            return Err(Failure::Problem(format!(
                "after /quit the terminal is {status}, not the same shell"
            )));
        }
        self.program = Program::Shell;
        self.pi = None;
        self.here.clear();
        let left = self
            .stage
            .wait_gone(|p, l| l.role == Role::Pi && p.pid == pid as i32, GRACE)
            .await;
        if !left.is_empty() {
            self.problems
                .push(format!("{}: Pi {pid} still ran after /quit", self.name));
        }
        Ok(())
    }

    /// `pi` at the shell starts Pi again, from the same shell, resuming the
    /// session's conversation.
    async fn start_pi(&mut self, g: u64) -> Played {
        let Some(terminal) = &self.terminal else {
            return Ok(());
        };
        let (id, shell) = (terminal.id.clone(), terminal.pid);
        self.type_text(g, "pi").await?;
        self.await_pi(g).await?;
        let status = self.terminal_status(g).await?;
        if status["terminal_id"] != id.as_str() || status["pid"].as_u64() != shell {
            return Err(Failure::Problem(format!(
                "`pi` at the shell left the terminal {status}, not the same shell"
            )));
        }
        Ok(())
    }

    /// `exit` at the shell suspends the session, attached or not.
    async fn exit(&mut self, g: u64, detached: bool) -> Played {
        if !self.ready(g, Some(Program::Shell)).await? {
            return Ok(());
        }
        self.pending = Some(Lifecycle::Exit);
        if detached {
            // The shell exits a moment after its client has gone.
            self.type_text(g, "sleep 0.3; exit").await?;
            let sent = match self.attachment.as_mut() {
                Some(attachment) => attachment.detach().await,
                None => Ok(()),
            };
            sent.map_err(|error| self.fail(g, format!("detaching failed: {error:#}")))?;
        } else {
            self.type_text(g, "exit").await?;
            // The client returns to the outer shell.
            self.screen(g, "the terminal to end after exit", SEEN, |v| {
                v.end.is_some()
            })
            .await?;
        }
        self.attachment = None;
        self.attached = false;
        self.until_status(g, "suspended").await?;
        self.pending = None;
        self.stopped(g, Status::Suspended, "exiting its shell")
            .await
    }

    async fn suspend(&mut self, g: u64) -> Played {
        if self.status != Status::Active {
            return Ok(());
        }
        self.pending = Some(Lifecycle::Suspend);
        let reply = self
            .call(
                "session.suspend",
                json!({"session_id": self.id}),
                false,
                None,
            )
            .await;
        let record = self.done(g, "session.suspend", reply)?;
        self.pending = None;
        if record["status"] != "suspended" {
            return Err(Failure::Problem(format!(
                "session.suspend left the session {}",
                record["status"]
            )));
        }
        self.release(g, "the suspension").await?;
        self.stopped(g, Status::Suspended, "its suspension").await
    }

    async fn close(&mut self, g: u64) -> Played {
        if self.status == Status::Closed {
            return Ok(());
        }
        self.pending = Some(Lifecycle::Close);
        let reply = self
            .call("session.close", json!({"session_id": self.id}), false, None)
            .await;
        let record = self.done(g, "session.close", reply)?;
        self.pending = None;
        if record["status"] != "closed" {
            return Err(Failure::Problem(format!(
                "session.close left the session {}",
                record["status"]
            )));
        }
        // Closing stops the shell and Pi, including the attached client.
        self.release(g, "closing").await?;
        self.stopped(g, Status::Closed, "closing its session")
            .await?;
        // A closed session cannot resume, nor start a terminal.
        let reply = self
            .call(
                "session.resume",
                json!({"session_id": self.id}),
                false,
                Some(&format!("{}#closed", self.name)),
            )
            .await;
        match reply {
            Reply::Failed(..) | Reply::Rejected(..) => {}
            Reply::Done(record) => self.problems.push(format!(
                "{}: a closed session resumed: {}",
                self.name, record["status"]
            )),
            other => return Err(self.fail(g, format!("resuming a closed session: {other}"))),
        }
        let reply = self
            .call(
                "terminal.ensure",
                json!({"pi": self.stage.pi, "rows": self.size.0, "cols": self.size.1}),
                true,
                None,
            )
            .await;
        match reply {
            Reply::Failed(..) | Reply::Rejected(..) => Ok(()),
            Reply::Done(status) => Err(Failure::Problem(format!(
                "a closed session started a terminal: {status}"
            ))),
            other => Err(self.fail(g, format!("a terminal for a closed session: {other}"))),
        }
    }

    /// The client of a terminal that stopped is let go.
    async fn release(&mut self, g: u64, what: &str) -> Played {
        if self.attachment.is_some() {
            self.screen(
                g,
                &format!("the client to be let go by {what}"),
                SEEN,
                |v| v.end.is_some(),
            )
            .await?;
        }
        self.attachment = None;
        self.attached = false;
        Ok(())
    }

    /// The session's terminal stopped: nothing of it may run, and its
    /// sockets and rc file are gone.
    async fn stopped(&mut self, g: u64, status: Status, what: &str) -> Played {
        self.status = status;
        self.running = false;
        self.attached = false;
        self.attachment = None;
        self.pi = None;
        self.here.clear();
        self.marker = None;
        self.terminal = None;
        // Anything of the session a crash did not strike: the terminal's
        // processes, and any that left its process groups or its session.
        let id = self.id.clone();
        let stage = Arc::clone(&self.stage);
        let left = stage
            .wait_gone(
                |p, l| {
                    l.session.as_deref() == Some(id.as_str())
                        && stage.crash_of(p, l).is_none()
                        && !stage.reported(p)
                },
                GRACE,
            )
            .await;
        for (process, label) in left {
            stage.report(&process);
            self.problems.push(format!(
                "{}: process {} ({}) outlived {what}: {}",
                self.name, process.pid, label.role, process.command
            ));
        }
        let mut files = vec![stage.sockets.join(format!("pty-{id}.sock"))];
        if let Some(generation) = self.generation.take() {
            files.push(stage.sockets.join(format!("pi-{generation}.sock")));
            files.extend(rc_files(&stage.data, &id).remove(&generation));
        }
        for file in files.into_iter().filter(|file| file.exists()) {
            self.problems
                .push(format!("{}: {what} left {}", self.name, file.display()));
        }
        if self.run.is_some() {
            self.check_run(g, status.name()).await?;
        }
        Ok(())
    }

    /// The session's graph is in the state its session's is.
    async fn check_run(&mut self, g: u64, expected: &str) -> Played {
        let reply = self.call("run.inspect", json!({}), true, None).await;
        let run = self.done(g, "run.inspect", reply)?;
        if run["status"] != expected {
            // A server that restarted since has not resumed the run yet.
            if self.stage.interrupted(g) {
                return Err(Failure::Crash);
            }
            self.problems.push(format!(
                "{}: the session's graph is {} where it should be {expected}",
                self.name, run["status"]
            ));
        }
        Ok(())
    }

    /// Every move the server accepted is in the graph's history once, and
    /// none it refused; one whose reply was lost at most once.
    async fn check_moves(&mut self, g: u64, when: &str) -> Played {
        let path = self.stage.dir.join(format!("export-{}.json", self.name));
        let _ = std::fs::remove_file(&path);
        let reply = self
            .call("inspect.export", json!({"path": path}), true, None)
            .await;
        self.done(g, "inspect.export", reply)?;
        let export: Value = std::fs::read(&path)
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .unwrap_or_default();
        let history = match History::parse(&export) {
            Ok(history) => history,
            Err(error) => {
                self.problems.push(format!(
                    "{}: an unreadable export {when}: {error:#}",
                    self.name
                ));
                return Ok(());
            }
        };
        if self.stage.interrupted(g) {
            return Err(Failure::Crash);
        }
        let mut found: BTreeMap<Vec<u8>, usize> = BTreeMap::new();
        for activation in history.activations.values() {
            *found.entry(activation.result.clone()).or_default() += 1;
        }
        for (tag, fate) in &self.moves {
            let times = found.get(tag.as_bytes()).copied().unwrap_or(0);
            let wrong = match fate {
                Fate::Done => times != 1,
                Fate::Refused => times != 0,
                Fate::Unknown => times > 1,
            };
            if wrong {
                self.problems.push(format!(
                    "{}: move {tag} ({fate:?}) is in the graph's history {times} times {when}",
                    self.name
                ));
            }
        }
        Ok(())
    }

    async fn start(&mut self, g: u64) -> Played {
        if self.status != Status::Active {
            return Ok(());
        }
        let reply = self
            .call(
                "flow.start",
                json!({"document": document(), "start_id": self.start_id}),
                true,
                None,
            )
            .await;
        let started = self.done(g, "flow.start", reply)?;
        let run = started["run_id"].as_str().unwrap_or_default().to_owned();
        if let Some(previous) = &self.run
            && previous != &run
        {
            return Err(Failure::Problem(format!(
                "a retried start made a second run, {run}, after {previous}"
            )));
        }
        self.run = Some(run.clone());
        self.count("graphs");
        // The session owns the run from now on.
        let record = self.inspect(g).await?;
        if record["run_id"] != run.as_str() {
            return Err(Failure::Problem(format!(
                "flow.start made run {run}, but the session is bound to {}",
                record["run_id"]
            )));
        }
        self.check_run(g, "active").await
    }

    async fn submit(&mut self, g: u64) -> Played {
        if self.status != Status::Active || self.run.is_none() {
            return Ok(());
        }
        let tag = format!("{}.m{}", self.name, self.moves.len() + 1);
        let args = json!({
            "trigger": {"kind": "root", "node_id": "source", "authority": ["workflow"]},
            "result": tag,
            "emissions": [{"edge_id": "flow", "payload": tag, "authority": {"kind": "carry"}}],
        });
        let reply = self.call("workflow.submit", args, true, None).await;
        let fate = match &reply {
            Reply::Done(_) => Fate::Done,
            Reply::Uncertain(_) => Fate::Unknown,
            _ => Fate::Refused,
        };
        self.moves.push((tag, fate));
        self.count("moves");
        self.done(g, "workflow.submit", reply).map(|_| ())
    }

    /// Compares the model with what the server says of the session.
    async fn verify(&mut self, g: u64) -> Played {
        let reply = self.call("session.list", json!({}), false, None).await;
        let list = self.done(g, "session.list", reply)?;
        let Some(record) = list["sessions"]
            .as_array()
            .and_then(|all| all.iter().find(|s| s["session_id"] == self.id.as_str()))
            .cloned()
        else {
            return Err(Failure::Problem("session.list lost the session".into()));
        };
        let mut wrong = Vec::new();
        if record["status"] != self.status.name() {
            wrong.push(format!(
                "session.list says {} where the script has {}",
                record["status"],
                self.status.name()
            ));
        }
        if record["run_id"] != json!(self.run) {
            wrong.push(format!(
                "its run is {}, not {:?}",
                record["run_id"], self.run
            ));
        }
        if record["pi"]["active_conversation_id"] != self.conversation.as_str() {
            wrong.push(format!(
                "its active conversation became {}",
                record["pi"]["active_conversation_id"]
            ));
        }
        let terminal = self.terminal_status(g).await?;
        if terminal["running"] != self.running {
            wrong.push(format!(
                "its terminal running is {}, not {}",
                terminal["running"], self.running
            ));
        } else if self.running {
            if terminal["attached"] != self.attached {
                wrong.push(format!(
                    "its terminal attached is {}, not {}",
                    terminal["attached"], self.attached
                ));
            }
            if terminal["manager_mode"] != self.program.name() {
                wrong.push(format!(
                    "its program is {} (error {}), not {}",
                    terminal["manager_mode"],
                    terminal["manager_error"],
                    self.program.name()
                ));
            }
            if let Some(known) = &self.terminal
                && terminal["terminal_id"] != known.id.as_str()
            {
                wrong.push(format!("its terminal became {}", terminal["terminal_id"]));
            }
        }
        if !wrong.is_empty() {
            return Err(self.fail(g, format!("the server disagrees: {}", wrong.join("; "))));
        }
        let faults = self.view().problems;
        if !faults.is_empty() {
            return Err(Failure::Problem(format!(
                "the client saw {}",
                faults.join("; ")
            )));
        }
        Ok(())
    }

    /// After a crash: waits for the new server, learns what became of a
    /// lifecycle change whose reply was lost, and attaches again, which
    /// resumes the session. Then nothing of the crashed server's terminal of
    /// this session may run.
    async fn recover(&mut self, mut since: u64) {
        loop {
            let now = self.stage.restarted(since).await;
            // A crash takes the server's terminals, and this client's
            // attachment with them; a new server knows no environment.
            self.attachment = None;
            self.attached = false;
            self.running = false;
            self.terminal = None;
            self.pi = None;
            self.here.clear();
            self.marker = None;
            self.generation = None;
            let recovered = match self.reconcile(now).await {
                Ok(()) if self.status == Status::Active => self.open(now).await,
                other => other,
            };
            match recovered {
                Ok(()) => {
                    self.seen = now;
                    break;
                }
                Err(Failure::Crash) => since = now,
                Err(Failure::Problem(problem)) => {
                    self.problems
                        .push(format!("{} recovering from a crash: {problem}", self.name));
                    self.abandoned = true;
                    return;
                }
            }
        }
        self.count("recoveries");
        self.note(&format!("  recovered: {}", self.status.name()));
        if self.status == Status::Active {
            // The graph resumed with its session, with its history; a later
            // crash leaves this to the next recovery.
            if self.run.is_some() {
                let g = self.seen;
                let checked = match self.check_run(g, "active").await {
                    Ok(()) => self.check_moves(g, "after the crash").await,
                    failed => failed,
                };
                if let Err(Failure::Problem(problem)) = checked {
                    self.problems.push(format!("{}: {problem}", self.name));
                }
            }
            let id = self.id.clone();
            let stage = Arc::clone(&self.stage);
            for (process, label) in stage
                .wait_gone(
                    |p, l| {
                        l.session.as_deref() == Some(id.as_str()) && stage.crash_of(p, l).is_some()
                    },
                    GRACE,
                )
                .await
            {
                // Only if the session is still resumed on this server.
                if !self.stage.interrupted(self.seen) {
                    stage.leftover(process, label, When::Resumed);
                }
            }
        }
    }

    /// A lifecycle change whose reply a crash took: the session is as it was
    /// or as the change leaves it. An interrupted suspension or close stays
    /// visible, and retrying finishes it.
    async fn reconcile(&mut self, g: u64) -> Played {
        let Some(pending) = self.pending.take() else {
            return Ok(());
        };
        let record = self.inspect(g).await?;
        let status = record["status"].as_str().unwrap_or_default().to_owned();
        let (allowed, finish): (&[&str], Option<&str>) = match pending {
            Lifecycle::Resume => (&["active", "suspended"], None),
            Lifecycle::Suspend | Lifecycle::Exit => (
                &["active", "suspending", "suspended"],
                Some("session.suspend"),
            ),
            Lifecycle::Close => (&["active", "closing", "closed"], Some("session.close")),
        };
        if !allowed.contains(&status.as_str()) {
            return Err(Failure::Problem(format!(
                "after a crash took the reply to {pending:?}, the session is {status}"
            )));
        }
        let status = match (status.as_str(), finish) {
            ("suspending" | "closing", Some(operation)) => {
                self.pending = Some(pending);
                let reply = self
                    .call(operation, json!({"session_id": self.id}), false, None)
                    .await;
                let record = self.done(g, operation, reply)?;
                self.pending = None;
                record["status"].as_str().unwrap_or_default().to_owned()
            }
            _ => status,
        };
        self.status = match status.as_str() {
            "active" => Status::Active,
            "suspended" => Status::Suspended,
            "closed" => Status::Closed,
            other => {
                return Err(Failure::Problem(format!(
                    "finishing {pending:?} after a crash left the session {other}"
                )));
            }
        };
        Ok(())
    }

    /// After the orderly stop and a restart: the graph comes back with its
    /// session, with its history.
    pub async fn graph_after_restart(&mut self) {
        if self.run.is_none() || self.status == Status::Closed || self.abandoned {
            return;
        }
        let g = self.stage.generation();
        self.seen = g;
        self.activations += 1;
        let marker = format!("{}#{}", self.name, self.activations);
        let reply = self
            .call(
                "session.resume",
                json!({"session_id": self.id}),
                false,
                Some(&marker),
            )
            .await;
        let checked = match self.done(g, "session.resume after the restart", reply) {
            Ok(_) => match self.check_run(g, "active").await {
                Ok(()) => {
                    self.check_moves(g, "after the orderly stop and restart")
                        .await
                }
                failed => failed,
            },
            Err(failed) => Err(failed),
        };
        match checked {
            Ok(()) => self.status = Status::Active,
            Err(Failure::Problem(problem)) => {
                self.problems.push(format!("{}: {problem}", self.name));
            }
            Err(Failure::Crash) => self
                .problems
                .push(format!("{}: the restarted server went away", self.name)),
        }
    }

    /// Pi's witness against what was typed: every line whose echo the client
    /// saw reached a Pi of this session exactly once, any other at most once,
    /// each Pi got its lines in the order typed, and nothing reached Pi that
    /// was not typed at Pi.
    pub fn judge_witness(&self) -> Vec<String> {
        let mut problems = Vec::new();
        let mut received: BTreeMap<&str, usize> = BTreeMap::new();
        let mut order: BTreeMap<u64, Vec<usize>> = BTreeMap::new();
        let position: BTreeMap<&str, usize> = self
            .typed
            .iter()
            .enumerate()
            .map(|(i, typed)| (typed.token.as_str(), i))
            .collect();
        let events = self.stage.witness(&self.id);
        for event in events.iter().filter(|e| e["event"] == "input") {
            let line = event["line"].as_str().unwrap_or_default();
            let pid = event["pid"].as_u64().unwrap_or_default();
            if COMMANDS.contains(&line) {
                continue;
            }
            match position.get(line) {
                Some(index) => {
                    *received.entry(line).or_default() += 1;
                    order.entry(pid).or_default().push(*index);
                }
                None if is_token(line) => problems.push(format!(
                    "{}: its Pi {pid} received {line}, typed in another session",
                    self.name
                )),
                None => problems.push(format!(
                    "{}: its Pi {pid} received {line:?}, which was never typed at Pi",
                    self.name
                )),
            }
        }
        for typed in &self.typed {
            let times = received.get(typed.token.as_str()).copied().unwrap_or(0);
            if (typed.seen && times != 1) || times > 1 {
                problems.push(format!(
                    "{}: the line {} reached Pi {times} times{}",
                    self.name,
                    typed.token,
                    if typed.seen {
                        ", and its echo was seen"
                    } else {
                        ""
                    }
                ));
            }
        }
        for (pid, indices) in order {
            if indices.windows(2).any(|pair| pair[0] >= pair[1]) {
                let lines: Vec<&str> = indices
                    .iter()
                    .map(|i| self.typed[*i].token.as_str())
                    .collect();
                problems.push(format!(
                    "{}: Pi {pid} received its lines out of order: {lines:?}",
                    self.name
                ));
            }
        }
        problems
    }

    /// Lets go of the terminal, as the end of the harness does.
    pub fn let_go(&mut self) {
        self.attachment = None;
    }
}

/// The session graph: two external nodes, as in examples/flow.json.
fn document() -> Value {
    json!({
        "name": "session-graph",
        "entry": "source",
        "nodes": [
            {"id": "source", "component": "external"},
            {"id": "sink", "component": "external"}
        ],
        "edges": [{"from": "source", "to": "sink", "name": "flow"}]
    })
}

fn ready_pid(view: &View) -> Option<u32> {
    view.lines("READY ")
        .first()?
        .split_whitespace()
        .next()?
        .parse()
        .ok()
}

/// A line the driver typed: `s<index>.<number>`.
fn is_token(line: &str) -> bool {
    line.split_once('.').is_some_and(|(session, number)| {
        session.len() > 1
            && session.starts_with('s')
            && session[1..].bytes().all(|b| b.is_ascii_digit())
            && number.bytes().all(|b| b.is_ascii_digit())
    })
}

fn same_path(a: &Path, b: &Path) -> bool {
    match (std::fs::canonicalize(a), std::fs::canonicalize(b)) {
        (Ok(a), Ok(b)) => a == b,
        _ => a == b,
    }
}

/// The last rows of a screen with anything on them, for a problem's text.
fn tail(view: &View) -> String {
    let rows: Vec<&str> = view
        .rows
        .iter()
        .map(String::as_str)
        .filter(|row| !row.is_empty())
        .collect();
    let from = rows.len().saturating_sub(4);
    format!("{:?}", &rows[from..])
}
