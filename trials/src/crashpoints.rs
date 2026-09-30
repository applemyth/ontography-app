//! Crash points: crashes where random ones rarely land, between a commit
//! and its reply or inside a multi-step durable write. A library loaded into
//! the server (see `interpose`) numbers every durable system call it makes
//! under its data directory. For each script a dry run learns the points;
//! then each chosen point is played from a fresh data directory with the
//! library armed there: the server is killed just before or just after that
//! call, or the call fails with EIO or ENOSPC. A killed server is started
//! again without the library, the run resumed, and the script finished.
//!
//! The judges ask what the documentation promises (docs/WORKFLOWS.md,
//! docs/SESSIONS.md): the server starts and the run opens again; a move
//! whose reply was done is in history once, a lost one at most once, a
//! refused one never; the store agrees with the export; an orderly restart
//! changes nothing; an orderly stop leaves no processes, socket directories
//! or half-written files. A failed call must fail its request cleanly, and
//! the server must go on serving. Problems a dry run has too, without any
//! crash, are the baseline, reported once per script.

use crate::game::Log;
use crate::history::History;
use crate::interpose::{self, Action};
use crate::judge;
use crate::recovery::Driver;
use crate::scripts::Script;
use anyhow::Result;
use rand::{SeedableRng, seq::IndexedRandom, seq::SliceRandom};
use rand_chacha::ChaCha8Rng;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

#[derive(clap::Args, Debug)]
pub struct Args {
    /// Only this script.
    #[arg(long, value_enum)]
    script: Option<Script>,
    /// Only this action at the armed point.
    #[arg(long, value_enum)]
    mode: Option<Action>,
    /// Points to try per script and mode, as a seeded sample; 0 tries all.
    #[arg(long, default_value_t = 20)]
    limit: usize,
    /// The sample's seed; a new one by default.
    #[arg(long)]
    seed: Option<u64>,
    /// Exactly these points, numbered as the dry run numbers them.
    #[arg(long, value_delimiter = ',')]
    point: Vec<usize>,
    /// Points played at once, each with its own server.
    #[arg(long, default_value_t = 4)]
    jobs: usize,
}

/// One counted call of a script's run, named.
#[derive(Clone, Debug)]
struct Point {
    index: usize,
    label: String,
    step: String,
}

/// One play of a script: a dry run, or a run armed at a point.
struct Played {
    script: Script,
    armed: Option<(usize, Action)>,
    dir: PathBuf,
    points: Vec<Point>,
    focus: usize,
    problems: Vec<String>,
    notes: Vec<String>,
    took: Duration,
}

impl Played {
    /// The armed point, if the server reached it.
    fn reached(&self) -> Option<&Point> {
        let (at, _) = self.armed?;
        self.points.iter().find(|point| point.index == at)
    }

    fn name(&self) -> String {
        match self.armed {
            None => format!("{}-dry", self.script.name()),
            Some((at, action)) => format!("{}-{}-{at}", self.script.name(), action.name()),
        }
    }
}

struct Setting {
    binary: PathBuf,
    library: PathBuf,
    root: PathBuf,
}

pub async fn run(args: Args, binary: &Path, runs: &Path) -> Result<()> {
    let root = runs.join("crashpoints");
    std::fs::create_dir_all(&root)?;
    crate::clear_stale(&root);
    let library = interpose::build(&root)?;
    let setting = Arc::new(Setting {
        binary: binary.into(),
        library,
        root,
    });
    let seed = args.seed.unwrap_or_else(|| rand::random::<u32>() as u64);
    let scripts = args.script.map_or(Script::ALL.to_vec(), |s| vec![s]);
    let actions = args.mode.map_or(Action::ALL.to_vec(), |a| vec![a]);
    println!(
        "crashpoints: seed {seed}, limit {} per script and mode, {} at once",
        args.limit, args.jobs
    );
    let started = Instant::now();

    // A dry run of each script learns its points and its baseline.
    let queue = scripts.iter().map(|script| (*script, None)).collect();
    let mut dry: BTreeMap<Script, Played> = BTreeMap::new();
    let mut failed = false;
    for (script, _, played) in play_all(&setting, queue, args.jobs, |_| {}).await {
        match played {
            Ok(played) => {
                let after = played.points.len().saturating_sub(played.focus);
                println!(
                    "{}: dry run, {} points, {after} after setup, {:.1}s",
                    script.name(),
                    played.points.len(),
                    played.took.as_secs_f64()
                );
                keep_or_remove(&played, !played.problems.is_empty());
                dry.insert(script, played);
            }
            Err(error) => {
                failed = true;
                println!("{}: the dry run could not run: {error:#}", script.name());
            }
        }
    }

    let mut queue = Vec::new();
    for (script, played) in &dry {
        for action in &actions {
            for at in choose(played, &args, seed, *script, *action) {
                queue.push((*script, Some((at, *action))));
            }
        }
    }
    println!("{} points to play", queue.len());
    let baselines: BTreeMap<Script, BTreeSet<String>> = dry
        .iter()
        .map(|(script, played)| (*script, general(played)))
        .collect();
    let progress = |played: &Played| {
        let new = new_problems(played, &baselines[&played.script]);
        let (at, action) = played.armed.expect("armed");
        let place = played.reached().map_or("not reached".into(), |p| {
            format!("[{}] {}", p.step, p.label)
        });
        println!(
            "  {}/{} {at} {place}: {}, {:.1}s",
            played.script.name(),
            action.name(),
            match new.len() {
                0 => "fine".into(),
                n => format!("{n} problems"),
            },
            played.took.as_secs_f64()
        );
    };
    let mut results = Vec::new();
    for (script, armed, played) in play_all(&setting, queue, args.jobs, progress).await {
        match played {
            Ok(played) => {
                let new = new_problems(&played, &baselines[&script]);
                keep_or_remove(&played, !new.is_empty());
                failed |= !new.is_empty();
                results.push(played);
            }
            Err(error) => {
                failed = true;
                let (at, action) = armed.expect("armed");
                println!(
                    "  {}/{} {at}: could not play: {error:#}",
                    script.name(),
                    action.name()
                );
            }
        }
    }
    summarize(&dry, &baselines, &results, &actions);
    println!(
        "{} points played, {:.0}s",
        results.len(),
        started.elapsed().as_secs_f64()
    );
    if failed {
        std::process::exit(1);
    }
    Ok(())
}

type Job = (Script, Option<(usize, Action)>);

/// Plays each job, `jobs` at once, calling `each` as each finishes.
async fn play_all(
    setting: &Arc<Setting>,
    queue: Vec<Job>,
    jobs: usize,
    mut each: impl FnMut(&Played),
) -> Vec<(Script, Option<(usize, Action)>, Result<Played>)> {
    let mut queue = queue.into_iter();
    let mut running = JoinSet::new();
    let mut done = Vec::new();
    loop {
        while running.len() < jobs.max(1) {
            let Some((script, armed)) = queue.next() else {
                break;
            };
            let setting = Arc::clone(setting);
            running.spawn(async move { (script, armed, play(&setting, script, armed).await) });
        }
        let Some(joined) = running.join_next().await else {
            break;
        };
        let (script, armed, played) = match joined {
            Ok(result) => result,
            Err(error) => {
                println!("a point's task failed: {error}");
                continue;
            }
        };
        if let Ok(played) = &played {
            each(played);
        }
        done.push((script, armed, played));
    }
    done
}

/// Plays a script from a fresh directory, dry or armed, and judges it.
async fn play(setting: &Setting, script: Script, armed: Option<(usize, Action)>) -> Result<Played> {
    let clock = Instant::now();
    let mut played = Played {
        script,
        armed,
        dir: PathBuf::new(),
        points: Vec::new(),
        focus: 0,
        problems: Vec::new(),
        notes: Vec::new(),
        took: Duration::ZERO,
    };
    let dir = setting.root.join(played.name());
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("witness"))?;
    // Problems name paths as the server sees them: canonical.
    let dir = dir.canonicalize()?;
    let witness = dir.join("witness");
    let program = std::env::current_exe()?;
    let world = script.world(&program, &witness);
    std::fs::write(
        dir.join("document.json"),
        serde_json::to_vec_pretty(&world.document)?,
    )?;
    let mut driver = Driver::start(&setting.binary, &dir, &setting.library, armed, &world).await?;
    let mut log = Log::default();
    script.play(&mut driver, &world, &mut log).await;
    let ending = driver.finish(&world).await;

    played.points = interpose::read(&driver.log)
        .iter()
        .map(|call| Point {
            index: call.index,
            label: interpose::label(call, &driver.data, &driver.run),
            step: call.step.clone(),
        })
        .collect();
    played.focus = driver.focus;
    let reached = played.reached().is_some();
    played.problems = std::mem::take(&mut driver.problems);
    played.notes = std::mem::take(&mut driver.notes);
    played
        .problems
        .extend(deaths(&driver, reached, &mut played.notes));
    if armed.is_some_and(|(_, action)| !action.kills()) && reached && !driver.excused {
        played
            .notes
            .push("no request failed: the app went on past the failed call".into());
    }

    // The workflow judge, with the leniency it gives chaos where a fault
    // was injected: an interrupted program may run again.
    log.crashes = usize::from(reached || !driver.deaths.is_empty());
    if let Some((status, export)) = &ending {
        match History::parse(export, &Default::default()) {
            Ok(history) => {
                // Scripts never edit their world: it has one version.
                let worlds = [Arc::new(world.clone())];
                let evidence = judge::Evidence {
                    worlds: &worlds,
                    history: &history,
                    log: &log,
                    status,
                    witness: judge::read_witness(&witness)?,
                };
                played.problems.extend(judge::judge(&evidence));
            }
            Err(error) => played
                .problems
                .push(format!("the final export does not parse: {error:#}")),
        }
    }
    played.problems.extend(preloaded(&witness));
    let mut seen = BTreeSet::new();
    played
        .problems
        .retain(|problem| seen.insert(problem.clone()));
    played.dir = dir;
    played.took = clock.elapsed();
    Ok(played)
}

/// How the server died. At a crash point it is killed, once; a failed call
/// must never end it, though a server may refuse to start cleanly.
fn deaths(driver: &Driver, reached: bool, notes: &mut Vec<String>) -> Vec<String> {
    let kills = reached && driver.armed.is_some_and(|(_, action)| action.kills());
    let mut problems = Vec::new();
    for (index, death) in driver.deaths.iter().enumerate() {
        if death.by_driver || (index == 0 && kills && death.killed()) {
            continue;
        }
        let clean = death
            .status
            .code()
            .is_some_and(|code| code != 0 && code != 101);
        if death.step == "server start" && !kills && clean {
            notes.push(format!(
                "the server refused to start at the failed call: {}",
                death.status
            ));
            continue;
        }
        problems.push(format!(
            "the server died during {}: {}",
            death.step, death.status
        ));
    }
    problems
}

/// Commands must never run with the library: the server's constructor
/// removes it from the environment its programs inherit.
fn preloaded(witness: &Path) -> Vec<String> {
    let mut problems = Vec::new();
    for entry in std::fs::read_dir(witness).into_iter().flatten().flatten() {
        let text = std::fs::read_to_string(entry.path()).unwrap_or_default();
        if text.contains("\"preloaded\":true") {
            problems.push(format!(
                "a program of {} ran with the crash-point library",
                entry.path().display()
            ));
        }
    }
    problems
}

/// The points to play for a script and action: those named, all when they
/// fit the limit, or else a seeded sample. Calls with the same label in the
/// same step, such as SQLite's page writes, are much alike, so the sample
/// takes a point from as many labels as it can before it takes a second
/// from any.
fn choose(dry: &Played, args: &Args, seed: u64, script: Script, action: Action) -> Vec<usize> {
    if !args.point.is_empty() {
        return args.point.clone();
    }
    let candidates: Vec<&Point> = dry.points.iter().filter(|p| p.index > dry.focus).collect();
    if args.limit == 0 || candidates.len() <= args.limit {
        return candidates.iter().map(|p| p.index).collect();
    }
    let mut alike: BTreeMap<(&str, &str), Vec<usize>> = BTreeMap::new();
    for point in &candidates {
        alike
            .entry((&point.label, &point.step))
            .or_default()
            .push(point.index);
    }
    let mut groups: Vec<Vec<usize>> = alike.into_values().collect();
    let mix = seed
        .wrapping_mul(1_000_003)
        .wrapping_add(script as u64 * 16 + action as u64);
    let mut rng = ChaCha8Rng::seed_from_u64(mix);
    groups.shuffle(&mut rng);
    let mut chosen: BTreeSet<usize> = groups
        .iter()
        .take(args.limit)
        .filter_map(|group| group.choose(&mut rng).copied())
        .collect();
    let mut rest: Vec<usize> = candidates
        .iter()
        .map(|p| p.index)
        .filter(|index| !chosen.contains(index))
        .collect();
    rest.shuffle(&mut rng);
    let missing = args.limit.saturating_sub(chosen.len());
    chosen.extend(rest.into_iter().take(missing));
    chosen.into_iter().collect()
}

/// A problem without what differs from one play to the next: directories,
/// the run's and other IDs, process IDs.
fn general_problem(problem: &str, dir: &Path) -> String {
    let text = problem.replace(&*dir.to_string_lossy(), "<dir>");
    let text = interpose::normalize(&text);
    // Process IDs and other long numbers.
    let mut out = String::new();
    let mut digits = String::new();
    for c in text.chars().chain(std::iter::once(' ')) {
        if c.is_ascii_digit() {
            digits.push(c);
            continue;
        }
        if digits.len() >= 4 {
            out.push_str("<n>");
        } else {
            out.push_str(&digits);
        }
        digits.clear();
        out.push(c);
    }
    out.pop();
    out
}

fn general(played: &Played) -> BTreeSet<String> {
    played
        .problems
        .iter()
        .map(|p| general_problem(p, &played.dir))
        .collect()
}

/// Problems a play has that its script's dry run did not.
fn new_problems(played: &Played, baseline: &BTreeSet<String>) -> Vec<String> {
    general(played)
        .into_iter()
        .filter(|p| !baseline.contains(p))
        .collect()
}

fn keep_or_remove(played: &Played, keep: bool) {
    if keep {
        println!("    kept {}", played.dir.display());
    } else {
        let _ = std::fs::remove_dir_all(&played.dir);
    }
}

/// Per script and mode: the points found and played, then the problems
/// grouped by the crash point that caused them, and a count of each note.
/// Last, each kind of problem, and of note, with the points that cause it.
fn summarize(
    dry: &BTreeMap<Script, Played>,
    baselines: &BTreeMap<Script, BTreeSet<String>>,
    results: &[Played],
    actions: &[Action],
) {
    // A kind of problem or note: where it came from, by script, mode and
    // crash point, and at which points.
    type Kinds = BTreeMap<String, BTreeMap<String, BTreeSet<usize>>>;
    let (mut problem_kinds, mut note_kinds) = (Kinds::new(), Kinds::new());
    println!("\nsummary");
    for (script, played) in dry {
        let baseline = &baselines[script];
        let after = played.points.len().saturating_sub(played.focus);
        println!(
            "{}: {} points, {after} after setup",
            script.name(),
            played.points.len()
        );
        for problem in baseline {
            println!("  baseline, without a crash too: {problem}");
        }
        for action in actions {
            let plays: Vec<&Played> = results
                .iter()
                .filter(|p| p.script == *script && p.armed.is_some_and(|(_, a)| a == *action))
                .collect();
            if plays.is_empty() {
                continue;
            }
            let reached = plays.iter().filter(|p| p.reached().is_some()).count();
            let average = plays.iter().map(|p| p.took).sum::<Duration>() / plays.len() as u32;
            let troubled = plays
                .iter()
                .filter(|p| !new_problems(p, baseline).is_empty())
                .count();
            println!(
                "  {}: played {}, reached {reached}, {:.1}s each, {troubled} with problems",
                action.name(),
                plays.len(),
                average.as_secs_f64(),
            );
            // By crash-point label: the points, and what they caused.
            let mut labels: BTreeMap<String, (BTreeSet<usize>, BTreeMap<String, usize>)> =
                BTreeMap::new();
            let mut notes: BTreeMap<String, usize> = BTreeMap::new();
            for played in &plays {
                let (at, _) = played.armed.expect("armed");
                let label = played.reached().map_or("(not reached)".into(), |p| {
                    format!("{} [{}]", p.label, p.step)
                });
                let place = format!("{}/{} {label}", script.name(), action.name());
                for problem in new_problems(played, baseline) {
                    let entry = labels.entry(label.clone()).or_default();
                    entry.0.insert(at);
                    *entry.1.entry(problem.clone()).or_default() += 1;
                    let places = problem_kinds.entry(problem).or_default();
                    places.entry(place.clone()).or_default().insert(at);
                }
                for note in &played.notes {
                    let note = general_problem(note, &played.dir);
                    *notes.entry(note.clone()).or_default() += 1;
                    let places = note_kinds.entry(note).or_default();
                    places.entry(place.clone()).or_default().insert(at);
                }
            }
            for (label, (points, problems)) in labels {
                println!("    {label} at {}", list(&points));
                for (problem, count) in problems {
                    println!("      {count}× {problem}");
                }
            }
            for (note, count) in notes {
                println!("    note {count}×: {note}");
            }
        }
    }
    for (title, kinds) in [
        ("problems", problem_kinds),
        ("notes, which are not problems", note_kinds),
    ] {
        if kinds.is_empty() {
            continue;
        }
        println!("\n{title} by kind, with the points that cause them");
        for (kind, places) in kinds {
            println!("  {kind}");
            for (place, points) in places {
                println!("    {place} at {}", list(&points));
            }
        }
    }
}

fn list(points: &BTreeSet<usize>) -> String {
    points
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}
