//! Bounded node-scoped package pages. Global graph previews are never package indexes.

use crate::{AppError, Result, client::Client};
use serde_json::{Value, json};
use std::{future::Future, time::Duration};
use tokio::sync::mpsc;

const PAGE_SIZE: usize = 100;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Selection {
    pub server_id: String,
    pub run_id: String,
    pub node_id: String,
}

#[derive(Default)]
struct Page {
    packages: Vec<Value>,
    next_after: Option<String>,
}

pub(super) struct Pages {
    revision: String,
    received: Page,
    outbound: Page,
}

pub(super) struct Request {
    id: String,
    selection: Selection,
    received: Option<Option<String>>,
    outbound: Option<Option<String>>,
    expected_revision: Option<String>,
    open_history: bool,
}

pub(super) struct Response {
    request: Request,
    result: Result<Pages>,
}

#[derive(Default)]
pub(super) struct State {
    selection: Option<Selection>,
    pages: Option<Pages>,
    pending: Option<String>,
    error: Option<String>,
    cursor: usize,
}

impl State {
    pub fn sync(&mut self, selection: Option<Selection>) {
        if self.selection != selection {
            *self = Self {
                selection,
                ..Self::default()
            };
        }
    }

    pub fn request(&mut self, next: bool, open_history: bool) -> Result<Option<Request>> {
        if self.pending.is_some() {
            return Ok(None);
        }
        let selection = self.selection.clone().ok_or_else(|| {
            AppError::new(
                "no_node_selected",
                "Select a live node before inspecting its packages",
            )
        })?;
        let (received, outbound, expected_revision) = if next {
            let pages = self.pages.as_ref().ok_or_else(|| {
                AppError::new(
                    "no_package_page",
                    "Press Enter or r to load the first package pages",
                )
            })?;
            let received = pages.received.next_after.clone().map(Some);
            let outbound = pages.outbound.next_after.clone().map(Some);
            if received.is_none() && outbound.is_none() {
                return Err(AppError::new(
                    "no_more_packages",
                    "These pages reached the end; r refreshes from the beginning",
                ));
            }
            (received, outbound, Some(pages.revision.clone()))
        } else {
            (Some(None), Some(None), None)
        };
        let id = uuid::Uuid::new_v4().to_string();
        self.pending = Some(id.clone());
        self.error = None;
        Ok(Some(Request {
            id,
            selection,
            received,
            outbound,
            expected_revision,
            open_history,
        }))
    }

    /// Late responses cannot repopulate a changed selection or superseded request.
    pub fn accept(&mut self, response: Response) -> Option<bool> {
        if self.selection.as_ref() != Some(&response.request.selection)
            || self.pending.as_deref() != Some(response.request.id.as_str())
        {
            return None;
        }
        self.pending = None;
        match response.result {
            Ok(pages) => {
                self.pages = Some(pages);
                self.cursor = 0;
                self.error = None;
            }
            Err(error) => {
                self.pages = None;
                self.error = Some(error.to_string());
            }
        }
        Some(response.request.open_history)
    }

    pub fn loaded(&self) -> bool {
        self.pages.is_some()
    }

    pub fn history_args(&mut self) -> Option<Value> {
        let pages = self.pages.as_ref()?;
        let count = pages.received.packages.len() + pages.outbound.packages.len();
        if count == 0 {
            return None;
        }
        let package = pages
            .received
            .packages
            .iter()
            .chain(&pages.outbound.packages)
            .nth(self.cursor % count)?;
        self.cursor = self.cursor.wrapping_add(1);
        Some(json!({"run_id":self.selection.as_ref()?.run_id,"package_id":package["package_id"]}))
    }

    pub fn details(&self, observed_revision: &Value) -> Value {
        match &self.pages {
            Some(pages) => {
                json!({"received":pages.received.packages,"outbound":pages.outbound.packages,"package_pages":{"revision":pages.revision,"graph_revision_differs":observed_revision.as_str()!=Some(pages.revision.as_str()),"limit_per_phase":PAGE_SIZE,"received_next_after":pages.received.next_after,"outbound_next_after":pages.outbound.next_after,"loading":self.pending.is_some(),"controls":"Graph: o cycles histories in these pages; n loads next pages; r refreshes first pages"}})
            }
            None => {
                json!({"received":null,"outbound":null,"package_pages":{"loading":self.pending.is_some(),"error":self.error,"controls":"Graph: Enter or r loads node-scoped pages; o loads and opens a history"}})
            }
        }
    }
}

fn args(request: &Request, phase: &str, after: &Option<Option<String>>) -> Option<Value> {
    let after = after.as_ref()?;
    let mut value = json!({"run_id":request.selection.run_id,"node_id":request.selection.node_id,"phase":phase,"limit":PAGE_SIZE});
    if let Some(after) = after {
        value["after"] = json!(after);
    }
    Some(value)
}

async fn load<F, Fut>(request: &Request, query: F) -> Result<Pages>
where
    F: Fn(Value) -> Fut,
    Fut: Future<Output = Result<Value>>,
{
    let phase = |phase, after: &Option<Option<String>>| {
        let args = args(request, phase, after);
        let query = &query;
        async move {
            match args {
                Some(args) => query(args).await.map(Some),
                None => Ok(None),
            }
        }
    };
    let (received, outbound) = tokio::join!(
        phase("received", &request.received),
        phase("outbound", &request.outbound)
    );
    let (received, outbound) = (received?, outbound?);
    let mut revision = request.expected_revision.clone();
    let mut parse = |value: Option<Value>| -> Result<Page> {
        let Some(value) = value else {
            return Ok(Page::default());
        };
        let current = value["revision"]
            .as_str()
            .ok_or_else(|| AppError::new("protocol_error", "Frontier response has no revision"))?;
        if revision
            .as_deref()
            .is_some_and(|expected| expected != current)
        {
            return Err(AppError::new(
                "frontier_changed",
                "Frontier changed between pages; press r to start a fresh node inspection",
            ));
        }
        revision = Some(current.into());
        let packages = value["packages"]
            .as_array()
            .ok_or_else(|| {
                AppError::new("protocol_error", "Frontier response has no package page")
            })?
            .clone();
        if packages.len() > PAGE_SIZE
            || packages
                .iter()
                .any(|p| p["node_id"].as_str() != Some(request.selection.node_id.as_str()))
        {
            return Err(AppError::new(
                "protocol_error",
                "Frontier response exceeds or escapes the selected node page",
            ));
        }
        Ok(Page {
            packages,
            next_after: value["next_after"].as_str().map(str::to_owned),
        })
    };
    let received = parse(received)?;
    let outbound = parse(outbound)?;
    Ok(Pages {
        revision: revision.unwrap_or_default(),
        received,
        outbound,
    })
}

pub(super) fn submit(client: &Client, tx: &mpsc::Sender<Response>, request: Request) {
    let client = client.clone();
    let tx = tx.clone();
    tokio::spawn(async move {
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            load(&request, |args| client.call("inspect.frontier", args)),
        )
        .await
        .unwrap_or_else(|_| {
            Err(AppError::new(
                "frontier_timeout",
                "Node package query timed out; press r to retry",
            ))
        });
        let _ = tx.send(Response { request, result }).await;
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{persistence::Paths, state::Service};

    fn selection(node: &str) -> Selection {
        Selection {
            server_id: "server-1".into(),
            run_id: "run-1".into(),
            node_id: node.into(),
        }
    }

    #[tokio::test]
    async fn selected_node_pages_reach_packages_missing_from_first_100_global_items() {
        let directory = tempfile::tempdir().unwrap();
        let service =
            Service::new(Paths::initialize(directory.path().join("data")).unwrap()).unwrap();
        let document: serde_json::Value =
            serde_json::from_str(include_str!("../../examples/flow.json")).unwrap();
        let started = crate::tools::dispatch(
            &service,
            "flow.start",
            &json!({"document":document,"project":directory.path()}),
        )
        .await
        .unwrap();
        let run_id = started["run_id"].as_str().unwrap();
        let mut emissions = (0..200)
            .map(|i| json!({"object_type":"Text","payload":format!("A-{i}")}))
            .collect::<Vec<_>>();
        emissions.push(json!({"edge_id":"A_to_B","payload":"input"}));
        crate::tools::dispatch(&service,"workflow.submit",&json!({"run_id":run_id,"trigger":{"kind":"root","node_id":"A","authority":["work"]},"result":"A","emissions":emissions})).await.unwrap();
        let input = crate::tools::dispatch(
            &service,
            "inspect.frontier",
            &json!({"run_id":run_id,"node_id":"B","phase":"received"}),
        )
        .await
        .unwrap();
        let emissions = (0..200)
            .map(|i| json!({"object_type":"Text","payload":format!("B-{i}")}))
            .collect::<Vec<_>>();
        crate::tools::dispatch(&service,"workflow.submit",&json!({"run_id":run_id,"trigger":{"kind":"packages","package_ids":[input["packages"][0]["package_id"]]},"result":"B","emissions":emissions})).await.unwrap();
        let global = crate::tools::dispatch(&service, "run.inspect", &json!({"run_id":run_id}))
            .await
            .unwrap();
        let global_packages = global["frontier"]["outbound"].as_array().unwrap();
        assert_eq!(global_packages.len(), 100);
        let node = ["A", "B"]
            .into_iter()
            .find(|node| {
                global_packages
                    .iter()
                    .all(|p| p["node_id"].as_str() != Some(*node))
            })
            .expect("one producer's 200 packages are beyond the global preview");
        let mut state = State::default();
        state.sync(Some(Selection {
            server_id: service.server_id.clone(),
            run_id: run_id.into(),
            node_id: node.into(),
        }));
        let service_ref = &service;
        let query = |args| async move {
            crate::tools::dispatch(service_ref, "inspect.frontier", &args).await
        };
        let request = state.request(false, true).unwrap().unwrap();
        let pages = load(&request, query).await.unwrap();
        assert_eq!(pages.outbound.packages.len(), 100);
        assert!(pages.outbound.packages.iter().all(|p| p["node_id"] == node));
        assert!(pages.outbound.next_after.is_some());
        assert_eq!(
            state.accept(Response {
                request,
                result: Ok(pages)
            }),
            Some(true)
        );
        let first_history = state.history_args().unwrap();
        let history = crate::tools::dispatch(&service, "inspect.package", &first_history)
            .await
            .unwrap();
        assert!(!history.is_null());
        let request = state.request(true, false).unwrap().unwrap();
        let pages = load(&request, query).await.unwrap();
        assert_eq!(pages.outbound.packages.len(), 100);
        state.accept(Response {
            request,
            result: Ok(pages),
        });
        assert_ne!(
            state.history_args().unwrap()["package_id"],
            first_history["package_id"]
        );
        let request = state.request(true, false).unwrap().unwrap();
        let pages = load(&request, query).await.unwrap();
        assert!(pages.outbound.packages.is_empty());
        state.accept(Response {
            request,
            result: Ok(pages),
        });
        assert_eq!(
            state.request(true, false).err().unwrap().code,
            "no_more_packages"
        );
        service.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn mixed_or_changed_revisions_require_restart_and_late_responses_are_discarded() {
        let mut state = State::default();
        state.sync(Some(selection("A")));
        let request = state.request(false, false).unwrap().unwrap();
        let mixed=load(&request,|args|async move {Ok(json!({"revision":if args["phase"]=="received" {"1"}else{"2"},"packages":[],"next_after":null}))}).await;
        assert_eq!(mixed.err().unwrap().code, "frontier_changed");
        state.sync(Some(selection("B")));
        assert!(
            state
                .accept(Response {
                    request,
                    result: Err(AppError::new("old", "old selection"))
                })
                .is_none()
        );
        let request = state.request(false, false).unwrap().unwrap();
        let pages = load(&request, |_| async {
            Ok(json!({"revision":"2","packages":[],"next_after":"next"}))
        })
        .await
        .unwrap();
        state.accept(Response {
            request,
            result: Ok(pages),
        });
        let request = state.request(true, false).unwrap().unwrap();
        let changed = load(&request, |_| async {
            Ok(json!({"revision":"3","packages":[],"next_after":null}))
        })
        .await;
        assert_eq!(changed.err().unwrap().code, "frontier_changed");
        state.sync(Some(Selection {
            server_id: "server-2".into(),
            ..selection("B")
        }));
        assert!(
            state
                .accept(Response {
                    request,
                    result: Err(AppError::new("old", "old server"))
                })
                .is_none()
        );
        assert!(!state.loaded());
    }
}
