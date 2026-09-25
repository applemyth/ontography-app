//! A view of core topology. No graph mutations or local graph laws live here.

use std::collections::BTreeMap;

use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Color, Modifier, Style},
    widgets::Widget,
};
use serde::Deserialize;
use serde_json::Value;

const NODE_WIDTH: i32 = 26;
const ROW_HEIGHT: i32 = 5;

#[derive(Clone, Debug, Deserialize)]
pub struct NodeView {
    pub id: String,
    #[serde(default)]
    pub received: u64,
    #[serde(default)]
    pub outbound: u64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct EdgeView {
    pub id: String,
    pub source: String,
    pub target: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct GraphView {
    #[serde(default)]
    pub nodes: Vec<NodeView>,
    #[serde(default)]
    pub edges: Vec<EdgeView>,
}

impl GraphView {
    pub fn from_snapshot(snapshot: &Value) -> Result<Self, serde_json::Error> {
        let mut graph: Self = serde_json::from_value(snapshot["graph"].clone())?;
        graph.nodes.sort_by(|a, b| a.id.cmp(&b.id));
        graph.edges.sort_by(|a, b| a.id.cmp(&b.id));
        for node in &mut graph.nodes {
            let counts = &snapshot["frontier"]["counts"][&node.id];
            node.received = number(&counts["received"]);
            node.outbound = number(&counts["outbound"]);
        }
        Ok(graph)
    }

    pub fn selected_after_refresh(&self, old: Option<&str>) -> Option<String> {
        old.filter(|id| self.nodes.iter().any(|node| node.id == *id))
            .map(str::to_owned)
            .or_else(|| self.nodes.first().map(|node| node.id.clone()))
    }

    pub fn navigate(&self, selected: Option<&str>, delta: isize) -> Option<String> {
        if self.nodes.is_empty() {
            return None;
        }
        let index = selected
            .and_then(|id| self.nodes.iter().position(|node| node.id == id))
            .unwrap_or(0);
        let next = (index as isize + delta).rem_euclid(self.nodes.len() as isize) as usize;
        Some(self.nodes[next].id.clone())
    }

    pub fn node_y(&self, selected: &str) -> i32 {
        self.nodes
            .iter()
            .position(|node| node.id == selected)
            .unwrap_or(0) as i32
            * ROW_HEIGHT
    }

    pub fn incident_edges(&self, selected: &str) -> Vec<String> {
        self.edges
            .iter()
            .filter(|edge| edge.source == selected || edge.target == selected)
            .map(|edge| format!("{}: {} → {}", edge.id, edge.source, edge.target))
            .collect()
    }
}

fn number(value: &Value) -> u64 {
    value
        .as_u64()
        .or_else(|| value.as_str()?.parse().ok())
        .unwrap_or(0)
}

/// Every edge gets a distinct route, including parallel edges and self loops.
/// Stable string IDs are kept verbatim; viewport clipping cannot mutate topology.
pub struct GraphCanvas<'a> {
    pub graph: &'a GraphView,
    pub selected: Option<&'a str>,
    pub pan_x: i32,
    pub pan_y: i32,
}

impl Widget for GraphCanvas<'_> {
    fn render(self, area: Rect, buffer: &mut Buffer) {
        if area.is_empty() {
            return;
        }
        let normal = Style::default().fg(Color::DarkGray);
        let active = Style::default().fg(Color::Cyan);
        let rows: BTreeMap<_, _> = self
            .graph
            .nodes
            .iter()
            .enumerate()
            .map(|(index, node)| (node.id.as_str(), index as i32 * ROW_HEIGHT))
            .collect();
        for (index, edge) in self.graph.edges.iter().enumerate() {
            let (Some(source), Some(target)) = (
                rows.get(edge.source.as_str()),
                rows.get(edge.target.as_str()),
            ) else {
                continue;
            };
            let from = source + 1;
            let to = target + 2;
            let lane = NODE_WIDTH + 3 + index as i32 * 3;
            let style = if self.selected == Some(edge.source.as_str())
                || self.selected == Some(edge.target.as_str())
            {
                active
            } else {
                normal
            };
            for x in NODE_WIDTH.max(self.pan_x)..lane.min(self.pan_x + i32::from(area.width)) {
                self.route(buffer, area, x, from, "─", style);
                self.route(buffer, area, x, to, "─", style);
            }
            for y in (from.min(to) + 1).max(self.pan_y)
                ..from.max(to).min(self.pan_y + i32::from(area.height))
            {
                self.route(buffer, area, lane, y, "│", style);
            }
            self.route(
                buffer,
                area,
                lane,
                from,
                if from < to { "┐" } else { "┘" },
                style,
            );
            self.route(
                buffer,
                area,
                lane,
                to,
                if from < to { "┘" } else { "┐" },
                style,
            );
            self.put(buffer, area, NODE_WIDTH, to, "◀", style);
        }
        for (index, node) in self.graph.nodes.iter().enumerate() {
            let y = index as i32 * ROW_HEIGHT;
            let style = if self.selected == Some(node.id.as_str()) {
                active.add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(Color::White)
            };
            for x in 1..NODE_WIDTH - 1 {
                self.put(buffer, area, x, y, "─", style);
                self.put(buffer, area, x, y + 3, "─", style);
            }
            for offset in 1..3 {
                self.put(buffer, area, 0, y + offset, "│", style);
                self.put(buffer, area, NODE_WIDTH - 1, y + offset, "│", style);
            }
            for (x, offset, symbol) in [
                (0, 0, "┌"),
                (NODE_WIDTH - 1, 0, "┐"),
                (0, 3, "└"),
                (NODE_WIDTH - 1, 3, "┘"),
            ] {
                self.put(buffer, area, x, y + offset, symbol, style);
            }
            self.text(buffer, area, (2, y + 1), &node.id, NODE_WIDTH - 4, style);
            self.text(
                buffer,
                area,
                (2, y + 2),
                &format!("in {} · out {}", node.received, node.outbound),
                NODE_WIDTH - 4,
                normal,
            );
        }
        if self.graph.nodes.is_empty() {
            self.text(buffer, area, (0, 0), "No nodes in this graph", 80, normal);
        }
    }
}

impl GraphCanvas<'_> {
    fn route(&self, buffer: &mut Buffer, area: Rect, x: i32, y: i32, symbol: &str, style: Style) {
        let bits = |symbol: &str| match symbol {
            "─" => 3,
            "│" => 12,
            "┌" => 10,
            "┐" => 9,
            "└" => 6,
            "┘" => 5,
            "┬" => 11,
            "┴" => 7,
            "├" => 14,
            "┤" => 13,
            "┼" => 15,
            _ => 0,
        };
        let (screen_x, screen_y) = (x - self.pan_x, y - self.pan_y);
        if screen_x < 0
            || screen_y < 0
            || screen_x >= i32::from(area.width)
            || screen_y >= i32::from(area.height)
        {
            return;
        }
        let old = buffer[(area.x + screen_x as u16, area.y + screen_y as u16)].symbol();
        let merged = match bits(old) | bits(symbol) {
            3 => "─",
            12 => "│",
            10 => "┌",
            9 => "┐",
            6 => "└",
            5 => "┘",
            11 => "┬",
            7 => "┴",
            14 => "├",
            13 => "┤",
            15 => "┼",
            _ => symbol,
        };
        self.put(buffer, area, x, y, merged, style);
    }

    fn put(&self, buffer: &mut Buffer, area: Rect, x: i32, y: i32, symbol: &str, style: Style) {
        let (x, y) = (x - self.pan_x, y - self.pan_y);
        if x >= 0 && y >= 0 && x < i32::from(area.width) && y < i32::from(area.height) {
            buffer[(area.x + x as u16, area.y + y as u16)]
                .set_symbol(symbol)
                .set_style(style);
        }
    }

    fn text(
        &self,
        buffer: &mut Buffer,
        area: Rect,
        (x, y): (i32, i32),
        text: &str,
        limit: i32,
        style: Style,
    ) {
        // Ratatui performs Unicode cell-width clipping. Pan is applied before writing.
        let (screen_x, screen_y) = (x - self.pan_x, y - self.pan_y);
        if screen_x < 0
            || screen_y < 0
            || screen_x >= i32::from(area.width)
            || screen_y >= i32::from(area.height)
        {
            return;
        }
        let width = limit.min(i32::from(area.width) - screen_x) as usize;
        buffer.set_stringn(
            area.x + screen_x as u16,
            area.y + screen_y as u16,
            text,
            width,
            style,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn graph() -> GraphView {
        GraphView::from_snapshot(&json!({"graph":{"nodes":[{"id":"β"},{"id":"a"}],"edges":[
            {"id":"loop","source":"a","target":"a"},
            {"id":"first","source":"a","target":"β"},
            {"id":"second","source":"a","target":"β"},
            {"id":"return","source":"β","target":"a"}]}}))
        .unwrap()
    }

    #[test]
    fn preserves_parallel_edges_cycles_and_identity_when_order_changes() {
        let graph = graph();
        assert_eq!(graph.edges.len(), 4);
        assert_eq!(graph.incident_edges("a").len(), 4);
        assert_eq!(graph.selected_after_refresh(Some("β")), Some("β".into()));
        assert_eq!(graph.navigate(Some("β"), 1), Some("a".into()));
        assert_eq!(
            graph.selected_after_refresh(Some("removed")),
            Some("a".into())
        );
    }

    #[test]
    fn resize_and_pan_never_write_outside_canvas() {
        let graph = graph();
        for width in 0..45 {
            for pan in [0, 4, 80] {
                let bounds = Rect::new(0, 0, 50, 18);
                let mut buffer = Buffer::empty(bounds);
                let area = Rect::new(3, 2, width, 12);
                GraphCanvas {
                    graph: &graph,
                    selected: Some("a"),
                    pan_x: pan,
                    pan_y: pan,
                }
                .render(area, &mut buffer);
                assert_eq!(buffer[(0, 0)].symbol(), " ");
                assert_eq!(buffer[(49, 17)].symbol(), " ");
            }
        }
    }

    #[test]
    fn snapshot_counts_accept_wide_wire_integers() {
        let graph=GraphView::from_snapshot(&json!({"graph":{"nodes":[{"id":"a"}],"edges":[]},"frontier":{"counts":{"a":{"received":"9007199254740993","outbound":2}}}})).unwrap();
        assert_eq!(graph.nodes[0].received, 9_007_199_254_740_993);
    }

    #[test]
    fn distinct_parallel_lanes_and_self_loop_connectors_remain_visible() {
        let graph = graph();
        let area = Rect::new(0, 0, 45, 12);
        let mut buffer = Buffer::empty(area);
        GraphCanvas {
            graph: &graph,
            selected: Some("a"),
            pan_x: 0,
            pan_y: 0,
        }
        .render(area, &mut buffer);
        assert_eq!(buffer[(29, 4)].symbol(), "│");
        assert_eq!(buffer[(38, 4)].symbol(), "│");
        assert_eq!(buffer[(26, 2)].symbol(), "◀");
        assert!(matches!(buffer[(32, 1)].symbol(), "┬" | "┼"));
    }
}
