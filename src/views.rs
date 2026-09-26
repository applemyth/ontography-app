use crate::{AppError, Result};
use ontography::{
    ActivationId, Authority, AuthorityTag, ContentDigest, Kernel, Package, PackageId, Payload,
    Phase, ProposalDecision, Retirement, RetirementReason, SessionStatus, State,
};
use serde_json::{Value, json};
use std::sync::Arc;

pub fn status(status: SessionStatus) -> &'static str {
    match status {
        SessionStatus::Open => "open",
        SessionStatus::Closed => "closed",
        SessionStatus::Faulted => "faulted",
    }
}

pub fn graph(kernel: &Kernel) -> Value {
    json!({
        "nodes": kernel.graph().nodes().iter().map(|n| json!({"id":n.id()})).collect::<Vec<_>>(),
        "edges": kernel.graph().edges().iter().map(|e| json!({"id":e.id(),"source":e.source(),"target":e.target()})).collect::<Vec<_>>()
    })
}

pub fn package(id: PackageId, value: &Package) -> Value {
    json!({"package_id":id.to_string(),"producer":id.producer().to_string(),"edge_id":value.edge_id(),"node_id":value.node_id(),"object_type":value.object_type(),"authority":value.authority().tags().map(|t|t.id()).collect::<Vec<_>>(),"content_digest":value.content_digest().to_string()})
}

pub fn phase(value: Phase) -> &'static str {
    match value {
        Phase::In => "received",
        Phase::Out => "outbound",
    }
}

pub fn retirement_reason(value: RetirementReason) -> &'static str {
    match value {
        RetirementReason::HolderRemoved => "holder_removed",
        RetirementReason::NoAcceptingEdge => "no_accepting_edge",
        RetirementReason::RouteRemoved => "route_removed",
        RetirementReason::Explicit => "explicit",
    }
}

pub fn retirement(value: &Retirement) -> Value {
    json!({"reason":retirement_reason(value.reason()),"holder":value.holder(),
        "phase":phase(value.phase()),"revision":value.revision().to_string(),
        "evidence_activation_id":value.evidence().map(|id|id.to_string())})
}

/// Report current custody and historical disposition from one consistent snapshot.
pub fn package_state(state: &State, id: PackageId, value: &Package) -> Result<Value> {
    let position = state.position(id);
    let consumer = state.package_consumer(id);
    let retired = state.retirement(id);
    let disposition = match (position.is_some(), consumer.is_some(), retired.is_some()) {
        (true, false, false) => "live",
        (false, true, false) => "consumed",
        (false, false, true) => "retired",
        _ => {
            return Err(AppError::new(
                "invalid_state",
                "package disposition is inconsistent",
            ));
        }
    };
    Ok(
        json!({"package":package(id,value),"disposition":disposition,
        "consumer":consumer.map(|id|id.to_string()),
        "position":position.map(|p|json!({"holder":p.holder(),"phase":phase(p.phase())})),
        "delivery":state.deliveries().get(&id).map(|d|json!({"edge_id":d.edge_id(),"source":d.source(),"receiver":d.receiver()})),
        "retirement":retired.map(retirement)}),
    )
}

pub fn decision(value: ProposalDecision) -> Result<Value> {
    match value {
        ProposalDecision::Committed(id) => {
            Ok(json!({"activation_id":id.to_string(),"decision":"committed"}))
        }
        ProposalDecision::Rejected(reason) => Err(AppError::new("rejected", reason.to_string())),
    }
}

pub fn activation_id(value: &str) -> Result<ActivationId> {
    let value = uuid::Uuid::parse_str(value)
        .map_err(|_| AppError::invalid("activation_id must be a UUID"))?;
    Ok(ActivationId::from_u128(value.as_u128()))
}

pub fn package_id(value: &str) -> Result<PackageId> {
    let (producer, output) = value
        .split_once('/')
        .ok_or_else(|| AppError::invalid("package_id must be producer-UUID/output-UUID"))?;
    let output = uuid::Uuid::parse_str(output)
        .map_err(|_| AppError::invalid("invalid package output UUID"))?;
    Ok(PackageId::from_parts(
        activation_id(producer)?,
        output.as_u128(),
    ))
}

pub fn digest(value: &str) -> Result<ContentDigest> {
    if value.len() != 64 {
        return Err(AppError::invalid("content digest must have 64 hex digits"));
    }
    let mut bytes = [0u8; 32];
    for (i, part) in value.as_bytes().chunks_exact(2).enumerate() {
        let s = std::str::from_utf8(part).map_err(AppError::core)?;
        bytes[i] =
            u8::from_str_radix(s, 16).map_err(|_| AppError::invalid("invalid content digest"))?;
    }
    Ok(ContentDigest::from_bytes(bytes))
}

pub fn authority(tags: &[String]) -> Result<Authority> {
    Ok(Authority::new(
        tags.iter()
            .map(|tag| AuthorityTag::new(tag.as_str()))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(AppError::core)?,
    ))
}

/// The wire payload is explicit; arbitrary JSON objects cannot silently turn into messages.
pub fn payload(value: &Value) -> Result<Payload> {
    if let Some(text) = value.as_str() {
        return Ok(Arc::from(text.as_bytes()));
    }
    if let Some(bytes) = value.as_array() {
        let bytes = bytes
            .iter()
            .map(|v| {
                v.as_u64()
                    .filter(|v| *v <= 255)
                    .map(|v| v as u8)
                    .ok_or_else(|| AppError::invalid("payload bytes must be integers 0..255"))
            })
            .collect::<Result<Vec<_>>>()?;
        return Ok(Arc::from(bytes));
    }
    Err(AppError::invalid(
        "payload must be UTF-8 text or an array of bytes",
    ))
}

pub fn bytes(value: &[u8]) -> Value {
    match std::str::from_utf8(value) {
        Ok(text) => json!({"text":text,"length":value.len()}),
        Err(_) => json!({"bytes":value,"length":value.len()}),
    }
}

pub fn field<'a>(args: &'a Value, name: &str) -> Result<&'a str> {
    args.get(name)
        .and_then(Value::as_str)
        .ok_or_else(|| AppError::invalid(format!("{name} must be a string")))
}

pub fn integer(args: &Value, name: &str, default: u64) -> Result<u64> {
    match args.get(name) {
        None => Ok(default),
        Some(Value::String(s)) => s
            .parse()
            .map_err(|_| AppError::invalid(format!("{name} must be an unsigned integer"))),
        Some(v) => v
            .as_u64()
            .ok_or_else(|| AppError::invalid(format!("{name} must be an unsigned integer"))),
    }
}

pub fn limit(args: &Value) -> Result<usize> {
    let n = integer(args, "limit", 100)?;
    if n == 0 || n > 1000 {
        return Err(AppError::invalid("limit must be between 1 and 1000"));
    }
    Ok(n as usize)
}
