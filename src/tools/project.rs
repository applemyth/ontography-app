//! Existing core project/native declaration adapters, with trusted registered providers.

use crate::application::{ApplicationDeclaration, ApplicationFormat};
use crate::declarations::RewriteProductionDeclaration;
use crate::{AppError, Result, catalog::Operation, state::Service, views};
use serde_json::{Value, json};
use std::path::Path;

#[cfg(test)]
#[path = "project_tests.rs"]
mod tests;

pub fn operations() -> Vec<Operation> {
    let text = json!({"type":"string"});
    let rewrites = json!({"type":"array","items":{"type":"object"}});
    vec![
        Operation::new(
            "project.providers",
            "List installed trusted provider and native implementation descriptors.",
            json!({}),
            &[],
            false,
        ),
        Operation::new(
            "project.describe",
            "Ask registered core providers to describe a concise project without launching it.",
            json!({"document":text,"project":text}),
            &["document", "project"],
            false,
        ),
        Operation::new(
            "project.prepare",
            "Resolve and validate a concise project and optional rewrite grammar through core; return descriptions, expanded declaration, and admitted graph without launching workers.",
            json!({"document":text,"project":text,"rewrites":rewrites}),
            &["document", "project"],
            false,
        ),
        Operation::new(
            "project.start",
            "Start a persistent native or provider-composed core application with pinned source, registry versions, and resolution; send initial input only on this fresh run.",
            json!({"format":{"type":"string","enum":["native","project"]},"document":text,"project":text,"rewrites":rewrites,"input":{"oneOf":[{"type":"string"},{"type":"array","items":{"type":"integer","minimum":0,"maximum":255}}]}}),
            &["format", "document", "project", "input"],
            true,
        ),
        Operation::new(
            "project.native_validate",
            "Build an expanded native application and optional rewrite grammar through the trusted core registry without launching workers.",
            json!({"document":text,"rewrites":rewrites}),
            &["document"],
            false,
        ),
    ]
}

pub async fn dispatch(service: &Service, operation: &str, args: &Value) -> Result<Value> {
    if operation == "project.providers" {
        return Ok(service.registry.catalog());
    }
    let document = views::field(args, "document")?;
    if operation == "project.native_validate" {
        let (application, rewrites) =
            with_grammar(service.registry.native_application(document)?, args)?;
        return Ok(
            json!({"valid":true,"graph":views::graph(application.kernel()),"rewrite_grammar":rewrites,"launched":false}),
        );
    }
    let project = Path::new(views::field(args, "project")?);
    if !project.is_absolute() {
        return Err(AppError::invalid("project must be an absolute directory"));
    }
    match operation {
        "project.start" => {
            let project = project.canonicalize()?;
            let format: ApplicationFormat = serde_json::from_value(
                args.get("format")
                    .cloned()
                    .ok_or_else(|| AppError::invalid("format is required"))?,
            )?;
            let rewrites =
                serde_json::from_value(args.get("rewrites").cloned().unwrap_or_else(|| json!([])))?;
            let declaration = ApplicationDeclaration::new(
                format,
                document.into(),
                rewrites,
                &service.registry,
                &project,
            )?;
            let input = views::payload(
                args.get("input")
                    .ok_or_else(|| AppError::invalid("input is required"))?,
            )?;
            service.start_application(declaration, project, input).await
        }
        "project.describe" => {
            Ok(json!({"components":service.registry.describe_project(document,project)?}))
        }
        "project.prepare" => {
            let prepared = service.registry.prepare_project(document, project)?;
            let (application, rewrites) = with_grammar(prepared.application, args)?;
            Ok(
                json!({"valid":true,"launched":false,"graph":views::graph(application.kernel()),"rewrite_grammar":rewrites,"application_json":prepared.application_json,"components":prepared.components,"component_specs":prepared.component_specs,"project":prepared.project_root,"registry":service.registry.catalog()}),
            )
        }
        _ => Err(AppError::new("unknown_operation", operation)),
    }
}

fn with_grammar(
    application: ontography::Application,
    args: &Value,
) -> Result<(ontography::Application, Vec<RewriteProductionDeclaration>)> {
    let declarations: Vec<RewriteProductionDeclaration> =
        serde_json::from_value(args.get("rewrites").cloned().unwrap_or_else(|| json!([])))?;
    let kernel = application.kernel();
    let productions = declarations
        .iter()
        .map(|declaration| {
            declaration
                .compile(kernel.id(), kernel.schema(), kernel.contracts())
                .map_err(AppError::core)
        })
        .collect::<Result<Vec<_>>>()?;
    let grammar = ontography::RewriteGrammar::new(productions).map_err(AppError::core)?;
    Ok((application.with_grammar(grammar), declarations))
}
