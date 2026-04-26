use anyhow::{bail, Context, Result};
use clap::Subcommand;
use tabled::Tabled;

use rai_db::{AuditEvent, AuditEventId, AuditFilter, StorageProvider};

use crate::output::print_table;

#[derive(Subcommand)]
pub enum AuditAction {
    /// View recent audit log events
    Log {
        /// Maximum number of events to show
        #[arg(long, default_value_t = 50)]
        limit: usize,
        /// Filter by entity type: commodity, account, transaction, price, balance_assertion
        #[arg(long)]
        entity_type: Option<String>,
        /// Filter by entity ID
        #[arg(long)]
        entity_id: Option<i64>,
    },
    /// Show full audit event details and snapshots
    Show {
        /// Audit event ID
        id: i64,
    },
    /// Undo the most recent undoable mutation
    Undo {
        /// Number of mutation events to undo
        #[arg(long, default_value_t = 1)]
        steps: usize,
    },
    /// Redo the most recently undone mutation
    Redo {
        /// Number of mutation events to redo
        #[arg(long, default_value_t = 1)]
        steps: usize,
    },
}

#[derive(Tabled)]
struct AuditRow {
    #[tabled(rename = "ID")]
    id: i64,
    #[tabled(rename = "Time")]
    created_at: String,
    #[tabled(rename = "Kind")]
    kind: String,
    #[tabled(rename = "Operation")]
    operation: String,
    #[tabled(rename = "Entity")]
    entity: String,
    #[tabled(rename = "Summary")]
    summary: String,
}

pub fn handle(action: AuditAction, provider: &mut dyn StorageProvider) -> Result<()> {
    match action {
        AuditAction::Log {
            limit,
            entity_type,
            entity_id,
        } => log(provider, limit, entity_type, entity_id),
        AuditAction::Show { id } => show(provider, id),
        AuditAction::Undo { steps } => undo(provider, steps),
        AuditAction::Redo { steps } => redo(provider, steps),
    }
}

fn log(
    provider: &mut dyn StorageProvider,
    limit: usize,
    entity_type: Option<String>,
    entity_id: Option<i64>,
) -> Result<()> {
    let events = provider
        .list_audit_events(&AuditFilter {
            entity_type,
            entity_id,
            limit: Some(limit),
        })
        .context("Failed to list audit events")?;

    let rows: Vec<AuditRow> = events
        .into_iter()
        .map(|event| {
            let entity = format_entity(&event);
            AuditRow {
                id: event.id.0,
                created_at: event.created_at,
                kind: event.kind.as_str().to_string(),
                operation: event.operation,
                entity,
                summary: event.summary,
            }
        })
        .collect();

    print_table(&rows);
    Ok(())
}

fn show(provider: &mut dyn StorageProvider, id: i64) -> Result<()> {
    let event = provider
        .get_audit_event(AuditEventId(id))
        .context("Failed to look up audit event")?;
    let event = match event {
        Some(event) => event,
        None => bail!("Audit event {} not found", id),
    };

    println!("ID:        {}", event.id);
    println!("Created:   {}", event.created_at);
    println!("Kind:      {}", event.kind.as_str());
    println!("Operation: {}", event.operation);
    println!("Entity:    {}", format_entity(&event));
    println!(
        "Target:    {}",
        event
            .target_event_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    println!("Summary:   {}", event.summary);
    println!();
    println!("Before:");
    println!("{}", format_json_snapshot(event.before_json.as_deref()));
    println!();
    println!("After:");
    println!("{}", format_json_snapshot(event.after_json.as_deref()));
    Ok(())
}

fn undo(provider: &mut dyn StorageProvider, steps: usize) -> Result<()> {
    if steps == 0 {
        bail!("--steps must be greater than zero");
    }

    for _ in 0..steps {
        let event = provider
            .undo_last_audit_event()
            .context("Failed to undo audit event")?;
        let target = event
            .target_event_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| "?".to_string());
        println!(
            "Undid audit event {} (recorded undo as {})",
            target, event.id
        );
    }
    Ok(())
}

fn redo(provider: &mut dyn StorageProvider, steps: usize) -> Result<()> {
    if steps == 0 {
        bail!("--steps must be greater than zero");
    }

    for _ in 0..steps {
        let event = provider
            .redo_last_audit_event()
            .context("Failed to redo audit event")?;
        let target = event
            .target_event_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| "?".to_string());
        println!(
            "Redid audit event {} (recorded redo as {})",
            target, event.id
        );
    }
    Ok(())
}

fn format_entity(event: &AuditEvent) -> String {
    match event.entity_id {
        Some(id) => format!("{}#{}", event.entity_type, id),
        None => event.entity_type.clone(),
    }
}

fn format_json_snapshot(snapshot: Option<&str>) -> String {
    match snapshot {
        Some(json) => serde_json::from_str::<serde_json::Value>(json)
            .ok()
            .and_then(|value| serde_json::to_string_pretty(&value).ok())
            .unwrap_or_else(|| json.to_string()),
        None => "-".to_string(),
    }
}
