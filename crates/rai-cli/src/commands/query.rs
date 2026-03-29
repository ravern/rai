use anyhow::{Context, Result};

use rai_db::{QueryValue, StorageProvider};

use crate::output::print_raw_table;

pub fn handle(sql: Option<String>, provider: &mut dyn StorageProvider) -> Result<()> {
    match sql {
        Some(query) => execute_query(provider, &query),
        None => repl(provider),
    }
}

fn execute_query(provider: &dyn StorageProvider, sql: &str) -> Result<()> {
    let result = provider
        .query_raw(sql)
        .context("Query execution failed")?;

    let rows: Vec<Vec<String>> = result
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|val| match val {
                    QueryValue::Null => "NULL".to_string(),
                    QueryValue::Integer(i) => i.to_string(),
                    QueryValue::Real(f) => f.to_string(),
                    QueryValue::Text(s) => s.clone(),
                })
                .collect()
        })
        .collect();

    print_raw_table(&result.columns, &rows);
    Ok(())
}

fn repl(provider: &mut dyn StorageProvider) -> Result<()> {
    let mut rl = rustyline::DefaultEditor::new().context("Failed to initialize line editor")?;

    println!("rai SQL REPL. Type .quit or Ctrl-D to exit.");

    loop {
        let readline = rl.readline("rai> ");
        match readline {
            Ok(line) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                if trimmed == ".quit" {
                    break;
                }

                let _ = rl.add_history_entry(trimmed);

                match execute_query(provider, trimmed) {
                    Ok(()) => {}
                    Err(e) => {
                        eprintln!("Error: {:#}", e);
                    }
                }
            }
            Err(rustyline::error::ReadlineError::Interrupted) => {
                continue;
            }
            Err(rustyline::error::ReadlineError::Eof) => {
                break;
            }
            Err(err) => {
                eprintln!("Error: {}", err);
                break;
            }
        }
    }

    Ok(())
}
