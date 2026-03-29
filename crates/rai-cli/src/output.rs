use rust_decimal::Decimal;
use tabled::settings::Style;
use tabled::{Table, Tabled};

/// Print a table from a vector of Tabled rows.
pub fn print_table<T: Tabled>(rows: &[T]) {
    if rows.is_empty() {
        println!("(no results)");
        return;
    }
    let mut table = Table::new(rows);
    table.with(Style::rounded());
    println!("{table}");
}

/// Print a table from raw headers and rows (for query results, reports, etc.).
pub fn print_raw_table(headers: &[String], rows: &[Vec<String>]) {
    if rows.is_empty() {
        println!("(no results)");
        return;
    }

    use tabled::builder::Builder;

    let mut builder = Builder::default();
    builder.push_record(headers.iter().map(|h| h.as_str()));
    for row in rows {
        builder.push_record(row.iter().map(|c| c.as_str()));
    }
    let mut table = builder.build();
    table.with(Style::rounded());
    println!("{table}");
}

/// Format an amount with its commodity name.
#[allow(dead_code)]
pub fn format_amount(value: &Decimal, commodity_name: &str) -> String {
    format!("{} {}", value, commodity_name)
}

/// Render a sparkline from a series of values.
///
/// Uses Unicode block characters to represent relative magnitudes:
/// ▁▂▃▄▅▆▇█ (maps min -> ▁, max -> █).
/// Returns an empty string for empty input.
pub fn sparkline(values: &[f64]) -> String {
    if values.is_empty() {
        return String::new();
    }

    const CHARS: [char; 8] = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];

    let min = values.iter().cloned().fold(f64::INFINITY, f64::min);
    let max = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let range = max - min;

    values
        .iter()
        .map(|&v| {
            if range == 0.0 {
                CHARS[3] // middle char when all values are equal
            } else {
                let normalized = (v - min) / range; // 0.0 .. 1.0
                let idx = ((normalized * 7.0).round() as usize).min(7);
                CHARS[idx]
            }
        })
        .collect()
}

/// Render a single horizontal bar chart line.
///
/// Uses `█` for the filled portion and `░` for the empty portion.
/// The `label` is left-aligned, and the numeric value is shown after the bar.
pub fn bar_chart_line(label: &str, value: f64, max_value: f64, width: usize) -> String {
    let filled = if max_value > 0.0 {
        ((value / max_value) * width as f64).round() as usize
    } else {
        0
    };
    let filled = filled.min(width);
    let empty = width - filled;

    let bar: String = "█".repeat(filled);
    let rest: String = "░".repeat(empty);

    format!("{:<30} {:>12.2} │{}{}│", label, value, bar, rest)
}

/// Render a full bar chart from labels and values.
pub fn render_bar_chart(items: &[(String, f64)], width: usize) -> String {
    if items.is_empty() {
        return "(no data)".to_string();
    }

    let max_value = items
        .iter()
        .map(|(_, v)| *v)
        .fold(f64::NEG_INFINITY, f64::max)
        .max(0.0);

    items
        .iter()
        .map(|(label, value)| bar_chart_line(label, *value, max_value, width))
        .collect::<Vec<_>>()
        .join("\n")
}
