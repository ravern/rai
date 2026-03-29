use std::collections::HashMap;
use std::fs;
use std::io::Write;

use anyhow::{bail, Context, Result};
use chrono::NaiveDate;
use clap::{Subcommand, ValueEnum};
use rust_decimal::Decimal;

use rai_core::types::*;
use rai_db::StorageProvider;

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Clone, ValueEnum)]
pub enum Format {
    Rai,
    Beancount,
}

#[derive(Subcommand)]
pub enum FileAction {
    /// Export all data to a human-readable file
    Export {
        /// Output file path (prints to stdout if omitted)
        #[arg(short, long)]
        output: Option<String>,
        /// File format (default: rai)
        #[arg(short, long, default_value = "rai")]
        format: Format,
    },
    /// Import data from a file into the current profile
    Import {
        /// Path to the file
        path: String,
        /// File format (default: auto-detect from extension, falls back to rai)
        #[arg(short, long)]
        format: Option<Format>,
    },
}

pub fn handle(action: FileAction, provider: &mut dyn StorageProvider) -> Result<()> {
    match action {
        FileAction::Export { output, format } => export(provider, output.as_deref(), &format),
        FileAction::Import { path, format } => {
            let fmt = format.unwrap_or_else(|| detect_format(&path));
            import(provider, &path, &fmt)
        }
    }
}

fn detect_format(path: &str) -> Format {
    if path.ends_with(".beancount") || path.ends_with(".bean") || path.ends_with(".bc") {
        Format::Beancount
    } else {
        Format::Rai
    }
}

// ---------------------------------------------------------------------------
// Shared types
// ---------------------------------------------------------------------------

#[derive(Debug)]
enum Directive {
    Commodity {
        name: String,
        precision: u8,
        metadata: Metadata,
    },
    Open {
        date: NaiveDate,
        account: String,
        currencies: Vec<String>,
        booking_method: BookingMethod,
        metadata: Metadata,
    },
    Close {
        date: NaiveDate,
        account: String,
    },
    Transaction {
        date: NaiveDate,
        time: Option<chrono::NaiveTime>,
        status: TransactionStatus,
        payee: Option<String>,
        narration: Option<String>,
        tags: Vec<String>,
        links: Vec<String>,
        postings: Vec<ParsedPosting>,
        metadata: Metadata,
    },
    Price {
        date: NaiveDate,
        commodity: String,
        value: Decimal,
        target_commodity: String,
    },
    Balance {
        date: NaiveDate,
        account: String,
        amount: Decimal,
        commodity: String,
    },
}

#[derive(Debug)]
struct ParsedPosting {
    account: String,
    amount: Decimal,
    commodity: String,
    cost: Option<ParsedCost>,
    price: Option<ParsedPrice>,
    metadata: Metadata,
}

#[derive(Debug)]
struct ParsedCost {
    amount: Decimal,
    commodity: String,
    date: NaiveDate,
    label: Option<String>,
}

#[derive(Debug)]
struct ParsedPrice {
    amount: Decimal,
    commodity: String,
}

#[derive(Default)]
struct ImportCounts {
    commodities: usize,
    accounts: usize,
    transactions: usize,
    prices: usize,
    assertions: usize,
}

// ---------------------------------------------------------------------------
// Load all data from provider (shared by both exporters)
// ---------------------------------------------------------------------------

struct LedgerData {
    commodities: Vec<Commodity>,
    accounts: Vec<Account>,
    transactions: Vec<Transaction>,
    prices: Vec<Price>,
    assertions: Vec<BalanceAssertion>,
    commodity_name: HashMap<CommodityId, String>,
    account_name: HashMap<AccountId, String>,
}

fn load_all(provider: &mut dyn StorageProvider) -> Result<LedgerData> {
    let commodities = provider
        .list_commodities()
        .context("Failed to list commodities")?;
    let accounts = provider
        .list_accounts(&AccountFilter::default())
        .context("Failed to list accounts")?;
    let transactions = provider
        .list_transactions(&TransactionFilter::default())
        .context("Failed to list transactions")?;
    let prices = provider
        .list_prices(&PriceFilter::default())
        .context("Failed to list prices")?;
    let assertions = provider
        .list_balance_assertions(&BalanceAssertionFilter::default())
        .context("Failed to list balance assertions")?;

    let commodity_name: HashMap<CommodityId, String> =
        commodities.iter().map(|c| (c.id, c.name.clone())).collect();
    let account_name: HashMap<AccountId, String> =
        accounts.iter().map(|a| (a.id, a.name.clone())).collect();

    Ok(LedgerData {
        commodities,
        accounts,
        transactions,
        prices,
        assertions,
        commodity_name,
        account_name,
    })
}

impl LedgerData {
    fn cn(&self, id: CommodityId) -> &str {
        self.commodity_name.get(&id).map(|s| s.as_str()).unwrap_or("???")
    }
    fn an(&self, id: AccountId) -> &str {
        self.account_name.get(&id).map(|s| s.as_str()).unwrap_or("???")
    }
}

// ---------------------------------------------------------------------------
// Export
// ---------------------------------------------------------------------------

fn export(provider: &mut dyn StorageProvider, output: Option<&str>, format: &Format) -> Result<()> {
    let data = load_all(provider)?;
    let content = match format {
        Format::Rai => generate_rai(&data),
        Format::Beancount => generate_beancount(&data),
    };

    match output {
        Some(path) => {
            fs::write(path, &content)
                .with_context(|| format!("Failed to write to '{}'", path))?;
            println!("Exported to {}", path);
        }
        None => {
            std::io::stdout()
                .write_all(content.as_bytes())
                .context("Failed to write to stdout")?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Import (shared directive applier)
// ---------------------------------------------------------------------------

fn import(provider: &mut dyn StorageProvider, path: &str, format: &Format) -> Result<()> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("Failed to read '{}'", path))?;

    let directives = match format {
        Format::Rai => parse_rai(&content)?,
        Format::Beancount => parse_beancount(&content)?,
    };

    let counts = apply_directives(provider, &directives)?;

    println!(
        "Imported from {}: {} commodities, {} accounts, {} transactions, {} prices, {} balance assertions",
        path, counts.commodities, counts.accounts, counts.transactions, counts.prices, counts.assertions
    );
    Ok(())
}

fn apply_directives(
    provider: &mut dyn StorageProvider,
    directives: &[Directive],
) -> Result<ImportCounts> {
    let mut commodity_ids: HashMap<String, CommodityId> = HashMap::new();
    let mut account_ids: HashMap<String, AccountId> = HashMap::new();

    // Pre-populate maps with existing data so imports can be additive
    for c in provider.list_commodities().context("Failed to list commodities")? {
        commodity_ids.insert(c.name.clone(), c.id);
    }
    for a in provider
        .list_accounts(&AccountFilter::default())
        .context("Failed to list accounts")?
    {
        account_ids.insert(a.name.clone(), a.id);
    }

    let mut counts = ImportCounts::default();

    for directive in directives {
        match directive {
            Directive::Commodity {
                name,
                precision,
                metadata,
            } => {
                if commodity_ids.contains_key(name) {
                    continue;
                }
                let new = NewCommodity {
                    name: name.clone(),
                    precision: *precision,
                    metadata: metadata.clone(),
                };
                let c = provider
                    .create_commodity(&new)
                    .with_context(|| format!("Failed to create commodity '{}'", name))?;
                commodity_ids.insert(name.clone(), c.id);
                counts.commodities += 1;
            }
            Directive::Open {
                date,
                account,
                currencies,
                booking_method,
                metadata,
            } => {
                if account_ids.contains_key(account) {
                    continue;
                }
                let currency_ids: Vec<CommodityId> = currencies
                    .iter()
                    .map(|name| {
                        commodity_ids
                            .get(name)
                            .copied()
                            .with_context(|| {
                                format!(
                                    "Commodity '{}' not defined before account '{}'",
                                    name, account
                                )
                            })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let new = NewAccount {
                    name: account.clone(),
                    opened_at: *date,
                    currencies: currency_ids,
                    booking_method: *booking_method,
                    metadata: metadata.clone(),
                };
                let a = provider
                    .create_account(&new)
                    .with_context(|| format!("Failed to create account '{}'", account))?;
                account_ids.insert(account.clone(), a.id);
                counts.accounts += 1;
            }
            Directive::Close { date, account } => {
                let id = account_ids
                    .get(account)
                    .with_context(|| {
                        format!("Account '{}' not found for close directive", account)
                    })?;
                provider
                    .close_account(*id, *date)
                    .with_context(|| format!("Failed to close account '{}'", account))?;
            }
            Directive::Transaction {
                date,
                time,
                status,
                payee,
                narration,
                tags,
                links,
                postings,
                metadata,
            } => {
                let resolved_postings: Vec<NewPosting> = postings
                    .iter()
                    .map(|p| resolve_posting(p, &commodity_ids, &account_ids))
                    .collect::<Result<Vec<_>>>()?;

                let new_tx = NewTransaction {
                    date: *date,
                    time: *time,
                    status: *status,
                    payee: payee.clone(),
                    narration: narration.clone(),
                    tags: tags.clone(),
                    links: links.clone(),
                    postings: resolved_postings,
                    metadata: metadata.clone(),
                };
                provider
                    .create_transaction(&new_tx)
                    .context("Failed to create transaction")?;
                counts.transactions += 1;
            }
            Directive::Price {
                date,
                commodity,
                value,
                target_commodity,
            } => {
                let commodity_id = *commodity_ids
                    .get(commodity)
                    .with_context(|| format!("Commodity '{}' not found", commodity))?;
                let target_id = *commodity_ids
                    .get(target_commodity)
                    .with_context(|| format!("Commodity '{}' not found", target_commodity))?;
                let new = NewPrice {
                    date: *date,
                    commodity_id,
                    target_commodity_id: target_id,
                    value: *value,
                };
                provider
                    .create_price(&new)
                    .context("Failed to create price")?;
                counts.prices += 1;
            }
            Directive::Balance {
                date,
                account,
                amount,
                commodity,
            } => {
                let account_id = *account_ids
                    .get(account)
                    .with_context(|| format!("Account '{}' not found", account))?;
                let commodity_id = *commodity_ids
                    .get(commodity)
                    .with_context(|| format!("Commodity '{}' not found", commodity))?;
                let new = NewBalanceAssertion {
                    date: *date,
                    account_id,
                    expected: Amount {
                        value: *amount,
                        commodity_id,
                    },
                };
                provider
                    .create_balance_assertion(&new)
                    .context("Failed to create balance assertion")?;
                counts.assertions += 1;
            }
        }
    }

    Ok(counts)
}

fn resolve_posting(
    p: &ParsedPosting,
    commodity_ids: &HashMap<String, CommodityId>,
    account_ids: &HashMap<String, AccountId>,
) -> Result<NewPosting> {
    let account_id = *account_ids
        .get(&p.account)
        .with_context(|| format!("Account '{}' not found", p.account))?;
    let commodity_id = *commodity_ids
        .get(&p.commodity)
        .with_context(|| format!("Commodity '{}' not found", p.commodity))?;
    let cost = match &p.cost {
        Some(c) => {
            let cost_commodity_id = *commodity_ids
                .get(&c.commodity)
                .with_context(|| format!("Cost commodity '{}' not found", c.commodity))?;
            Some(Cost {
                amount: Amount {
                    value: c.amount,
                    commodity_id: cost_commodity_id,
                },
                date: c.date,
                label: c.label.clone(),
            })
        }
        None => None,
    };
    let price = match &p.price {
        Some(pr) => {
            let price_commodity_id = *commodity_ids
                .get(&pr.commodity)
                .with_context(|| format!("Price commodity '{}' not found", pr.commodity))?;
            Some(Amount {
                value: pr.amount,
                commodity_id: price_commodity_id,
            })
        }
        None => None,
    };
    Ok(NewPosting {
        account_id,
        units: Amount {
            value: p.amount,
            commodity_id,
        },
        cost,
        price,
        metadata: p.metadata.clone(),
    })
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn escape_str(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

fn unescape_str(s: &str) -> String {
    s.replace("\\\"", "\"").replace("\\\\", "\\")
}

fn parse_date(s: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .with_context(|| format!("Invalid date: '{}'", s))
}

fn write_posting(
    out: &mut String,
    posting: &Posting,
    data: &LedgerData,
    meta_prefix: &str,
) {
    let acct = data.an(posting.account_id);
    let amount_str = format!(
        "{} {}",
        posting.units.value,
        data.cn(posting.units.commodity_id)
    );

    let cost_str = match &posting.cost {
        Some(cost) => {
            let label_part = match &cost.label {
                Some(l) => format!(", \"{}\"", escape_str(l)),
                None => String::new(),
            };
            format!(
                " {{{} {}, {}{}}}",
                cost.amount.value,
                data.cn(cost.amount.commodity_id),
                cost.date,
                label_part
            )
        }
        None => String::new(),
    };

    let price_str = match &posting.price {
        Some(price) => format!(" @ {} {}", price.value, data.cn(price.commodity_id)),
        None => String::new(),
    };

    out.push_str(&format!("  {}  {}{}{}\n", acct, amount_str, cost_str, price_str));

    for (key, value) in &posting.metadata {
        let val_str = format_meta_value(value);
        out.push_str(&format!("    {}{}: {}\n", meta_prefix, key, val_str));
    }
}

fn format_meta_value(value: &MetadataValue) -> String {
    match value {
        MetadataValue::String(s) => format!("\"{}\"", escape_str(s)),
        MetadataValue::Number(n) => n.to_string(),
        MetadataValue::Date(d) => d.to_string(),
        MetadataValue::Bool(b) => b.to_string(),
    }
}

fn format_tx_header(tx: &Transaction) -> (String, String, String, String, String) {
    let status_char = match tx.status {
        TransactionStatus::Completed => "*",
        TransactionStatus::Pending => "!",
        TransactionStatus::Flagged => "#",
    };

    let time_part = match &tx.time {
        Some(t) => format!(" {}", t.format("%H:%M:%S")),
        None => String::new(),
    };

    let payee_part = match &tx.payee {
        Some(p) => format!(" \"{}\"", escape_str(p)),
        None => String::new(),
    };

    let narration_part = match &tx.narration {
        Some(n) => format!(" \"{}\"", escape_str(n)),
        None => String::new(),
    };

    let tags_links: String = tx
        .tags
        .iter()
        .map(|t| format!(" #{}", t))
        .chain(tx.links.iter().map(|l| format!(" ^{}", l)))
        .collect();

    (
        status_char.to_string(),
        time_part,
        payee_part,
        narration_part,
        tags_links,
    )
}

// ---------------------------------------------------------------------------
// Shared parsing helpers
// ---------------------------------------------------------------------------

fn parse_quoted_string(chars: &[char], pos: &mut usize) -> Result<String> {
    if *pos >= chars.len() || chars[*pos] != '"' {
        bail!("Expected quoted string");
    }
    *pos += 1;
    let mut result = String::new();
    while *pos < chars.len() {
        if chars[*pos] == '\\' && *pos + 1 < chars.len() {
            result.push(chars[*pos + 1]);
            *pos += 2;
        } else if chars[*pos] == '"' {
            *pos += 1;
            return Ok(result);
        } else {
            result.push(chars[*pos]);
            *pos += 1;
        }
    }
    bail!("Unterminated quoted string");
}

/// Parse the fields after the status character: quoted payee, quoted narration, #tags, ^links
fn parse_tx_fields(s: &str) -> Result<(Option<String>, Option<String>, Vec<String>, Vec<String>)> {
    let s = s.trim();
    if s.is_empty() {
        return Ok((None, None, Vec::new(), Vec::new()));
    }

    let mut payee = None;
    let mut narration = None;
    let mut tags = Vec::new();
    let mut links = Vec::new();
    let mut pos = 0;
    let chars: Vec<char> = s.chars().collect();

    let mut quoted_count = 0;
    while pos < chars.len() {
        if chars[pos] == ' ' {
            pos += 1;
            continue;
        }
        if chars[pos] == '"' && quoted_count < 2 {
            let content = parse_quoted_string(&chars, &mut pos)?;
            if quoted_count == 0 {
                payee = Some(content);
            } else {
                narration = Some(content);
            }
            quoted_count += 1;
        } else if chars[pos] == '#' {
            pos += 1;
            let start = pos;
            while pos < chars.len() && chars[pos] != ' ' {
                pos += 1;
            }
            tags.push(chars[start..pos].iter().collect());
        } else if chars[pos] == '^' {
            pos += 1;
            let start = pos;
            while pos < chars.len() && chars[pos] != ' ' {
                pos += 1;
            }
            links.push(chars[start..pos].iter().collect());
        } else {
            pos += 1;
        }
    }

    // One quoted string = narration only. Two = payee + narration.
    if quoted_count == 1 {
        narration = payee.take();
    }

    Ok((payee, narration, tags, links))
}

/// Parse a posting line: "Account  100.00 USD {50.00 EUR, 2024-01-01} @ 150.00 EUR"
fn parse_posting_line(line: &str, line_num: usize) -> Result<ParsedPosting> {
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() < 3 {
        bail!(
            "Invalid posting on line {}: expected 'Account amount commodity [...]'",
            line_num
        );
    }

    let account = parts[0].to_string();
    let amount: Decimal = parts[1]
        .parse()
        .with_context(|| format!("Invalid amount '{}' on line {}", parts[1], line_num))?;
    let commodity = parts[2].to_string();

    let mut cost = None;
    let mut price = None;

    let remaining = &parts[3..];
    let remaining_str: String = remaining.join(" ");

    // Parse cost: {...}
    if let Some(brace_start) = remaining_str.find('{') {
        if let Some(brace_end) = remaining_str.find('}') {
            let cost_content = &remaining_str[brace_start + 1..brace_end];
            cost = Some(parse_cost_content(cost_content, line_num)?);
        }
    }

    // Parse price: @ amount commodity
    let price_str = if remaining_str.contains('}') {
        remaining_str
            .split('}')
            .nth(1)
            .unwrap_or("")
            .trim()
            .to_string()
    } else {
        remaining_str.clone()
    };

    if let Some(at_pos) = price_str.find("@ ") {
        let after_at = &price_str[at_pos + 2..];
        let tokens: Vec<&str> = after_at.split_whitespace().collect();
        if tokens.len() >= 2 {
            let price_amount: Decimal = tokens[0]
                .parse()
                .with_context(|| {
                    format!("Invalid price amount '{}' on line {}", tokens[0], line_num)
                })?;
            let price_commodity = tokens[1].to_string();
            price = Some(ParsedPrice {
                amount: price_amount,
                commodity: price_commodity,
            });
        }
    }

    Ok(ParsedPosting {
        account,
        amount,
        commodity,
        cost,
        price,
        metadata: Metadata::new(),
    })
}

/// Parse cost content: "50.00 USD, 2024-01-01" or "50.00 USD, 2024-01-01, \"label\""
fn parse_cost_content(content: &str, line_num: usize) -> Result<ParsedCost> {
    let parts: Vec<&str> = content.split(',').map(|s| s.trim()).collect();
    if parts.is_empty() {
        bail!("Empty cost on line {}", line_num);
    }

    let amount_parts: Vec<&str> = parts[0].split_whitespace().collect();
    if amount_parts.len() < 2 {
        bail!(
            "Invalid cost format on line {}: expected 'amount commodity'",
            line_num
        );
    }
    let amount: Decimal = amount_parts[0]
        .parse()
        .with_context(|| format!("Invalid cost amount '{}' on line {}", amount_parts[0], line_num))?;
    let commodity = amount_parts[1].to_string();

    let date = if parts.len() > 1 {
        parse_date(parts[1].trim())
            .with_context(|| format!("Invalid cost date on line {}", line_num))?
    } else {
        bail!("Cost requires a date on line {}", line_num);
    };

    let label = if parts.len() > 2 {
        let label_str = parts[2].trim();
        if label_str.starts_with('"') && label_str.ends_with('"') && label_str.len() >= 2 {
            Some(unescape_str(&label_str[1..label_str.len() - 1]))
        } else {
            Some(label_str.to_string())
        }
    } else {
        None
    };

    Ok(ParsedCost {
        amount,
        commodity,
        date,
        label,
    })
}

/// Parse a "key: value" metadata pair
fn parse_meta_kv(s: &str) -> Result<Option<(String, MetadataValue)>> {
    let parts: Vec<&str> = s.splitn(2, ": ").collect();
    if parts.len() < 2 {
        return Ok(None);
    }
    let key = parts[0];
    let val_str = parts[1];

    if val_str.is_empty() {
        return Ok(None);
    }

    let value = if val_str.starts_with('"') && val_str.ends_with('"') && val_str.len() >= 2 {
        MetadataValue::String(unescape_str(&val_str[1..val_str.len() - 1]))
    } else if val_str == "true" || val_str == "TRUE" || val_str == "True" {
        MetadataValue::Bool(true)
    } else if val_str == "false" || val_str == "FALSE" || val_str == "False" {
        MetadataValue::Bool(false)
    } else if let Ok(d) = NaiveDate::parse_from_str(val_str, "%Y-%m-%d") {
        MetadataValue::Date(d)
    } else if let Ok(n) = val_str.parse::<Decimal>() {
        MetadataValue::Number(n)
    } else {
        MetadataValue::String(val_str.to_string())
    };

    Ok(Some((key.to_string(), value)))
}

/// Parse the transaction header to extract the optional time.
/// Returns (time, rest_after_status)
fn parse_tx_header_time(
    line: &str,
) -> (Option<chrono::NaiveTime>, &str) {
    // line = "DATE [TIME] STATUS rest..."
    // Find the position after the date
    let after_date = match line.find(' ') {
        Some(pos) => &line[pos + 1..],
        None => return (None, ""),
    };

    // Try parsing the next token as time
    let next_space = after_date.find(' ').unwrap_or(after_date.len());
    let maybe_time = &after_date[..next_space];

    if let Ok(time) = chrono::NaiveTime::parse_from_str(maybe_time, "%H:%M:%S") {
        let rest = if next_space < after_date.len() {
            &after_date[next_space + 1..]
        } else {
            ""
        };
        return (Some(time), rest);
    }
    if let Ok(time) = chrono::NaiveTime::parse_from_str(maybe_time, "%H:%M") {
        let rest = if next_space < after_date.len() {
            &after_date[next_space + 1..]
        } else {
            ""
        };
        return (Some(time), rest);
    }

    (None, after_date)
}

// =====================================================================
// RAI FORMAT
// =====================================================================

fn generate_rai(data: &LedgerData) -> String {
    let mut out = String::new();

    out.push_str("; rai ledger v1\n");

    // Commodities
    if !data.commodities.is_empty() {
        out.push_str("\n; --- Commodities ---\n");
        for c in &data.commodities {
            out.push_str(&format!("\ncommodity {}\n", c.name));
            out.push_str(&format!("  precision: {}\n", c.precision));
            for (key, value) in &c.metadata {
                out.push_str(&format!("  meta {}: {}\n", key, format_meta_value(value)));
            }
        }
    }

    // Accounts
    if !data.accounts.is_empty() {
        out.push_str("\n; --- Accounts ---\n");
        for a in &data.accounts {
            let currencies: Vec<&str> = a.currencies.iter().map(|id| data.cn(*id)).collect();
            let currency_suffix = if currencies.is_empty() {
                String::new()
            } else {
                format!(" {}", currencies.join(", "))
            };
            out.push_str(&format!("\n{} open {}{}\n", a.opened_at, a.name, currency_suffix));
            if a.booking_method != BookingMethod::Strict {
                out.push_str(&format!("  booking-method: {}\n", a.booking_method.as_str()));
            }
            for (key, value) in &a.metadata {
                out.push_str(&format!("  meta {}: {}\n", key, format_meta_value(value)));
            }
        }

        for a in &data.accounts {
            if let Some(closed_at) = a.closed_at {
                out.push_str(&format!("\n{} close {}\n", closed_at, a.name));
            }
        }
    }

    // Transactions
    if !data.transactions.is_empty() {
        out.push_str("\n; --- Transactions ---\n");
        for tx in &data.transactions {
            let (status, time_part, payee_part, narration_part, tags_links) = format_tx_header(tx);
            out.push_str(&format!(
                "\n{}{} {}{}{}{}\n",
                tx.date, time_part, status, payee_part, narration_part, tags_links
            ));
            for (key, value) in &tx.metadata {
                out.push_str(&format!("  meta {}: {}\n", key, format_meta_value(value)));
            }
            for posting in &tx.postings {
                write_posting(&mut out, posting, data, "meta ");
            }
        }
    }

    // Prices
    if !data.prices.is_empty() {
        out.push_str("\n; --- Prices ---\n");
        for p in &data.prices {
            out.push_str(&format!(
                "\n{} price {} {} {}\n",
                p.date,
                data.cn(p.commodity_id),
                p.value,
                data.cn(p.target_commodity_id)
            ));
        }
    }

    // Balance assertions
    if !data.assertions.is_empty() {
        out.push_str("\n; --- Balance Assertions ---\n");
        for a in &data.assertions {
            out.push_str(&format!(
                "\n{} balance {} {} {}\n",
                a.date,
                data.an(a.account_id),
                a.expected.value,
                data.cn(a.expected.commodity_id)
            ));
        }
    }

    out
}

fn parse_rai(content: &str) -> Result<Vec<Directive>> {
    let lines: Vec<&str> = content.lines().collect();
    let mut directives: Vec<Directive> = Vec::new();
    let mut i = 0;

    while i < lines.len() {
        let line = lines[i];
        let trimmed = line.trim();

        if trimmed.is_empty() || trimmed.starts_with(';') {
            i += 1;
            continue;
        }

        if trimmed.starts_with("commodity ") {
            let name = trimmed["commodity ".len()..].trim().to_string();
            i += 1;
            let mut precision: u8 = 0;
            let mut metadata = Metadata::new();
            while i < lines.len() {
                let cont = lines[i];
                if !cont.starts_with("  ") || cont.trim().is_empty() {
                    break;
                }
                let ct = cont.trim();
                if ct.starts_with("; ") {
                    i += 1;
                    continue;
                }
                if let Some(rest) = ct.strip_prefix("precision: ") {
                    precision = rest.trim().parse().with_context(|| {
                        format!("Invalid precision: '{}'", rest)
                    })?;
                } else if let Some(rest) = ct.strip_prefix("meta ") {
                    if let Some((key, val)) = parse_meta_kv(rest)? {
                        metadata.insert(key, val);
                    }
                }
                i += 1;
            }
            directives.push(Directive::Commodity {
                name,
                precision,
                metadata,
            });
        } else if trimmed.starts_with(|c: char| c.is_ascii_digit()) {
            i = parse_date_directive_rai(&lines, i, trimmed, &mut directives)?;
        } else {
            bail!("Unexpected line {}: '{}'", i + 1, trimmed);
        }
    }

    Ok(directives)
}

fn parse_date_directive_rai(
    lines: &[&str],
    mut i: usize,
    trimmed: &str,
    directives: &mut Vec<Directive>,
) -> Result<usize> {
    let parts: Vec<&str> = trimmed.splitn(3, ' ').collect();
    if parts.len() < 2 {
        bail!("Invalid directive on line {}: '{}'", i + 1, trimmed);
    }

    let date = parse_date(parts[0]).with_context(|| format!("Line {}", i + 1))?;

    match parts[1] {
        "open" => {
            let rest = if parts.len() > 2 { parts[2] } else { "" };
            let tokens: Vec<&str> = rest.split_whitespace().collect();
            if tokens.is_empty() {
                bail!("Missing account name in open directive on line {}", i + 1);
            }
            let account = tokens[0].to_string();
            let currencies: Vec<String> = tokens[1..]
                .iter()
                .flat_map(|t| t.split(','))
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();

            i += 1;
            let mut booking_method = BookingMethod::Strict;
            let mut metadata = Metadata::new();
            while i < lines.len() {
                let cont = lines[i];
                if !cont.starts_with("  ") || cont.trim().is_empty() {
                    break;
                }
                let ct = cont.trim();
                if ct.starts_with("; ") {
                    i += 1;
                    continue;
                }
                if let Some(rest) = ct.strip_prefix("booking-method: ") {
                    booking_method = BookingMethod::from_str(rest.trim())
                        .with_context(|| format!("Unknown booking method: '{}'", rest))?;
                } else if let Some(rest) = ct.strip_prefix("meta ") {
                    if let Some((key, val)) = parse_meta_kv(rest)? {
                        metadata.insert(key, val);
                    }
                }
                i += 1;
            }
            directives.push(Directive::Open {
                date,
                account,
                currencies,
                booking_method,
                metadata,
            });
        }
        "close" => {
            let rest = if parts.len() > 2 { parts[2] } else { "" };
            let account = rest.split_whitespace().next().unwrap_or("").to_string();
            if account.is_empty() {
                bail!("Missing account name in close directive on line {}", i + 1);
            }
            directives.push(Directive::Close { date, account });
            i += 1;
        }
        "price" => {
            let rest = if parts.len() > 2 { parts[2] } else { "" };
            let tokens: Vec<&str> = rest.split_whitespace().collect();
            if tokens.len() < 3 {
                bail!(
                    "Price directive requires: price COMMODITY AMOUNT TARGET on line {}",
                    i + 1
                );
            }
            directives.push(Directive::Price {
                date,
                commodity: tokens[0].to_string(),
                value: tokens[1].parse().with_context(|| {
                    format!("Invalid price amount '{}' on line {}", tokens[1], i + 1)
                })?,
                target_commodity: tokens[2].to_string(),
            });
            i += 1;
        }
        "balance" => {
            let rest = if parts.len() > 2 { parts[2] } else { "" };
            let tokens: Vec<&str> = rest.split_whitespace().collect();
            if tokens.len() < 3 {
                bail!(
                    "Balance directive requires: balance ACCOUNT AMOUNT COMMODITY on line {}",
                    i + 1
                );
            }
            directives.push(Directive::Balance {
                date,
                account: tokens[0].to_string(),
                amount: tokens[1].parse().with_context(|| {
                    format!("Invalid amount '{}' on line {}", tokens[1], i + 1)
                })?,
                commodity: tokens[2].to_string(),
            });
            i += 1;
        }
        s if s == "*" || s == "!" || s == "#" => {
            let status = TransactionStatus::from_str(s).unwrap();
            let (time, after_status) = parse_tx_header_time(trimmed);
            // after_status = "STATUS rest..." or "rest..." depending on whether time was consumed
            // We need to get the part after the status char
            let tx_rest = if time.is_some() {
                // after_status = "STATUS rest..."
                let after_s = after_status.find(' ').map(|p| &after_status[p + 1..]).unwrap_or("");
                after_s
            } else {
                // after_status = "STATUS rest..."
                let after_s = after_status.find(' ').map(|p| &after_status[p + 1..]).unwrap_or("");
                after_s
            };
            let (payee, narration, tags, links) = parse_tx_fields(tx_rest)?;
            i += 1;
            let (postings, tx_metadata, new_i) = parse_tx_body_rai(lines, i)?;
            i = new_i;
            directives.push(Directive::Transaction {
                date,
                time,
                status,
                payee,
                narration,
                tags,
                links,
                postings,
                metadata: tx_metadata,
            });
        }
        _ => {
            // Could be "DATE TIME STATUS ..." where parts[1] is a time
            if chrono::NaiveTime::parse_from_str(parts[1], "%H:%M:%S").is_ok()
                || chrono::NaiveTime::parse_from_str(parts[1], "%H:%M").is_ok()
            {
                let (time, after_time) = parse_tx_header_time(trimmed);
                // after_time = "STATUS rest..."
                let status_char = after_time
                    .chars()
                    .next()
                    .with_context(|| format!("Missing status on line {}", i + 1))?;
                let status = TransactionStatus::from_str(&status_char.to_string())
                    .with_context(|| {
                        format!("Unknown status '{}' on line {}", status_char, i + 1)
                    })?;
                let tx_rest = if after_time.len() > 1 {
                    &after_time[2..]
                } else {
                    ""
                };
                let (payee, narration, tags, links) = parse_tx_fields(tx_rest)?;
                i += 1;
                let (postings, tx_metadata, new_i) = parse_tx_body_rai(lines, i)?;
                i = new_i;
                directives.push(Directive::Transaction {
                    date,
                    time,
                    status,
                    payee,
                    narration,
                    tags,
                    links,
                    postings,
                    metadata: tx_metadata,
                });
            } else {
                bail!("Unknown directive '{}' on line {}", parts[1], i + 1);
            }
        }
    }

    Ok(i)
}

fn parse_tx_body_rai(
    lines: &[&str],
    mut i: usize,
) -> Result<(Vec<ParsedPosting>, Metadata, usize)> {
    let mut postings: Vec<ParsedPosting> = Vec::new();
    let mut tx_metadata = Metadata::new();

    while i < lines.len() {
        let cont = lines[i];
        if !cont.starts_with("  ") || cont.trim().is_empty() {
            break;
        }
        let ct = cont.trim();
        if ct.starts_with("; ") {
            i += 1;
            continue;
        }
        if ct.starts_with("meta ") {
            if cont.starts_with("    ") && !postings.is_empty() {
                if let Some(rest) = ct.strip_prefix("meta ") {
                    if let Some((key, val)) = parse_meta_kv(rest)? {
                        postings.last_mut().unwrap().metadata.insert(key, val);
                    }
                }
            } else if let Some(rest) = ct.strip_prefix("meta ") {
                if let Some((key, val)) = parse_meta_kv(rest)? {
                    tx_metadata.insert(key, val);
                }
            }
            i += 1;
            continue;
        }
        let posting = parse_posting_line(ct, i + 1)?;
        postings.push(posting);
        i += 1;
    }

    Ok((postings, tx_metadata, i))
}

// =====================================================================
// BEANCOUNT FORMAT
// =====================================================================

fn generate_beancount(data: &LedgerData) -> String {
    let mut out = String::new();

    out.push_str(";; -*- mode: beancount; coding: utf-8; -*-\n");
    out.push_str(";; Exported from rai\n");

    // Commodities — beancount uses "YYYY-MM-DD commodity NAME"
    // Use the earliest date we can find, or 1970-01-01
    if !data.commodities.is_empty() {
        out.push('\n');
        for c in &data.commodities {
            out.push_str(&format!("1970-01-01 commodity {}\n", c.name));
            // Store precision as metadata so we can round-trip
            out.push_str(&format!("  rai-precision: {}\n", c.precision));
            for (key, value) in &c.metadata {
                out.push_str(&format!("  {}: {}\n", key, format_meta_value(value)));
            }
        }
    }

    // Accounts
    if !data.accounts.is_empty() {
        out.push('\n');
        for a in &data.accounts {
            let currencies: Vec<&str> = a.currencies.iter().map(|id| data.cn(*id)).collect();
            let currency_suffix = if currencies.is_empty() {
                String::new()
            } else {
                format!(" {}", currencies.join(","))
            };
            // Beancount booking method is a quoted string at the end
            let booking_suffix = if a.booking_method != BookingMethod::Strict {
                format!(" \"{}\"", beancount_booking_str(a.booking_method))
            } else {
                String::new()
            };
            out.push_str(&format!(
                "{} open {}{}{}\n",
                a.opened_at, a.name, currency_suffix, booking_suffix
            ));
            for (key, value) in &a.metadata {
                out.push_str(&format!("  {}: {}\n", key, format_meta_value(value)));
            }
        }

        // Close directives
        for a in &data.accounts {
            if let Some(closed_at) = a.closed_at {
                out.push_str(&format!("{} close {}\n", closed_at, a.name));
            }
        }
    }

    // Transactions
    if !data.transactions.is_empty() {
        out.push('\n');
        for tx in &data.transactions {
            let (status, _time_part, payee_part, narration_part, tags_links) =
                format_tx_header(tx);

            // Beancount doesn't have native time support, store as metadata
            out.push_str(&format!(
                "{} {}{}{}{}\n",
                tx.date, status, payee_part, narration_part, tags_links
            ));

            if let Some(ref t) = tx.time {
                out.push_str(&format!("  time: \"{}\"\n", t.format("%H:%M:%S")));
            }
            for (key, value) in &tx.metadata {
                out.push_str(&format!("  {}: {}\n", key, format_meta_value(value)));
            }
            for posting in &tx.postings {
                write_posting(&mut out, posting, data, "");
            }
        }
    }

    // Prices
    if !data.prices.is_empty() {
        out.push('\n');
        for p in &data.prices {
            out.push_str(&format!(
                "{} price {} {} {}\n",
                p.date,
                data.cn(p.commodity_id),
                p.value,
                data.cn(p.target_commodity_id)
            ));
        }
    }

    // Balance assertions — beancount uses "pad" for automatic padding, but we use "balance"
    if !data.assertions.is_empty() {
        out.push('\n');
        for a in &data.assertions {
            out.push_str(&format!(
                "{} balance {} {} {}\n",
                a.date,
                data.an(a.account_id),
                a.expected.value,
                data.cn(a.expected.commodity_id)
            ));
        }
    }

    out
}

fn beancount_booking_str(bm: BookingMethod) -> &'static str {
    match bm {
        BookingMethod::Strict => "STRICT",
        BookingMethod::StrictWithSize => "STRICT_WITH_SIZE",
        BookingMethod::Fifo => "FIFO",
        BookingMethod::Lifo => "LIFO",
        BookingMethod::Hifo => "HIFO",
        BookingMethod::Average => "AVERAGE",
        BookingMethod::None => "NONE",
    }
}

fn beancount_booking_from_str(s: &str) -> Option<BookingMethod> {
    match s.to_uppercase().as_str() {
        "STRICT" => Some(BookingMethod::Strict),
        "STRICT_WITH_SIZE" => Some(BookingMethod::StrictWithSize),
        "FIFO" => Some(BookingMethod::Fifo),
        "LIFO" => Some(BookingMethod::Lifo),
        "HIFO" => Some(BookingMethod::Hifo),
        "AVERAGE" => Some(BookingMethod::Average),
        "NONE" => Some(BookingMethod::None),
        _ => None,
    }
}

fn parse_beancount(content: &str) -> Result<Vec<Directive>> {
    let lines: Vec<&str> = content.lines().collect();
    let mut directives: Vec<Directive> = Vec::new();
    let mut i = 0;

    while i < lines.len() {
        let line = lines[i];
        let trimmed = line.trim();

        if trimmed.is_empty() || trimmed.starts_with(';') {
            i += 1;
            continue;
        }

        // Skip beancount-only directives we don't support
        if trimmed.starts_with("option ")
            || trimmed.starts_with("plugin ")
            || trimmed.starts_with("include ")
        {
            i += 1;
            continue;
        }

        if !trimmed.starts_with(|c: char| c.is_ascii_digit()) {
            // Skip unrecognized non-date lines
            i += 1;
            continue;
        }

        // Date-prefixed directive
        let parts: Vec<&str> = trimmed.splitn(3, ' ').collect();
        if parts.len() < 2 {
            i += 1;
            continue;
        }

        let date = match parse_date(parts[0]) {
            Ok(d) => d,
            Err(_) => {
                i += 1;
                continue;
            }
        };

        let rest = if parts.len() > 2 { parts[2] } else { "" };

        match parts[1] {
            "commodity" => {
                let name = rest.split_whitespace().next().unwrap_or("").to_string();
                if name.is_empty() {
                    i += 1;
                    continue;
                }
                i += 1;
                // Parse metadata lines for precision
                let mut precision: u8 = 2; // default
                let mut metadata = Metadata::new();
                while i < lines.len() {
                    let cont = lines[i];
                    if !cont.starts_with("  ") || cont.trim().is_empty() {
                        break;
                    }
                    let ct = cont.trim();
                    if ct.starts_with(';') {
                        i += 1;
                        continue;
                    }
                    if let Some(rest) = ct.strip_prefix("rai-precision: ") {
                        if let Ok(p) = rest.trim().parse::<u8>() {
                            precision = p;
                        }
                    } else if let Some((key, val)) = parse_meta_kv(ct)? {
                        metadata.insert(key, val);
                    }
                    i += 1;
                }
                directives.push(Directive::Commodity {
                    name,
                    precision,
                    metadata,
                });
            }
            "open" => {
                // Format: "ACCOUNT [CURRENCIES] [\"BOOKING\"]"
                // Parse tokens, watching for quoted booking method at the end
                let chars: Vec<char> = rest.chars().collect();
                let mut pos = 0;
                let mut tokens: Vec<String> = Vec::new();
                let mut booking_str: Option<String> = None;

                while pos < chars.len() {
                    while pos < chars.len() && chars[pos] == ' ' {
                        pos += 1;
                    }
                    if pos >= chars.len() {
                        break;
                    }
                    if chars[pos] == '"' {
                        // Quoted booking method
                        booking_str = Some(parse_quoted_string(&chars, &mut pos)?);
                    } else {
                        let start = pos;
                        while pos < chars.len() && chars[pos] != ' ' {
                            pos += 1;
                        }
                        tokens.push(chars[start..pos].iter().collect());
                    }
                }

                if tokens.is_empty() {
                    bail!("Missing account name in open directive on line {}", i + 1);
                }

                let account = tokens[0].clone();
                let currencies: Vec<String> = tokens[1..]
                    .iter()
                    .flat_map(|t| t.split(','))
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();

                let booking_method = booking_str
                    .as_deref()
                    .and_then(beancount_booking_from_str)
                    .unwrap_or(BookingMethod::Strict);

                i += 1;
                let mut metadata = Metadata::new();
                while i < lines.len() {
                    let cont = lines[i];
                    if !cont.starts_with("  ") || cont.trim().is_empty() {
                        break;
                    }
                    let ct = cont.trim();
                    if ct.starts_with(';') {
                        i += 1;
                        continue;
                    }
                    if let Some((key, val)) = parse_meta_kv(ct)? {
                        metadata.insert(key, val);
                    }
                    i += 1;
                }

                directives.push(Directive::Open {
                    date,
                    account,
                    currencies,
                    booking_method,
                    metadata,
                });
            }
            "close" => {
                let account = rest.split_whitespace().next().unwrap_or("").to_string();
                if !account.is_empty() {
                    directives.push(Directive::Close { date, account });
                }
                i += 1;
                // Skip metadata
                while i < lines.len() && lines[i].starts_with("  ") && !lines[i].trim().is_empty()
                {
                    i += 1;
                }
            }
            "price" => {
                let tokens: Vec<&str> = rest.split_whitespace().collect();
                if tokens.len() >= 3 {
                    if let Ok(value) = tokens[1].parse::<Decimal>() {
                        directives.push(Directive::Price {
                            date,
                            commodity: tokens[0].to_string(),
                            value,
                            target_commodity: tokens[2].to_string(),
                        });
                    }
                }
                i += 1;
                while i < lines.len() && lines[i].starts_with("  ") && !lines[i].trim().is_empty()
                {
                    i += 1;
                }
            }
            "balance" => {
                let tokens: Vec<&str> = rest.split_whitespace().collect();
                if tokens.len() >= 3 {
                    if let Ok(amount) = tokens[1].parse::<Decimal>() {
                        directives.push(Directive::Balance {
                            date,
                            account: tokens[0].to_string(),
                            amount,
                            commodity: tokens[2].to_string(),
                        });
                    }
                }
                i += 1;
                while i < lines.len() && lines[i].starts_with("  ") && !lines[i].trim().is_empty()
                {
                    i += 1;
                }
            }
            s if s == "*" || s == "!" || s == "txn" => {
                let status = match s {
                    "*" | "txn" => TransactionStatus::Completed,
                    "!" => TransactionStatus::Pending,
                    _ => TransactionStatus::Completed,
                };

                let (payee, narration, tags, links) = parse_tx_fields(rest)?;

                i += 1;

                // Parse body: metadata and postings
                let mut postings: Vec<ParsedPosting> = Vec::new();
                let mut tx_metadata = Metadata::new();
                let mut time = None;

                while i < lines.len() {
                    let cont = lines[i];
                    if !cont.starts_with("  ") || cont.trim().is_empty() {
                        break;
                    }
                    let ct = cont.trim();
                    if ct.starts_with(';') {
                        i += 1;
                        continue;
                    }

                    // Check if this is a metadata line (contains ": " but doesn't look like a posting)
                    // Postings start with an account name (Capital letter or special prefix)
                    // Metadata looks like "key: value"
                    if is_metadata_line(ct) {
                        // Check for posting metadata (4+ spaces indent)
                        if cont.starts_with("    ") && !postings.is_empty() {
                            if let Some((key, val)) = parse_meta_kv(ct)? {
                                postings.last_mut().unwrap().metadata.insert(key, val);
                            }
                        } else if let Some(rest) = ct.strip_prefix("time: ") {
                            // Special: extract time metadata
                            let time_str = rest.trim().trim_matches('"');
                            if let Ok(t) =
                                chrono::NaiveTime::parse_from_str(time_str, "%H:%M:%S")
                            {
                                time = Some(t);
                            } else if let Ok(t) =
                                chrono::NaiveTime::parse_from_str(time_str, "%H:%M")
                            {
                                time = Some(t);
                            } else {
                                if let Some((key, val)) = parse_meta_kv(ct)? {
                                    tx_metadata.insert(key, val);
                                }
                            }
                        } else {
                            if let Some((key, val)) = parse_meta_kv(ct)? {
                                tx_metadata.insert(key, val);
                            }
                        }
                        i += 1;
                        continue;
                    }

                    // Try parsing as posting
                    match parse_posting_line(ct, i + 1) {
                        Ok(posting) => postings.push(posting),
                        Err(_) => {
                            // Could be metadata without colon, skip
                        }
                    }
                    i += 1;
                }

                directives.push(Directive::Transaction {
                    date,
                    time,
                    status,
                    payee,
                    narration,
                    tags,
                    links,
                    postings,
                    metadata: tx_metadata,
                });
            }
            // Skip unsupported directives (pad, note, document, event, query, custom)
            _ => {
                i += 1;
                while i < lines.len() && lines[i].starts_with("  ") && !lines[i].trim().is_empty()
                {
                    i += 1;
                }
            }
        }
    }

    Ok(directives)
}

/// Heuristic: a line is metadata if it matches "word: value" and the first word
/// is lowercase (account names start with uppercase).
fn is_metadata_line(line: &str) -> bool {
    if let Some(colon_pos) = line.find(": ") {
        let key = &line[..colon_pos];
        // Metadata keys are typically lowercase identifiers
        // Account names start with Assets/Liabilities/Income/Expenses/Equity (uppercase)
        !key.is_empty()
            && key.chars().next().map(|c| c.is_lowercase()).unwrap_or(false)
            && !key.contains(' ')
    } else {
        false
    }
}
