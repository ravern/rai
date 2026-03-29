use std::collections::HashMap;

use anyhow::{bail, Context, Result};
use chrono::{NaiveDate, NaiveTime};
use clap::Subcommand;
use rust_decimal::Decimal;
use tabled::Tabled;

use rai_core::types::*;
use rai_db::StorageProvider;

use crate::output::print_table;

#[derive(Subcommand)]
pub enum TxAction {
    /// Record a new transaction with two or more postings
    Create {
        /// Transaction date in YYYY-MM-DD format
        #[arg(long)]
        date: String,
        /// Transaction time in HH:MM:SS or HH:MM format
        #[arg(long)]
        time: Option<String>,
        /// Who the transaction is with
        #[arg(long)]
        payee: Option<String>,
        /// Description of the transaction
        #[arg(long)]
        narration: Option<String>,
        /// Transaction status: completed (default), pending, or flagged
        #[arg(long, default_value = "completed")]
        status: String,
        /// Tag for categorization (repeatable, e.g. --tag travel --tag reimbursable)
        #[arg(long)]
        tag: Vec<String>,
        /// Link to connect related transactions (repeatable)
        #[arg(long)]
        link: Vec<String>,
        /// Posting: "Account amount commodity [cost:amt comm date] [price:amt comm]" or "Account" to infer
        #[arg(long, required = true)]
        posting: Vec<String>,
    },
    /// List transactions with optional date, account, payee, tag, and status filters
    List {
        /// Start date (inclusive, YYYY-MM-DD)
        #[arg(long)]
        from: Option<String>,
        /// End date (inclusive, YYYY-MM-DD)
        #[arg(long)]
        to: Option<String>,
        /// Show only transactions touching this account
        #[arg(long)]
        account: Option<String>,
        /// Filter by payee name
        #[arg(long)]
        payee: Option<String>,
        /// Filter by tag
        #[arg(long)]
        tag: Option<String>,
        /// Filter by status: completed, pending, or flagged
        #[arg(long)]
        status: Option<String>,
    },
    /// Show full details of a transaction including all postings
    Show {
        /// Transaction ID
        id: i64,
    },
    /// Update transaction header fields (date, payee, narration, status)
    Update {
        /// Transaction ID to update
        id: i64,
        /// New date in YYYY-MM-DD format
        #[arg(long)]
        date: Option<String>,
        /// New payee
        #[arg(long)]
        payee: Option<String>,
        /// New narration
        #[arg(long)]
        narration: Option<String>,
        /// New status: completed, pending, or flagged
        #[arg(long)]
        status: Option<String>,
    },
    /// Delete a transaction and all its postings
    Delete {
        /// Transaction ID to delete
        id: i64,
    },
}

#[derive(Tabled)]
struct TxRow {
    #[tabled(rename = "ID")]
    id: i64,
    #[tabled(rename = "Date")]
    date: String,
    #[tabled(rename = "Status")]
    status: String,
    #[tabled(rename = "Payee")]
    payee: String,
    #[tabled(rename = "Narration")]
    narration: String,
    #[tabled(rename = "Postings")]
    postings: usize,
}

fn parse_date(s: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .with_context(|| format!("Invalid date format '{}'. Expected YYYY-MM-DD", s))
}

fn parse_time(s: &str) -> Result<NaiveTime> {
    NaiveTime::parse_from_str(s, "%H:%M:%S")
        .or_else(|_| NaiveTime::parse_from_str(s, "%H:%M"))
        .with_context(|| format!("Invalid time format '{}'. Expected HH:MM:SS or HH:MM", s))
}

/// Parsed posting before account/commodity resolution
struct ParsedPosting {
    account_name: String,
    amount: Option<Decimal>,
    commodity_name: Option<String>,
    cost: Option<ParsedCost>,
    price: Option<ParsedPrice>,
}

struct ParsedCost {
    amount: Decimal,
    commodity_name: String,
    date: NaiveDate,
}

struct ParsedPrice {
    amount: Decimal,
    commodity_name: String,
}

/// Parse a posting string.
/// Formats:
///   "Account:Name 100.00 USD"
///   "Account:Name 100.00 USD cost:50.00 EUR 2024-01-01"
///   "Account:Name 100.00 USD price:150.00 EUR"
///   "Account:Name" (amount inferred)
fn parse_posting_str(s: &str) -> Result<ParsedPosting> {
    let parts: Vec<&str> = s.split_whitespace().collect();
    if parts.is_empty() {
        bail!("Empty posting specification");
    }

    let account_name = parts[0].to_string();

    // If only account name, this is an inferred posting
    if parts.len() == 1 {
        return Ok(ParsedPosting {
            account_name,
            amount: None,
            commodity_name: None,
            cost: None,
            price: None,
        });
    }

    // Parse amount and commodity
    if parts.len() < 3 {
        bail!(
            "Invalid posting format: '{}'. Expected 'Account amount commodity [cost:...] [price:...]'",
            s
        );
    }

    let amount: Decimal = parts[1]
        .parse()
        .with_context(|| format!("Invalid amount: '{}'", parts[1]))?;
    let commodity_name = parts[2].to_string();

    let mut cost = None;
    let mut price = None;

    // Parse optional cost: and price: segments
    let mut i = 3;
    while i < parts.len() {
        if parts[i].starts_with("cost:") {
            // cost:amount commodity date
            let cost_amount_str = &parts[i]["cost:".len()..];
            let cost_amount: Decimal = cost_amount_str
                .parse()
                .with_context(|| format!("Invalid cost amount: '{}'", cost_amount_str))?;

            if i + 2 >= parts.len() {
                bail!("Cost requires: cost:<amount> <commodity> <date>");
            }
            let cost_commodity = parts[i + 1].to_string();
            let cost_date = parse_date(parts[i + 2])?;

            cost = Some(ParsedCost {
                amount: cost_amount,
                commodity_name: cost_commodity,
                date: cost_date,
            });
            i += 3;
        } else if parts[i].starts_with("price:") {
            // price:amount commodity
            let price_amount_str = &parts[i]["price:".len()..];
            let price_amount: Decimal = price_amount_str
                .parse()
                .with_context(|| format!("Invalid price amount: '{}'", price_amount_str))?;

            if i + 1 >= parts.len() {
                bail!("Price requires: price:<amount> <commodity>");
            }
            let price_commodity = parts[i + 1].to_string();

            price = Some(ParsedPrice {
                amount: price_amount,
                commodity_name: price_commodity,
            });
            i += 2;
        } else {
            bail!("Unexpected token in posting: '{}'", parts[i]);
        }
    }

    Ok(ParsedPosting {
        account_name,
        amount: Some(amount),
        commodity_name: Some(commodity_name),
        cost,
        price,
    })
}

fn resolve_commodity(
    provider: &dyn StorageProvider,
    name: &str,
    cache: &mut HashMap<String, Commodity>,
) -> Result<Commodity> {
    if let Some(c) = cache.get(name) {
        return Ok(c.clone());
    }
    let commodity = provider
        .get_commodity_by_name(name)
        .with_context(|| format!("Failed to look up commodity '{}'", name))?;
    let commodity = match commodity {
        Some(c) => c,
        None => bail!("Commodity '{}' not found", name),
    };
    cache.insert(name.to_string(), commodity.clone());
    Ok(commodity)
}

pub fn handle(action: TxAction, provider: &mut dyn StorageProvider) -> Result<()> {
    match action {
        TxAction::Create {
            date,
            time,
            payee,
            narration,
            status,
            tag,
            link,
            posting,
        } => create(
            provider,
            &date,
            time.as_deref(),
            payee,
            narration,
            &status,
            tag,
            link,
            posting,
        ),
        TxAction::List {
            from,
            to,
            account,
            payee,
            tag,
            status,
        } => list(
            provider,
            from.as_deref(),
            to.as_deref(),
            account.as_deref(),
            payee.as_deref(),
            tag.as_deref(),
            status.as_deref(),
        ),
        TxAction::Show { id } => show(provider, id),
        TxAction::Update {
            id,
            date,
            payee,
            narration,
            status,
        } => update(
            provider,
            id,
            date.as_deref(),
            payee,
            narration,
            status.as_deref(),
        ),
        TxAction::Delete { id } => delete(provider, id),
    }
}

#[allow(clippy::too_many_arguments)]
fn create(
    provider: &mut dyn StorageProvider,
    date_str: &str,
    time_str: Option<&str>,
    payee: Option<String>,
    narration: Option<String>,
    status_str: &str,
    tags: Vec<String>,
    links: Vec<String>,
    posting_strs: Vec<String>,
) -> Result<()> {
    let date = parse_date(date_str)?;
    let time = match time_str {
        Some(t) => Some(parse_time(t)?),
        None => None,
    };
    let status =
        TransactionStatus::from_str(status_str).with_context(|| {
            format!("Unknown status: '{}'. Use completed, pending, or flagged", status_str)
        })?;

    // Parse all posting strings
    let parsed: Vec<ParsedPosting> = posting_strs
        .iter()
        .map(|s| parse_posting_str(s))
        .collect::<Result<Vec<_>>>()?;

    // Resolve accounts and commodities
    let mut commodity_cache: HashMap<String, Commodity> = HashMap::new();
    let mut resolved_postings: Vec<Option<NewPosting>> = Vec::new();
    let mut inferred_index: Option<usize> = None;

    for (idx, p) in parsed.iter().enumerate() {
        let account = provider
            .get_account_by_name(&p.account_name)
            .with_context(|| format!("Failed to look up account '{}'", p.account_name))?;
        let account = match account {
            Some(a) => a,
            None => bail!("Account '{}' not found", p.account_name),
        };

        if p.amount.is_none() {
            // This posting needs amount inference
            if inferred_index.is_some() {
                bail!("Only one posting may have its amount inferred");
            }
            inferred_index = Some(idx);
            // Placeholder; we'll fill in later
            resolved_postings.push(None);
            continue;
        }

        let amount = p.amount.unwrap();
        let commodity_name = p.commodity_name.as_ref().unwrap();
        let commodity = resolve_commodity(provider, commodity_name, &mut commodity_cache)?;

        let cost = match &p.cost {
            Some(c) => {
                let cost_commodity =
                    resolve_commodity(provider, &c.commodity_name, &mut commodity_cache)?;
                Some(Cost {
                    amount: Amount {
                        value: c.amount,
                        commodity_id: cost_commodity.id,
                    },
                    date: c.date,
                    label: None,
                })
            }
            None => None,
        };

        let price = match &p.price {
            Some(pr) => {
                let price_commodity =
                    resolve_commodity(provider, &pr.commodity_name, &mut commodity_cache)?;
                Some(Amount {
                    value: pr.amount,
                    commodity_id: price_commodity.id,
                })
            }
            None => None,
        };

        resolved_postings.push(Some(NewPosting {
            account_id: account.id,
            units: Amount {
                value: amount,
                commodity_id: commodity.id,
            },
            cost,
            price,
            metadata: Metadata::new(),
        }));
    }

    // Infer missing posting amount if needed
    if let Some(infer_idx) = inferred_index {
        // Compute the weight of all other postings grouped by commodity
        let mut weights: HashMap<CommodityId, Decimal> = HashMap::new();
        for (idx, rp) in resolved_postings.iter().enumerate() {
            if idx == infer_idx {
                continue;
            }
            let posting = rp.as_ref().unwrap();
            // Compute weight
            let weight = if let Some(ref cost) = posting.cost {
                Amount {
                    value: posting.units.value * cost.amount.value,
                    commodity_id: cost.amount.commodity_id,
                }
            } else if let Some(ref price) = posting.price {
                Amount {
                    value: posting.units.value * price.value,
                    commodity_id: price.commodity_id,
                }
            } else {
                posting.units.clone()
            };
            *weights
                .entry(weight.commodity_id)
                .or_insert(Decimal::ZERO) += weight.value;
        }

        // The inferred posting must balance in a single commodity
        let non_zero: Vec<(CommodityId, Decimal)> = weights
            .into_iter()
            .filter(|(_, v)| !v.is_zero())
            .collect();

        if non_zero.len() > 1 {
            bail!(
                "Cannot infer posting amount: transaction involves multiple commodities. \
                 Please specify the amount explicitly."
            );
        }

        let p = &parsed[infer_idx];
        let account = provider
            .get_account_by_name(&p.account_name)
            .with_context(|| format!("Failed to look up account '{}'", p.account_name))?;
        let account = match account {
            Some(a) => a,
            None => bail!("Account '{}' not found", p.account_name),
        };

        let (commodity_id, total) = if non_zero.is_empty() {
            // All other postings sum to zero; we cannot infer commodity
            bail!(
                "Cannot infer posting amount: all other postings already balance to zero"
            );
        } else {
            non_zero[0]
        };

        resolved_postings[infer_idx] = Some(NewPosting {
            account_id: account.id,
            units: Amount {
                value: -total,
                commodity_id,
            },
            cost: None,
            price: None,
            metadata: Metadata::new(),
        });
    }

    let postings: Vec<NewPosting> = resolved_postings.into_iter().map(|p| p.unwrap()).collect();

    let new_tx = NewTransaction {
        date,
        time,
        status,
        payee,
        narration,
        tags,
        links,
        postings,
        metadata: Metadata::new(),
    };

    let tx = provider
        .create_transaction(&new_tx)
        .context("Failed to create transaction")?;
    println!(
        "Created transaction {} on {} ({} postings)",
        tx.id,
        tx.date,
        tx.postings.len()
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn list(
    provider: &mut dyn StorageProvider,
    from: Option<&str>,
    to: Option<&str>,
    account: Option<&str>,
    payee: Option<&str>,
    tag: Option<&str>,
    status: Option<&str>,
) -> Result<()> {
    let from_date = from.map(parse_date).transpose()?;
    let to_date = to.map(parse_date).transpose()?;

    let account_id = match account {
        Some(name) => {
            let acc = provider
                .get_account_by_name(name)
                .with_context(|| format!("Failed to look up account '{}'", name))?;
            let acc = match acc {
                Some(a) => a,
                None => bail!("Account '{}' not found", name),
            };
            Some(acc.id)
        }
        None => None,
    };

    let tx_status = match status {
        Some(s) => Some(
            TransactionStatus::from_str(s)
                .with_context(|| format!("Unknown status: '{}'", s))?,
        ),
        None => None,
    };

    let filter = TransactionFilter {
        from: from_date,
        to: to_date,
        account_id,
        payee: payee.map(|s| s.to_string()),
        tag: tag.map(|s| s.to_string()),
        status: tx_status,
    };

    let transactions = provider
        .list_transactions(&filter)
        .context("Failed to list transactions")?;

    let rows: Vec<TxRow> = transactions
        .into_iter()
        .map(|tx| TxRow {
            id: tx.id.0,
            date: tx.date.to_string(),
            status: tx.status.as_str().to_string(),
            payee: tx.payee.unwrap_or_default(),
            narration: tx.narration.unwrap_or_default(),
            postings: tx.postings.len(),
        })
        .collect();

    print_table(&rows);
    Ok(())
}

fn show(provider: &mut dyn StorageProvider, id: i64) -> Result<()> {
    let tx = provider
        .get_transaction(TransactionId(id))
        .context("Failed to look up transaction")?;
    let tx = match tx {
        Some(t) => t,
        None => bail!("Transaction {} not found", id),
    };

    // Build commodity name cache for display
    let commodities = provider
        .list_commodities()
        .context("Failed to list commodities")?;
    let commodity_map: HashMap<CommodityId, String> = commodities
        .into_iter()
        .map(|c| (c.id, c.name))
        .collect();

    // Build account name cache
    let accounts = provider
        .list_accounts(&AccountFilter::default())
        .context("Failed to list accounts")?;
    let account_map: HashMap<AccountId, String> = accounts
        .into_iter()
        .map(|a| (a.id, a.name))
        .collect();

    let commodity_name = |id: CommodityId| -> String {
        commodity_map
            .get(&id)
            .cloned()
            .unwrap_or_else(|| format!("commodity#{}", id.0))
    };

    let account_name = |id: AccountId| -> String {
        account_map
            .get(&id)
            .cloned()
            .unwrap_or_else(|| format!("account#{}", id.0))
    };

    println!("ID:        {}", tx.id);
    println!("Date:      {}", tx.date);
    if let Some(ref t) = tx.time {
        println!("Time:      {}", t);
    }
    println!("Status:    {}", tx.status.as_str());
    if let Some(ref p) = tx.payee {
        println!("Payee:     {}", p);
    }
    if let Some(ref n) = tx.narration {
        println!("Narration: {}", n);
    }
    if !tx.tags.is_empty() {
        println!("Tags:      {}", tx.tags.join(", "));
    }
    if !tx.links.is_empty() {
        println!("Links:     {}", tx.links.join(", "));
    }

    println!("Postings:");
    for posting in &tx.postings {
        let acct = account_name(posting.account_id);
        let units = format!(
            "{} {}",
            posting.units.value,
            commodity_name(posting.units.commodity_id)
        );

        let mut extra = String::new();
        if let Some(ref cost) = posting.cost {
            extra.push_str(&format!(
                " {{{}  {} {}}}",
                cost.amount.value,
                commodity_name(cost.amount.commodity_id),
                cost.date
            ));
        }
        if let Some(ref price) = posting.price {
            extra.push_str(&format!(
                " @ {} {}",
                price.value,
                commodity_name(price.commodity_id)
            ));
        }

        println!("  {} {}{}", acct, units, extra);
    }

    if !tx.metadata.is_empty() {
        println!("Metadata:");
        for (key, value) in &tx.metadata {
            println!("  {}: {:?}", key, value);
        }
    }
    Ok(())
}

fn update(
    provider: &mut dyn StorageProvider,
    id: i64,
    date: Option<&str>,
    payee: Option<String>,
    narration: Option<String>,
    status: Option<&str>,
) -> Result<()> {
    let tx_id = TransactionId(id);

    // Verify transaction exists
    let existing = provider
        .get_transaction(tx_id)
        .context("Failed to look up transaction")?;
    if existing.is_none() {
        bail!("Transaction {} not found", id);
    }

    let new_date = date.map(parse_date).transpose()?;
    let new_status = match status {
        Some(s) => Some(
            TransactionStatus::from_str(s)
                .with_context(|| format!("Unknown status: '{}'", s))?,
        ),
        None => None,
    };

    let update = TransactionUpdate {
        date: new_date,
        time: None,
        status: new_status,
        payee: payee.map(Some),
        narration: narration.map(Some),
    };

    let tx = provider
        .update_transaction(tx_id, &update)
        .context("Failed to update transaction")?;
    println!("Updated transaction {}", tx.id);
    Ok(())
}

fn delete(provider: &mut dyn StorageProvider, id: i64) -> Result<()> {
    let tx_id = TransactionId(id);

    let existing = provider
        .get_transaction(tx_id)
        .context("Failed to look up transaction")?;
    if existing.is_none() {
        bail!("Transaction {} not found", id);
    }

    provider
        .delete_transaction(tx_id)
        .context("Failed to delete transaction")?;
    println!("Deleted transaction {}", id);
    Ok(())
}
