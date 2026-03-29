# Beancount Core Research

## Scope

This note answers:

- What Beancount is, at a high level
- What is in a Beancount file
- How the core double-entry ledger works
- How currency independence and multi-currency behavior work today

It uses three sources together:

- current Beancount code in `~/Repos/beancount/beancount`
- current Beancount docs in `~/Repos/beancount/docs`
- current Fava code only where it clarifies the surrounding ecosystem

When the docs and the code differ, the code wins.

## Table of Contents

- [1. What Beancount Is](#1-what-beancount-is)
- [2. What Is In a Beancount File](#2-what-is-in-a-beancount-file)
- [2.1 Directives Are the Top-Level Data Model](#21-directives-are-the-top-level-data-model)
- [2.2 Transactions Are Lists of Postings](#22-transactions-are-lists-of-postings)
- [2.3 Amount, Position, Cost, Inventory](#23-amount-position-cost-inventory)
- [2.4 Metadata, Tags, Links, Documents, Events, Queries](#24-metadata-tags-links-documents-events-queries)
- [2.5 Same-Day Ordering Matters](#25-same-day-ordering-matters)
- [3. Core Double-Entry Ledger Mechanism](#3-core-double-entry-ledger-mechanism)
- [3.1 Loader Pipeline](#31-loader-pipeline)
- [3.2 Double-Entry Is Enforced by "Weight", Not Just Units](#32-double-entry-is-enforced-by-weight-not-just-units)
- [3.3 Residual Check = "Does This Transaction Balance?"](#33-residual-check--does-this-transaction-balance)
- [3.4 Booking and Lot Reduction](#34-booking-and-lot-reduction)
- [3.5 Inference / Interpolation](#35-inference--interpolation)
- [4. Currency Independence and Multi-Currency](#4-currency-independence-and-multi-currency)
- [4.1 No Built-In Base Currency](#41-no-built-in-base-currency)
- [4.2 Mixed Commodities Are Native](#42-mixed-commodities-are-native)
- [4.3 Multi-Currency Transactions Use Prices, But Cost Wins Over Price](#43-multi-currency-transactions-use-prices-but-cost-wins-over-price)
- [4.4 Currency Constraints Are Optional, Not Fundamental](#44-currency-constraints-are-optional-not-fundamental)
- [4.5 Current Multi-Currency Weak Spot: Synthetic Conversion Entries](#45-current-multi-currency-weak-spot-synthetic-conversion-entries)
- [5. Takeaways for a Better Successor](#5-takeaways-for-a-better-successor)

## 1. What Beancount Is

At a high level, Beancount is a text-first double-entry accounting system. The user writes a ledger in a plain text language, Beancount parses it into immutable Python data structures, applies booking and plugins, validates the result, and then other tools query or render that in-memory ledger.

The current package metadata is still the clearest short summary:

```toml
# ~/Repos/beancount/beancount/pyproject.toml
description = "Command-line Double-Entry Accounting"
```

```toml
# ~/Repos/beancount/beancount/pyproject.toml
readme = { content-type = "text/x-rst", text = """
A double-entry accounting system that uses text files as input.
...
define financial transaction records in a text file, load them in
memory and generate and export a variety of reports
""" }
```

The user docs say the same thing, but with an important detail: Beancount is not only for fiat money. It is a general counting system for typed units:

- `docs/docs/beancount_language_syntax.md`: "it is a generic counting tool that works with multiple currencies, commodities held at cost (e.g., stocks), and even allows you to track unusual things, like vacation hours, air miles and rewards points"

My practical reading of the codebase:

- Beancount core is a parser + data model + booking engine + validation pipeline.
- It is intentionally deterministic and file-centric.
- It is not a live-sync product. External imports, prices, query UI, and web UI increasingly live outside the core repo.

## 2. What Is In a Beancount File

### 2.1 Directives Are the Top-Level Data Model

The fundamental unit is a dated directive. The core code says this directly:

```py
# ~/Repos/beancount/beancount/beancount/core/data.py
# Common Attributes (prepended to declared list):
#   meta: ...
#   date: A datetime.date instance; all directives have an associated date. Note:
#     Beancount does not consider time, only dates.
```

That "dates only, no times" rule is important. A Beancount file is not an event stream with timestamps; it is a dated ledger with stable within-day ordering rules.

The top-level directive set is explicit in code:

```py
# ~/Repos/beancount/beancount/beancount/core/data.py
ALL_DIRECTIVES = (
    Open,
    Close,
    Commodity,
    Pad,
    Balance,
    Transaction,
    Note,
    Event,
    Query,
    Price,
    Document,
    Custom,
)
```

So a Beancount file can contain:

- account lifecycle: `open`, `close`
- commodity declarations: `commodity`
- checking and initialization helpers: `balance`, `pad`
- journal entries: `transaction`
- annotation and attached artifacts: `note`, `document`
- market/reference data: `price`
- timeline variables: `event`
- named saved queries: `query`
- extension points: `custom`

### 2.2 Transactions Are Lists of Postings

The accounting center of gravity is `Transaction`, which contains `Posting` values. If the user says "splits", Beancount calls those "postings" or "legs".

```py
# ~/Repos/beancount/beancount/beancount/core/data.py
class Posting(NamedTuple):
    """
    Postings are contained in Transaction entries. These represent the individual
    legs of a transaction.
    """
    account: Account
    units: Optional[Amount]
    cost: Optional[Union[Cost, CostSpec]]
    price: Optional[Amount]
    flag: Optional[Flag]
    meta: Optional[Meta]
```

```py
# ~/Repos/beancount/beancount/beancount/core/data.py
class Transaction(NamedTuple):
    meta: Meta
    date: datetime.date
    flag: Optional[Flag]
    payee: Optional[str]
    narration: Optional[str]
    tags: frozenset[str]
    links: frozenset[str]
    postings: list[Posting]
```

This is the core semantic payload of a ledger:

- a dated transaction
- optional payee/narration
- optional tags and links
- one or more postings, each with account, units, optional cost, optional price, optional per-posting metadata

### 2.3 Amount, Position, Cost, Inventory

Beancount's internal model is more explicit than many accounting tools. A posting is not "just a number". The core types are:

```py
# ~/Repos/beancount/beancount/beancount/core/amount.py
class Amount(NamedTuple("Amount", [("number", Optional[Decimal]), ("currency", str)])):
    """An 'Amount' represents a number of a particular unit of something."""
```

```py
# ~/Repos/beancount/beancount/beancount/core/position.py
class Position(NamedTuple("Position", [("units", Amount), ("cost", Optional[Cost])])):
    """A 'Position' is a pair of units and optional cost.
    This is used to track inventories.
    """
```

```py
# ~/Repos/beancount/beancount/beancount/core/position.py
class Cost(NamedTuple):
    number: Decimal
    currency: str
    date: datetime.date
    label: Optional[str]
```

And inventories are first-class:

```py
# ~/Repos/beancount/beancount/beancount/core/inventory.py
"""A container for an inventory of positions.

This module provides a container class that can hold positions. An inventory is
a mapping of positions, where each position is keyed by

  (currency: str, cost: Cost) -> position: Position
"""
```

This is a major architectural choice. Beancount is not fundamentally "a table of debits and credits". It is "a typed ledger whose postings reduce into inventories of positions".

### 2.4 Metadata, Tags, Links, Documents, Events, Queries

Beancount carries more than transactions:

- metadata is arbitrary typed key/value data on directives and postings
- tags are lightweight labels for transactions
- links tie related transactions together over time
- documents attach external files
- events track non-financial state changes over time
- queries store named BQL snippets in the ledger itself

The docs are accurate here, and the code confirms it. For example:

```py
# ~/Repos/beancount/beancount/beancount/core/data.py
class Query(NamedTuple):
    """
    A named query declaration.
    ...
    query_string: The SQL query string to be run or made available.
    """
    meta: Meta
    date: datetime.date
    name: str
    query_string: str
```

### 2.5 Same-Day Ordering Matters

Although declaration order in the source file mostly does not matter, same-day ordering is normalized by the engine:

```py
# ~/Repos/beancount/beancount/beancount/core/data.py
SORT_ORDER = {Open: -2, Balance: -1, Document: 1, Close: 2}
```

```py
# ~/Repos/beancount/beancount/beancount/core/data.py
def entry_sortkey(entry: Directive) -> tuple[datetime.date, int, int]:
    return (entry.date, SORT_ORDER.get(type(entry), 0), entry.meta["lineno"])
```

Implication:

- `open` happens first on a day
- `balance` checks happen before transactions on that day
- `document` comes after transactions
- `close` comes last

This is one of the quiet but important semantics of the system.

## 3. Core Double-Entry Ledger Mechanism

### 3.1 Loader Pipeline

The loader shows the actual execution model:

```py
# ~/Repos/beancount/beancount/beancount/loader.py
with misc_utils.log_time("parse", log_timings, indent=1):
    entries, parse_errors, options_map = _parse_recursive(...)
    entries.sort(key=data.entry_sortkey)

with misc_utils.log_time("booking", log_timings, indent=1):
    entries, balance_errors = booking.book(entries, options_map)

with misc_utils.log_time("run_transformations", log_timings, indent=1):
    entries, errors = run_transformations(...)

with misc_utils.log_time("beancount.ops.validate", log_timings, indent=1):
    valid_errors = validation.validate(entries, options_map, ...)
```

So the core loop is:

1. parse text into directive objects
2. sort them deterministically
3. run booking/interpolation to resolve incomplete postings and lots
4. run plugins/transformations
5. validate invariants

### 3.2 Double-Entry Is Enforced by "Weight", Not Just Units

The most important implementation detail is that Beancount balances transactions by each posting's "weight", not always by raw units.

```py
# ~/Repos/beancount/beancount/beancount/core/convert.py
def get_weight(pos):
    """Return the weight of a Position or Posting.

    This is the amount that will need to be balanced from a posting of a
    transaction.
    """
```

The code then applies the rule:

```py
# ~/Repos/beancount/beancount/beancount/core/convert.py
if isinstance(cost, Cost) and isinstance(cost.number, Decimal):
    weight = Amount(cost.number * pos.units.number, cost.currency)
else:
    weight = units
    if not isinstance(pos, Position):
        price = pos.price
        if price is not None:
            converted_number = price.number * units.number
            weight = Amount(converted_number, price.currency)
```

Interpretation:

- simple cash posting: weight is the units
- posting with `@ price`: weight is converted by price
- posting held at cost: weight is cost basis, not market price

That is exactly why Beancount can auto-compute gains on sales held at cost.

### 3.3 Residual Check = "Does This Transaction Balance?"

Validation is also straightforward in code:

```py
# ~/Repos/beancount/beancount/beancount/core/interpolate.py
def compute_residual(postings):
    inventory = Inventory()
    for posting in postings:
        if posting.meta and posting.meta.get(AUTOMATIC_RESIDUAL, False):
            continue
        inventory.add_amount(convert.get_weight(posting))
    return inventory
```

```py
# ~/Repos/beancount/beancount/beancount/ops/validation.py
residual = interpolate.compute_residual(entry.postings)
tolerances = interpolate.infer_tolerances(entry.postings, options_map)
if not residual.is_small(tolerances):
    errors.append(... "Transaction does not balance: {}".format(residual))
```

So the double-entry invariant is:

- convert each posting to its balancing weight
- sum weights into an `Inventory`
- require the residual to be zero within inferred tolerances

This is a clean design. The residual is typed and multi-currency aware, not flattened to one scalar number.

### 3.4 Booking and Lot Reduction

Beancount's lot logic is a separate stage after parsing. The booking entrypoint:

```py
# ~/Repos/beancount/beancount/beancount/parser/booking.py
def book(incomplete_entries, options_map, initial_balances=None):
    """Book inventory lots and complete all positions with incomplete numbers."""
```

Ambiguous reductions are resolved by account booking method:

```py
# ~/Repos/beancount/beancount/beancount/core/data.py
class Booking(enum.Enum):
    STRICT = "STRICT"
    STRICT_WITH_SIZE = "STRICT_WITH_SIZE"
    NONE = "NONE"
    AVERAGE = "AVERAGE"
    FIFO = "FIFO"
    LIFO = "LIFO"
    HIFO = "HIFO"
```

And the actual implementations live in `parser/booking_method.py`, for example:

```py
# ~/Repos/beancount/beancount/beancount/parser/booking_method.py
def booking_method_FIFO(entry, posting, matches):
    """FIFO booking method implementation."""
```

Important current gap: `AVERAGE` exists in the enum, but the implementation is still disabled:

```py
# ~/Repos/beancount/beancount/beancount/parser/booking_method.py
def booking_method_AVERAGE(entry, posting, matches):
    errors = [AmbiguousMatchError(entry.meta, "AVERAGE method is not supported", entry)]
    return booked_reductions, booked_matches, errors, False
```

That matters for any "better Beancount" plan. Average-cost support is not a solved problem in the present core.

### 3.5 Inference / Interpolation

Beancount lets the user omit at most one posting amount and infer it from the balancing residual. The docs describe this accurately, and the code path is the booking/interpolation pipeline above.

This is one of the best examples of the system's ergonomics:

- the ledger stays explicit
- the balancing model stays strict
- users still get one missing leg auto-filled

## 4. Currency Independence and Multi-Currency

### 4.1 No Built-In Base Currency

The docs are explicit, and the code agrees: there is no privileged currency.

- `docs/docs/beancount_language_syntax.md`: "Beancount knows of no such thing; from its perspective all of these instruments are treated similarly."

The `Amount` type is just `(number, currency)`, and currencies are names, not enum values baked into the program:

```py
# ~/Repos/beancount/beancount/beancount/core/amount.py
class Amount(NamedTuple("Amount", [("number", Optional[Decimal]), ("currency", str)])):
```

The syntax docs also make the intended model clear:

- `USD`, `CAD`, `EUR`, `MSFT`, `AIRMILE`, `VACHR` are all the same kind of thing to Beancount: typed commodities

This is not "multi-currency support added on top". It is a commodity-typed core from the start.

### 4.2 Mixed Commodities Are Native

Inventories can hold multiple commodities at once. The docs say it plainly:

- `docs/docs/how_inventories_work.md`: "An inventory may contain more than one type of commodity."

And the code representation supports it directly:

```py
# ~/Repos/beancount/beancount/beancount/core/inventory.py
class Inventory(dict[tuple[str, Optional[Cost]], Position]):
    """An Inventory is a set of positions, indexed for efficiency."""
```

The important consequence is that a residual can itself be multi-commodity. Beancount does not force everything through USD-equivalent arithmetic.

### 4.3 Multi-Currency Transactions Use Prices, But Cost Wins Over Price

For a simple currency conversion posting, `@` price contributes the balancing weight. For a held-at-cost posting, the cost basis is the balancing weight and the price becomes informational/market-facing instead.

The code path is still `convert.get_weight()` above.

The docs explain the same rule, and this is worth preserving in any rewrite:

- `docs/docs/how_inventories_work.md`: "the price is not used by the balancing algorithm if there is a cost basis; the cost basis is the number used to balance the postings"

That is a very specific semantic choice. It is what lets Beancount compute capital gains cleanly while still recording sale prices.

### 4.4 Currency Constraints Are Optional, Not Fundamental

Accounts can optionally restrict allowed currencies, but this is validation layered on top, not a base assumption:

```py
# ~/Repos/beancount/beancount/beancount/core/data.py
class Open(NamedTuple):
    ...
    currencies: list[Currency]
```

```py
# ~/Repos/beancount/beancount/beancount/ops/validation.py
def validate_currency_constraints(entries, options_map):
    """Check the currency constraints from account open declarations."""
```

So the model is:

- ledger is inherently multi-commodity
- accounts may optionally constrain which commodities they accept

### 4.5 Current Multi-Currency Weak Spot: Synthetic Conversion Entries

The docs for v3 explicitly call out a current weakness: report-time normalization across currencies still relies on synthetic conversion entries.

The implementation is in `ops/summarize.py`:

```py
# ~/Repos/beancount/beancount/beancount/ops/summarize.py
def conversions(...):
    """Insert a conversion entry at date 'date' at the given account."""
```

And the key comment is unusually blunt:

```py
# ~/Repos/beancount/beancount/beancount/ops/summarize.py
# Important note: Set the cost to zero here to maintain the balance
# invariant. (This is the only single place we cheat on the balance rule
# in the entire system and this is necessary; see documentation on
# Conversions.)
```

That aligns with the design note in `docs/docs/beancount_v3.md`, which calls the current approach "a bit of a kludge".

There is also a prototype plugin for a better currency-account-based approach:

```py
# ~/Repos/beancount/beancount/beancount/plugins/currency_accounts.py
"""An implementation of currency accounts.

This is an automatic implementation of the method described here:
https://www.mathstat.dal.ca/~selinger/accounting/tutorial.html
"""
```

But the same file immediately warns:

```py
# ~/Repos/beancount/beancount/beancount/plugins/currency_accounts.py
WARNING: This is a prototype.
```

So the research conclusion is clear: Beancount is strongly currency-independent at the data-model level, but the cross-currency reporting story is still one of the most obvious places for a successor to improve.

## 5. Takeaways for a Better Successor

### Strong parts worth preserving

- Plain-text ledger as source of truth
- Immutable, explicit typed core objects
- First-class `Amount`, `Position`, and `Inventory` types
- Weight-based balancing instead of naive scalar summation
- Native multi-commodity model with no built-in base currency
- Deterministic loader pipeline and plugin stage separation

### Weak parts worth redesigning

- query engine split out of core but still conceptually central
- report-time currency normalization still depends on synthetic conversion entries
- booking methods expose features, like `AVERAGE`, that are not actually implemented
- date-only model prevents richer posting/settlement semantics
- historical docs sometimes describe a larger tool surface than current v3 exports

### Bottom line

Beancount's deepest idea is not "plain text accounting". It is a typed inventory engine for double-entry accounting, driven by a compact text language. Any "better Beancount" should preserve that core insight while cleaning up the multi-currency normalization story, query architecture, and unsupported or half-finished booking features.
