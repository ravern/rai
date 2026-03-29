# Beancount Query and Interface Research

## Scope

This note covers:

- where the Beancount query language lives today
- how the current query implementation works
- the current command/interface surface for real users
- five grounded user stories

For this section I had to go beyond the three local repos:

- the Beancount repo and docs clearly show that the query engine moved out
- Fava depends on `beanquery`
- the local workspace did not include the `beanquery` source

So I cloned `https://github.com/beancount/beanquery` into `.context/beanquery` and used that code directly.

Important caveat: this gives a current upstream `beanquery` snapshot. The local Fava repo currently depends on `beanquery>=0.1,<0.3`, so I use the cloned repo here to describe the engine architecture and layering, not to claim an exact dependency-locked match for every API detail.

## Table of Contents

- [1. Query Language Implementation: Current Boundary](#1-query-language-implementation-current-boundary)
- [1.1 Beancount Core Still Parses and Stores Query Directives](#11-beancount-core-still-parses-and-stores-query-directives)
- [1.2 But the Actual Query Engine Is No Longer in the Core Repo](#12-but-the-actual-query-engine-is-no-longer-in-the-core-repo)
- [2. How `beanquery` Works](#2-how-beanquery-works)
- [2.1 Entry Point and Data Source Model](#21-entry-point-and-data-source-model)
- [2.2 The Beancount Source Attaches Tables](#22-the-beancount-source-attaches-tables)
- [2.3 Parsing](#23-parsing)
- [2.4 Compilation](#24-compilation)
- [2.5 Execution](#25-execution)
- [2.6 Beancount-Specific Columns and Functions](#26-beancount-specific-columns-and-functions)
- [3. How Fava Uses the Query Engine](#3-how-fava-uses-the-query-engine)
- [4. Current User Interface Surface](#4-current-user-interface-surface)
- [4.1 Current Core Beancount CLI](#41-current-core-beancount-cli)
- [4.2 Query CLI Is Now `beanquery`](#42-query-cli-is-now-beanquery)
- [4.3 Web UI Is Fava](#43-web-ui-is-fava)
- [4.4 Historical Docs Are Larger Than Current Reality](#44-historical-docs-are-larger-than-current-reality)
- [5. Five Grounded User Stories](#5-five-grounded-user-stories)
- [5.1 Story 1: Personal Finance Operator](#51-story-1-personal-finance-operator)
- [5.2 Story 2: Expat or Traveler With Real Multi-Currency Life](#52-story-2-expat-or-traveler-with-real-multi-currency-life)
- [5.3 Story 3: Investor Tracking Lots and Gains](#53-story-3-investor-tracking-lots-and-gains)
- [5.4 Story 4: Shared-Expense or Project Accounting User](#54-story-4-shared-expense-or-project-accounting-user)
- [5.5 Story 5: Automation-Heavy Power User](#55-story-5-automation-heavy-power-user)
- [6. Implications for a Better Successor](#6-implications-for-a-better-successor)

## 1. Query Language Implementation: Current Boundary

### 1.1 Beancount Core Still Parses and Stores Query Directives

Beancount still has `query` as a first-class directive in the ledger language:

```py
# ~/Repos/beancount/beancount/beancount/core/data.py
class Query(NamedTuple):
    meta: Meta
    date: datetime.date
    name: str
    query_string: str
```

And the parser still constructs that object:

```py
# ~/Repos/beancount/beancount/beancount/parser/grammar.py
def query(self, filename, lineno, date, query_name, query_string, kvlist):
    meta = new_metadata(filename, lineno, kvlist)
    return Query(meta, date, query_name, query_string)
```

The grammar entry is direct:

```yacc
# ~/Repos/beancount/beancount/beancount/parser/grammar.y
query:
  DATE QUERY STRING STRING eol key_value_list
```

So the core language still knows about stored named queries.

### 1.2 But the Actual Query Engine Is No Longer in the Core Repo

This is explicitly documented in the v3 design notes:

- `docs/docs/beancount_v3.md`: "The query language will be factored out into a completely separate repo"

That matches the code reality:

- the current `beancount` package exports no `bean-query` script in `pyproject.toml`
- Fava depends on `beanquery`
- `beanquery` is now its own package and CLI

So the architectural split today is:

- Beancount core: parse ledger, store `Query` directives, expose Beancount data structures
- beanquery: parse/compile/execute BQL against Beancount data
- Fava: provide web UI and wrap `beanquery` for users

## 2. How `beanquery` Works

### 2.1 Entry Point and Data Source Model

The top-level API is DB-API-shaped:

```py
# .context/beanquery/beanquery/__init__.py
def connect(dsn, **kwargs):
    return Connection(dsn, **kwargs)
```

```py
# .context/beanquery/beanquery/__init__.py
class Connection:
    def attach(self, dsn, **kwargs):
        scheme = urlparse(dsn).scheme
        source = importlib.import_module(f'beanquery.sources.{scheme}')
        source.attach(self, dsn, **kwargs)
```

This is an important design choice. `beanquery` is not hard-wired only for Beancount. It is a small query engine with pluggable sources. Beancount is one source, accessed as `beancount:`.

### 2.2 The Beancount Source Attaches Tables

The Beancount adapter is in `beanquery/sources/beancount.py`:

```py
# .context/beanquery/beanquery/sources/beancount.py
def attach(context, dsn, *, entries=None, errors=None, options=None):
    filename = urlparse(dsn).path
    if filename:
        entries, errors, options = loader.load_file(filename)
    for table in _TABLES:
        context.tables[table.name] = table(entries, options)
    context.options.update(options)
    context.errors.extend(errors)
    context.tables[None] = context.tables['postings']
```

This tells us exactly what the query engine expects from Beancount:

- loaded `entries`
- parse/validation `errors`
- `options`
- a set of typed tables layered over those entries

The same file defines tables like:

- `transactions`
- `prices`
- `balances`
- `notes`
- `events`
- `documents`
- `accounts`
- `commodities`

So BQL is not a SQL veneer over a SQL database. It is a query language over an in-memory typed object graph built from ledger directives.

### 2.3 Parsing

The parser is a standalone component:

```py
# .context/beanquery/beanquery/parser/__init__.py
def parse(text):
    try:
        return BQLParser().parse(text, semantics=BQLSemantics())
```

The semantics object lowers textual syntax into AST nodes:

```py
# .context/beanquery/beanquery/parser/__init__.py
class BQLSemantics:
    def integer(self, value):
        return int(value)
    def decimal(self, value):
        return decimal.Decimal(value)
    def date(self, value):
        return datetime.date.fromisoformat(value)
```

The grammar itself includes both SQL-like `SELECT` and convenience commands:

```ebnf
# .context/beanquery/beanquery/parser/bql.ebnf
statement
    =
    | select
    | balances
    | journal
```

And the `FROM` clause is extended with Beancount-specific period operations:

```ebnf
# .context/beanquery/beanquery/parser/bql.ebnf
| 'OPEN' ~ 'ON' open:date ['CLOSE' ('ON' close:date | {} close:`True`)] ['CLEAR' clear:`True`]
| 'CLOSE' ~ ('ON' close:date | {} close:`True`) ['CLEAR' clear:`True`]
| 'CLEAR' ~ clear:`True`
```

This is one of the clearest places where BQL stops being generic SQL and becomes ledger-aware query syntax.

### 2.4 Compilation

`beanquery` compiles AST nodes into executable evaluation objects:

```py
# .context/beanquery/beanquery/compiler.py
class Compiler:
    def compile(self, query, parameters=None):
        """Compile an AST into an executable statement."""
        ...
        return self._compile(query)
```

For `SELECT`, the compiler:

- compiles `FROM`
- compiles target expressions
- compiles `WHERE`
- resolves `GROUP BY`
- resolves `ORDER BY`
- returns an `EvalQuery`

The final compiled representation is explicit:

```py
# .context/beanquery/beanquery/query_compile.py
@dataclasses.dataclass
class EvalQuery:
    table: tables.Table
    c_targets: list
    c_where: EvalNode
    group_indexes: list[int]
    having_index: int
    order_spec: list[tuple[int, ast.Ordering]]
    limit: int
    distinct: bool
```

The compiler also shows a pragmatic language feature: implicit grouping can still happen if aggregates and non-aggregates are mixed:

```py
# .context/beanquery/beanquery/compiler.py
SUPPORT_IMPLICIT_GROUPBY = True
```

That is convenient, but it is also a semantic choice a successor should revisit explicitly.

### 2.5 Execution

Execution is row-oriented and table-backed:

```py
# .context/beanquery/beanquery/cursor.py
def execute(self, query, params=None):
    if not isinstance(query, parser.ast.Node):
        query = parser.parse(query)
    query = compiler.compile(self._context, query, params)
    description, rows = query()
```

And `EvalQuery.__call__()` delegates to the select executor:

```py
# .context/beanquery/beanquery/query_compile.py
def __call__(self):
    return query_execute.execute_select(self)
```

The executor has two modes:

- non-aggregate: scan rows and evaluate target expressions
- aggregate: allocate aggregate state, group rows, finalize stores, then render result rows

```py
# .context/beanquery/beanquery/query_execute.py
if query.group_indexes is None:
    for context in query.table:
        if c_where is None or c_where(context):
            values = [c_expr(context) for c_expr in c_target_exprs]
            rows.append(values)
else:
    ...
    aggregates = collections.defaultdict(create)
```

This is a compact interpreter, not a database optimizer.

### 2.6 Beancount-Specific Columns and Functions

The Beancount-specific semantic environment lives in `query_env.py`:

```py
# .context/beanquery/beanquery/query_env.py
"""Environment object for compiler.

This module contains the various column accessors and function evaluators that
are made available by the query compiler via their compilation context objects.
"""
```

That file registers:

- column accessors
- scalar functions
- aggregate functions
- account/date/metadata helpers
- inventory and conversion helpers

Examples visible in code:

- `open_date()`, `close_date()`
- `meta()`, `entry_meta()`, `commodity_meta()`
- `units()`, `cost()`, `value()`, `convert()`
- `sum(position)` returning an `Inventory`

This is the real answer to "how Beancount query language is implemented": BQL is a generic parser/compiler/executor wrapped around a Beancount-specific semantic environment of tables, columns, and functions.

## 3. How Fava Uses the Query Engine

Fava wraps `beanquery`, rather than re-implementing BQL itself.

```py
# ~/Repos/beancount/fava/src/fava/core/query_shell.py
from beanquery import CompilationError
from beanquery import connect
from beanquery import Cursor
from beanquery import ParseError
from beanquery.shell import BQLShell
```

Its shell wrapper is thin:

```py
# ~/Repos/beancount/fava/src/fava/core/query_shell.py
class FavaBQLShell(BQLShell):
    """A light wrapper around Beancount's shell."""
```

And it builds the Beancount-backed query context exactly like `bean-query` does:

```py
# ~/Repos/beancount/fava/src/fava/core/query_shell.py
self.context = connect(
    "beancount:",
    entries=entries,
    errors=self.ledger.errors,
    options=self.ledger.options,
)
```

Practical implication:

- Fava is not "the query implementation"
- Fava is a UI layer over the same query engine

That is a good boundary in principle. It also means query correctness is shared between CLI and web UI.

## 4. Current User Interface Surface

### 4.1 Current Core Beancount CLI

The current v3 Beancount package exports only these scripts:

```toml
# ~/Repos/beancount/beancount/pyproject.toml
[project.scripts]
bean-check = "beancount.scripts.check:main"
bean-doctor = "beancount.scripts.doctor:main"
bean-example = "beancount.scripts.example:main"
bean-format = "beancount.scripts.format:main"
treeify = "beancount.tools.treeify:main"
```

That is the authoritative current command surface for the core package.

The roles are:

- `bean-check`: load, validate, and exit nonzero on errors
- `bean-doctor`: debugging and introspection commands
- `bean-format`: whitespace-only formatter for ledger files
- `bean-example`: generate example ledger
- `treeify`: text tree formatter utility

Examples from code:

```py
# ~/Repos/beancount/beancount/beancount/scripts/check.py
"""Parse, check and realize a beancount ledger."""
```

```py
# ~/Repos/beancount/beancount/beancount/scripts/doctor.py
"""Debugging tool for those finding bugs in Beancount."""
```

```py
# ~/Repos/beancount/beancount/beancount/scripts/format.py
"""Automatically format a Beancount ledger."""
```

### 4.2 Query CLI Is Now `beanquery`

The `bean-query` command comes from `beanquery`, not the current core Beancount package:

```py
# .context/beanquery/beanquery/shell.py
@click.command()
def main(filename, query, numberify, format, output, no_errors):
    """An interactive interpreter for the Beancount Query Language."""
```

And it auto-wraps plain filenames as `beancount:` DSNs:

```py
# .context/beanquery/beanquery/shell.py
source = filename if re.match('[a-z]{2,}:',filename) else 'beancount:' + filename
```

For most serious users, `bean-query` is still a major part of the ecosystem. It just no longer ships from the same repo/package.

### 4.3 Web UI Is Fava

Fava's package metadata is explicit:

```toml
# ~/Repos/beancount/fava/pyproject.toml
description = "Web interface for the accounting tool Beancount."
```

Its CLI is similarly direct:

```py
# ~/Repos/beancount/fava/src/fava/cli.py
def main(...):
    """Start Fava for FILENAMES on http://<host>:<port>."""
```

And the README shows the expected user path:

```rst
# ~/Repos/beancount/fava/README.rst
pip3 install fava
fava ledger.beancount
```

For current users, Fava is the primary interactive interface. The Beancount v3 design note also says this plainly:

- `docs/docs/beancount_v3.md`: "Fava subsumes bean-web"

### 4.4 Historical Docs Are Larger Than Current Reality

This matters for research. Some of the Beancount docs still describe an older, broader tool suite:

- `docs/docs/running_beancount_and_generating_reports.md` documents `bean-report`, `bean-web`, `bean-bake`, and older report workflows

But the same file begins with:

- "This document applies to tools from the v2 branch which have been deprecated"

So the practical present-day interface is:

- author ledger text
- run `bean-check`
- run `bean-query` for ad hoc reports
- run `fava` for browsing, editing, and interactive reports
- use `bean-doctor` when debugging loader/parser issues

## 5. Five Grounded User Stories

These are not fictional product fantasies. They are grounded in the Beancount docs, example workflows, and current tool surface.

### Story 1: Personal finance operator

A user keeps one canonical text ledger for checking, savings, credit cards, salary, taxes, and recurring expenses. They edit the file in a text editor, run `bean-check` constantly, and use Fava to browse balances, journals, and account trees.

Why this is source-grounded:

- `docs/docs/getting_started_with_beancount.md`
- `fava/README.rst`
- `fava/src/fava/help/beancount_syntax.md`

### Story 2: Expat or traveler with real multi-currency life

A user holds assets and expenses in USD, CAD, EUR, MXN, or arbitrary instruments, and wants one ledger that preserves original currencies instead of flattening everything into a home currency. They need price directives, conversions, and reports that can still aggregate mixed inventories sensibly.

Why this is source-grounded:

- `docs/docs/beancount_language_syntax.md`
- `docs/docs/how_inventories_work.md`
- `docs/docs/sharing_expenses_with_beancount.md`

### Story 3: Investor tracking lots and gains

A user buys and sells securities, wants explicit cost basis on lots, wants FIFO/LIFO/HIFO behavior where appropriate, and wants capital gains to emerge from the ledger instead of from a separate spreadsheet.

Why this is source-grounded:

- `docs/docs/how_inventories_work.md`
- `docs/docs/trading_with_beancount.md`
- `docs/docs/stock_vesting_in_beancount.md`
- `beancount/parser/booking_method.py`

### Story 4: Shared-expense or project accounting user

A user treats a trip, household, or project as its own mini-entity, tracks who paid for what, and uses the ledger to compute who ultimately owes whom after equal or unequal splits.

Why this is source-grounded:

- `docs/docs/sharing_expenses_with_beancount.md`

### Story 5: Automation-heavy power user

A user downloads bank files, semi-automates import and cleanup, stores the canonical ledger in plain text, and uses named queries plus Fava query pages to build custom views, exports, and reconciliation workflows.

Why this is source-grounded:

- `docs/docs/importing_external_data.md`
- `docs/docs/beancount_query_language.md`
- `fava/src/fava/help/import.md`
- `fava/src/fava/help/features.md`

## 6. Implications for a Better Successor

### What is strong today

- query engine is already separated enough to be reused from CLI and web UI
- Beancount data model is rich enough that queries operate on typed values, not just strings
- Fava proves there is demand for a polished interactive layer over the ledger

### What is awkward today

- the conceptual product boundary is split across four repos: core, docs, Fava, beanquery
- the docs still carry v2-era mental models, while the current v3 package exports a much smaller CLI
- named queries are stored in the ledger, but the execution engine is no longer in the same package
- BQL uses a custom interpreter and custom semantic environment, which is powerful but makes the public contract harder to describe than a stricter schema-driven model

### Bottom line

If the goal is "a better Beancount", there are two defensible directions:

1. Keep the current split, but make it explicit and stable:
   core ledger engine, query engine, and UI as clearly versioned layers with a documented schema between them.

2. Re-integrate around one stable execution model:
   text parser, immutable ledger IR, query runtime, and UI all built around one public typed representation.

Either way, the current research points to one central requirement: the next system should treat the typed ledger representation as the stable product boundary, not just the text syntax.
