# rai — Storage Provider

## Design

The `rai-db` crate defines a storage provider trait and ships a SQLite implementation. The trait is designed so that other backends (Postgres, etc.) can be added later.

## Provider Trait

```rust
/// Core storage operations. Each method maps to CRUD on the public schema.
trait StorageProvider {
    // Commodities
    fn create_commodity(&mut self, commodity: &NewCommodity) -> Result<Commodity>;
    fn get_commodity(&self, id: CommodityId) -> Result<Option<Commodity>>;
    fn get_commodity_by_name(&self, name: &str) -> Result<Option<Commodity>>;
    fn list_commodities(&self) -> Result<Vec<Commodity>>;
    fn update_commodity(&mut self, id: CommodityId, update: &CommodityUpdate) -> Result<Commodity>;
    fn delete_commodity(&mut self, id: CommodityId) -> Result<()>;

    // Accounts
    fn create_account(&mut self, account: &NewAccount) -> Result<Account>;
    fn get_account(&self, id: AccountId) -> Result<Option<Account>>;
    fn get_account_by_name(&self, name: &str) -> Result<Option<Account>>;
    fn list_accounts(&self, filter: &AccountFilter) -> Result<Vec<Account>>;
    fn update_account(&mut self, id: AccountId, update: &AccountUpdate) -> Result<Account>;
    fn open_account(&mut self, id: AccountId, date: NaiveDate) -> Result<Account>;
    fn close_account(&mut self, id: AccountId, date: NaiveDate) -> Result<Account>;
    fn delete_account(&mut self, id: AccountId) -> Result<()>;

    // Transactions (with postings)
    fn create_transaction(&mut self, tx: &NewTransaction) -> Result<Transaction>;
    fn get_transaction(&self, id: TransactionId) -> Result<Option<Transaction>>;
    fn list_transactions(&self, filter: &TransactionFilter) -> Result<Vec<Transaction>>;
    fn update_transaction(&mut self, id: TransactionId, update: &TransactionUpdate) -> Result<Transaction>;
    fn delete_transaction(&mut self, id: TransactionId) -> Result<()>;

    // Prices
    fn create_price(&mut self, price: &NewPrice) -> Result<Price>;
    fn get_price(&self, commodity: CommodityId, target: CommodityId, date: NaiveDate) -> Result<Option<Price>>;
    fn list_prices(&self, filter: &PriceFilter) -> Result<Vec<Price>>;
    fn delete_price(&mut self, id: PriceId) -> Result<()>;

    // Balance assertions
    fn create_balance_assertion(&mut self, assertion: &NewBalanceAssertion) -> Result<BalanceAssertion>;
    fn list_balance_assertions(&self, filter: &BalanceAssertionFilter) -> Result<Vec<BalanceAssertion>>;
    fn delete_balance_assertion(&mut self, id: BalanceAssertionId) -> Result<()>;

    // Raw query passthrough
    fn query_raw(&self, sql: &str) -> Result<QueryResult>;

    // Schema initialization
    fn initialize(&mut self) -> Result<()>;
}
```

## Raw Query

The `query_raw` method passes SQL directly to the underlying database. For SQLite, this is literal SQL. The REPL in the CLI uses this method.

```rust
struct QueryResult {
    columns: Vec<String>,
    rows: Vec<Vec<Value>>,  // Value is a simple enum: Null, Integer, Real, Text
}
```

## SQLite Implementation

```rust
struct SqliteProvider {
    conn: rusqlite::Connection,
}

impl SqliteProvider {
    fn open(path: &Path) -> Result<Self>;
    fn open_in_memory() -> Result<Self>;  // for testing
}

impl StorageProvider for SqliteProvider { ... }
```

## Transaction Atomicity

Write operations that span multiple tables (e.g., creating a transaction with postings, tags, links, and metadata) are wrapped in a database transaction to ensure atomicity.
