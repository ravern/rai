# Beancount Research

This directory contains source-backed research for the current Beancount ecosystem, focused on understanding the existing system before planning a better successor.

## Snapshot

Code snapshots used for this research:

- `beancount`: `923ea6c5464bc0a2fb6316f60db581d28bd0f909`
- `docs`: `49a4896e30f6fd265ec2396906839b5db494e2f3`
- `fava`: `d677bc824f0e1a62ad0ffa05e224e35995ff4b5e`
- `beanquery` (cloned into `.context/beanquery` because the local Beancount repos do not include the query engine): `aa0776285a25baeedf151e9f582bef0314f76004`

Note: the `beanquery` snapshot is an upstream code snapshot used to understand the current engine architecture. Fava's local dependency range is still `beanquery>=0.1,<0.3`, so treat API-level details there as "current upstream shape", not a lockfile-exact reproduction of the local Fava environment.

## Documents

- [beancount-core.md](./beancount-core.md)
  - What Beancount is
  - What lives in a Beancount file
  - Core double-entry and inventory mechanics
  - Currency independence and multi-currency behavior
  - Research takeaways for a successor design

- [beancount-query-and-interfaces.md](./beancount-query-and-interfaces.md)
  - Query language implementation boundary
  - `beanquery` internals and how Fava uses them
  - Current user-facing command surface
  - Five grounded user stories
  - Design implications for a better version

## Method

- I treated the docs as helpful but non-authoritative when they conflicted with code.
- For implementation claims, I quoted Python sources directly.
- For user-facing behavior, I preferred current exported commands and current dependencies over older v2 documentation.
