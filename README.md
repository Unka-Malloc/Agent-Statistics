# Agent-Statistics

The agent statistics family: detector crates that acquire the facts, and the
journal engine that measures and keeps them.

| Crate | Question it answers |
| --- | --- |
| `token-usage-detector` | What did the work cost? Maps conversation, session and stream records into bound usage observations through the push, bounded-pull and cursor collection protocol, with the source identity, epoch, scope and measurement attached by the host — never by the payload. |
| `balance-detector` | How much remains? Maps provider account responses into bound credit and subscription-quota facts (remaining amounts, weekly/monthly remaining percentages, reset times), with the account bound by the host and provider-specific fetching behind the `BalanceSource` port. |
| `agent-statistics` | What does the history say? The usage journal, incremental projection, windowing, attribution, retained-report persistence and request counters; product knowledge arrives through the `StatisticsHost` port. |

The detector crates are boundary layers: they open no connections, store no
ledgers and compute no charges. Provider-specific acquisition lives in
adapters; `agent-statistics` is the accounting side the host composes.

## License

Apache-2.0
