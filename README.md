# Agent-Statistics

Detector crates for agent statistics: how much was used, and how much is left.

| Crate | Question it answers |
| --- | --- |
| `token-usage-detector` | What did the work cost? Maps conversation, session and stream records into bound usage observations through the push, bounded-pull and cursor collection protocol, with the source identity, epoch, scope and measurement attached by the host — never by the payload. |
| `balance-detector` | How much remains? Maps provider account responses into bound credit and subscription-quota facts (remaining amounts, weekly/monthly remaining percentages, reset times), with the account bound by the host and provider-specific fetching behind the `BalanceSource` port. |

Both crates are boundary layers: they open no connections, store no ledgers
and compute no charges. Provider-specific acquisition lives in adapters;
aggregation, attribution and accounting live in the statistics consumer.

## License

Apache-2.0
