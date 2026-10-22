# Agent rules for Agent-Statistics

## Boundary

Two detector crates, one boundary rule each way:

- `token-usage-detector` — maps conversation, session and stream records into
  bound usage observations (push, bounded pull, cursor). A payload never names
  its own source, epoch, scope or measurement.
- `balance-detector` — maps provider account responses into bound credit and
  subscription-quota facts (weekly/monthly remaining share). A payload never
  names the account it describes.

Both crates are the boundary layer, not clients: no network connections, no
ledgers, no charges. Provider-specific acquisition lives in adapters above;
accounting lives in the statistics consumer below.

## Verify

`scripts/verify.sh`
