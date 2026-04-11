# Known Limitations

## Wildberries Stocks API Access

- Current status: `wb_stocks_current` cannot be fully populated with the current WB `base` token.
- Reason: WB analytics stocks endpoint (`seller-analytics`) requires a `Personal` or `Service` token with `Analytics` category access.
- Current behavior in ETL:
  - `update_all` continues in non-strict mode.
  - WB stocks step logs warning (`403 base token is not allowed`) and safely skips data load.

## TODO

- Enable full WB stocks ingestion after receiving a `Personal/Service` token with `Analytics` access.
