# Adding a channel

Companion to [DESIGN.md](../DESIGN.md).

The core never refers to a channel by name. The order transaction loops over the enabled rows in `sales_channels`, and rate limits, batch size and the polling cursor are columns there. A new channel needs four things:

1. A `sales_channels` row, disabled at first, including its `erp_customer_code` for Business Central.
2. One adapter class with `verifyWebhook`, `parseEvent`, `poll` (polling channels only) and `pushStock(rows)`, found by code through a generic `/webhooks/:code` route.
3. One more worker process with its own queue.
4. `channel_inventory` rows for every SKU with `last_synced_quantity` empty, then enable the channel. The normal push loop does the initial full sync, riskiest SKUs first.

Orders, movements, outbox, the ERP worker and the ops screen don't change. One limit: every channel is told the same quantity, so giving a channel a bigger share of scarce stock would need a new allocation rule.
