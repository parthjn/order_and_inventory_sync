# Stock reservation pseudo-code

Companion to section 4 of [DESIGN.md](../DESIGN.md). Optional reading, the walkthrough in the main doc stands on its own.

## Reserving stock for an order

```
reserve(order):
  lock the order row (sales_channel_id, external_order_id)
  if order is not new: return                    -- replay, nothing to do
  for each item, sorted by sku:                  -- fixed order avoids deadlocks
    if a live HELD reservation exists for this checkout (website only):
      mark it CONFIRMED; continue
    n = UPDATE warehouse_inventory SET reserved = reserved + :qty
        WHERE sku = :sku AND on_hand - reserved >= :qty
    if n == 0: rollback, mark order OVERSOLD, alert, return
    INSERT reservations (status = 'CONFIRMED', ...)
    INSERT inventory_movements (delta_reserved = +qty, reason = 'RESERVE', reservation_id, ...)
    UPDATE channel_inventory SET desired_quantity = <new exposed qty>
        for every enabled channel
  order.status = CONFIRMED
  INSERT outbox_events (ERP_DELIVERY_REQUIRED, order_id)
  INSERT outbox_events (STOCK_SYNC_REQUIRED, sku)
  INSERT erp_deliveries (order_id, status = 'pending')
```

## Website hold (checkout) and expiry

```
hold(checkout):                                  -- website, when checkout starts
  for each item, sorted by sku:
    n = same conditional UPDATE as above
    if n == 0: tell the customer it is out of stock; release anything already held
    INSERT reservations (status = 'HELD', expires_at = now + hold_time)
    INSERT inventory_movements (delta_reserved = +qty, reason = 'RESERVE', ...)

expire_holds():                                  -- sweeper, runs every minute
  for each HELD reservation past expires_at:
    UPDATE warehouse_inventory SET reserved = reserved - qty
    mark the reservation EXPIRED
    INSERT inventory_movements (delta_reserved = -qty, reason = 'EXPIRE', ...)
```
