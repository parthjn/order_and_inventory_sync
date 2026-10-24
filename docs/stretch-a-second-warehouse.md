# Stretch A: second warehouse

Companion to [DESIGN.md](../DESIGN.md).

`warehouse_inventory` becomes keyed by `(warehouse_id, sku)` and `inventory_movements` gets a `warehouse_id`. The reserve step picks a warehouse (nearest to the shipping pincode, falling back to the other) inside the same conditional-update transaction, and the reservation row records which one was used. What channels see is the sum across warehouses, unless a channel supports per-warehouse stock. ERP orders carry a location code. Splitting one order across warehouses is out. The change is mostly in the reserve step and the `desired_quantity` sum.
