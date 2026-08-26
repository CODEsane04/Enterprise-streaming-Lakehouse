"""
Time-Dilated Olist Data Mutator
Engineer 1 — Enterprise Agentic Streaming Lakehouse

Replays the Olist CSV dataset as live database mutations so Debezium
captures INSERT, UPDATE, and DELETE events via PostgreSQL WAL.

Flow per order:
  1. INSERT customer (if new)
  2. INSERT products + sellers (if new)
  3. INSERT order with status='processing'
  4. INSERT order_items + order_payments
  5. Wait DELAY seconds -> UPDATE order to 'approved'
  6. Wait DELAY seconds -> UPDATE order to 'shipped'
  7. Wait DELAY seconds -> UPDATE order to 'delivered'
  8. Periodically inject anomalies for Flink DLQ testing
"""

import os
import time
import pandas as pd
from sqlalchemy import create_engine, text

# ── Config ────────────────────────────────────────────────────
DB_URL    = "postgresql+psycopg2://postgres:postgres@localhost:5433/olist_ecommerce"
DATA_DIR  = r"d:\DE project\data\olist"
DELAY     = 2.0   # seconds between status transitions
MAX_ORDERS = 200  # how many orders to simulate (set None for all)
# ─────────────────────────────────────────────────────────────

engine = create_engine(DB_URL, echo=False)

# ── Track what we've already inserted to avoid duplicate PK errors ──
inserted_customers = set()
inserted_products  = set()
inserted_sellers   = set()

def safe_insert(df, table, engine, dedup_col=None, inserted_set=None):
    """Insert rows, skipping already-inserted PKs to avoid duplicate errors."""
    if df.empty:
        return
    if dedup_col and inserted_set is not None:
        df = df[~df[dedup_col].isin(inserted_set)]
        if df.empty:
            return
        inserted_set.update(df[dedup_col].tolist())

    # Use INSERT ... ON CONFLICT DO NOTHING via raw SQL per row
    df.to_sql(table, engine, if_exists="append", index=False,
              method="multi", chunksize=500)

def load_data():
    print("Loading CSV files into memory...")

    orders     = pd.read_csv(os.path.join(DATA_DIR, "olist_orders_dataset.csv"))
    order_items = pd.read_csv(os.path.join(DATA_DIR, "olist_order_items_dataset.csv"))
    payments   = pd.read_csv(os.path.join(DATA_DIR, "olist_order_payments_dataset.csv"))
    customers  = pd.read_csv(os.path.join(DATA_DIR, "olist_customers_dataset.csv"))
    products   = pd.read_csv(os.path.join(DATA_DIR, "olist_products_dataset.csv"))
    sellers    = pd.read_csv(os.path.join(DATA_DIR, "olist_sellers_dataset.csv"))
    translation = pd.read_csv(os.path.join(DATA_DIR, "product_category_name_translation.csv"))

    # Rename CSV columns to match our DB schema
    customers = customers.rename(columns={
        "customer_zip_code_prefix": "customer_zip_code_prefix"  # already matches
    })

    # Parse timestamps
    ts_cols = [
        "order_purchase_timestamp", "order_approved_at",
        "order_delivered_carrier_date", "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ]
    for col in ts_cols:
        if col in orders.columns:
            orders[col] = pd.to_datetime(orders[col], errors="coerce")

    # Sort chronologically — simulate real passage of time
    orders = orders.sort_values("order_purchase_timestamp").reset_index(drop=True)

    if MAX_ORDERS:
        orders = orders.head(MAX_ORDERS)

    print(f"Loaded {len(orders)} orders to simulate.")
    return orders, order_items, payments, customers, products, sellers, translation

def seed_static_data(customers, products, sellers, translation, engine):
    """
    Load static lookup tables once at startup.
    We load ALL customers, products, sellers upfront so FK constraints
    are satisfied when we later insert orders.
    """
    print("Seeding static lookup data (customers, products, sellers, translations)...")

    with engine.begin() as conn:
        # Translation table (no duplicates expected)
        for _, row in translation.iterrows():
            conn.execute(text("""
                INSERT INTO product_category_name_translation
                    (product_category_name, product_category_name_english)
                VALUES (:pt, :en)
                ON CONFLICT DO NOTHING
            """), {"pt": row["product_category_name"], "en": row["product_category_name_english"]})

        # Products
        for _, row in products.iterrows():
            conn.execute(text("""
                INSERT INTO products
                    (product_id, product_category_name, product_name_length,
                     product_description_length, product_photos_qty,
                     product_weight_g, product_length_cm, product_height_cm,
                     product_width_cm)
                VALUES
                    (:id, :cat, :name_len, :desc_len, :photos,
                     :weight, :length, :height, :width)
                ON CONFLICT DO NOTHING
            """), {
                "id":       row["product_id"],
                "cat":      row.get("product_category_name", None),
                "name_len": _int(row.get("product_name_length")),
                "desc_len": _int(row.get("product_description_length")),
                "photos":   _int(row.get("product_photos_qty")),
                "weight":   _int(row.get("product_weight_g")),
                "length":   _int(row.get("product_length_cm")),
                "height":   _int(row.get("product_height_cm")),
                "width":    _int(row.get("product_width_cm")),
            })

        # Sellers
        for _, row in sellers.iterrows():
            conn.execute(text("""
                INSERT INTO sellers
                    (seller_id, seller_zip_code_prefix, seller_city, seller_state)
                VALUES (:id, :zip, :city, :state)
                ON CONFLICT DO NOTHING
            """), {
                "id":   row["seller_id"],
                "zip":  str(row["seller_zip_code_prefix"]),
                "city": row["seller_city"],
                "state": row["seller_state"],
            })

        # Customers
        for _, row in customers.iterrows():
            conn.execute(text("""
                INSERT INTO customers
                    (customer_id, customer_unique_id, customer_zip_code_prefix,
                     customer_city, customer_state)
                VALUES (:id, :uid, :zip, :city, :state)
                ON CONFLICT DO NOTHING
            """), {
                "id":   row["customer_id"],
                "uid":  row["customer_unique_id"],
                "zip":  str(row["customer_zip_code_prefix"]),
                "city": row["customer_city"],
                "state": row["customer_state"],
            })

    print("Static seed complete.")

def run_simulation(orders, order_items, payments, engine):
    print("\nStarting time-dilated simulation. Watch Redpanda Console at http://localhost:8088\n")
    print("-" * 60)

    for i, (_, order) in enumerate(orders.iterrows()):
        order_id    = order["order_id"]
        customer_id = order["customer_id"]

        print(f"[{i+1}/{len(orders)}] Order: {order_id[:16]}...")

        # ── Step 1: INSERT order as 'processing' ──────────────
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO orders (
                    order_id, customer_id, order_status,
                    order_purchase_timestamp, order_approved_at,
                    order_delivered_carrier_date, order_delivered_customer_date,
                    order_estimated_delivery_date
                ) VALUES (
                    :order_id, :customer_id, 'processing',
                    :purchase_ts, NULL, NULL, NULL, :est_delivery
                )
                ON CONFLICT DO NOTHING
            """), {
                "order_id":    order_id,
                "customer_id": customer_id,
                "purchase_ts": order["order_purchase_timestamp"],
                "est_delivery": _ts(order.get("order_estimated_delivery_date")),
            })
        print(f"  -> INSERT  status=processing")

        # ── Step 2: INSERT order_items ─────────────────────────
        items = order_items[order_items["order_id"] == order_id]
        if not items.empty:
            with engine.begin() as conn:
                for _, item in items.iterrows():
                    conn.execute(text("""
                        INSERT INTO order_items (
                            order_id, order_item_id, product_id, seller_id,
                            shipping_limit_date, price, freight_value
                        ) VALUES (
                            :order_id, :item_id, :product_id, :seller_id,
                            :ship_date, :price, :freight
                        )
                        ON CONFLICT DO NOTHING
                    """), {
                        "order_id":   order_id,
                        "item_id":    int(item["order_item_id"]),
                        "product_id": item["product_id"],
                        "seller_id":  item["seller_id"],
                        "ship_date":  _ts(item.get("shipping_limit_date")),
                        "price":      _float(item.get("price")),
                        "freight":    _float(item.get("freight_value")),
                    })
            print(f"  -> INSERT  {len(items)} item(s)")

        # ── Step 3: INSERT payments ────────────────────────────
        pmts = payments[payments["order_id"] == order_id]
        if not pmts.empty:
            with engine.begin() as conn:
                for _, pmt in pmts.iterrows():
                    conn.execute(text("""
                        INSERT INTO order_payments (
                            order_id, payment_sequential, payment_type,
                            payment_installments, payment_value
                        ) VALUES (
                            :order_id, :seq, :ptype, :installments, :value
                        )
                        ON CONFLICT DO NOTHING
                    """), {
                        "order_id":     order_id,
                        "seq":          int(pmt["payment_sequential"]),
                        "ptype":        pmt["payment_type"],
                        "installments": int(pmt.get("payment_installments", 1)),
                        "value":        float(pmt["payment_value"]),
                    })

        # ── Inject anomaly every 10th order (for Flink DLQ) ───
        if (i + 1) % 10 == 0:
            _inject_anomaly(engine, order_id)

        # ── Step 4: UPDATE -> approved ─────────────────────────
        time.sleep(DELAY)
        with engine.begin() as conn:
            conn.execute(text(
                "UPDATE orders SET order_status='approved', updated_at=NOW() WHERE order_id=:oid"
            ), {"oid": order_id})
        print(f"  -> UPDATE  status=approved")

        # ── Step 5: UPDATE -> shipped ──────────────────────────
        time.sleep(DELAY)
        with engine.begin() as conn:
            conn.execute(text(
                "UPDATE orders SET order_status='shipped', updated_at=NOW() WHERE order_id=:oid"
            ), {"oid": order_id})
        print(f"  -> UPDATE  status=shipped")

        # ── Step 6: UPDATE -> delivered ────────────────────────
        time.sleep(DELAY)
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE orders
                SET order_status='delivered',
                    order_delivered_customer_date=NOW(),
                    updated_at=NOW()
                WHERE order_id=:oid
            """), {"oid": order_id})
        print(f"  -> UPDATE  status=delivered")
        print()

    print("Simulation complete!")


def _inject_anomaly(engine, order_id):
    """
    Deliberately write a bad order_items record with a negative price.
    Engineer 2's Flink SQL will catch this and route it to the DLQ topic.
    """
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO order_items (
                order_id, order_item_id, product_id, seller_id,
                shipping_limit_date, price, freight_value
            )
            SELECT :order_id, 99, product_id, seller_id, NULL, -999.99, NULL
            FROM order_items WHERE order_id = :order_id LIMIT 1
            ON CONFLICT DO NOTHING
        """), {"order_id": order_id})
    print(f"  !! ANOMALY injected (negative price) for Flink DLQ testing")


# ── Helpers ───────────────────────────────────────────────────
def _int(val):
    try:
        v = int(val)
        return None if pd.isna(val) else v
    except:
        return None

def _float(val):
    try:
        v = float(val)
        return None if pd.isna(val) else v
    except:
        return None

def _ts(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    return pd.to_datetime(val, errors="coerce")


# ── Entry Point ───────────────────────────────────────────────
if __name__ == "__main__":
    orders, order_items, payments, customers, products, sellers, translation = load_data()
    seed_static_data(customers, products, sellers, translation, engine)
    run_simulation(orders, order_items, payments, engine)
