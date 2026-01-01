import pandas as pd
import logging
import requests
import os
from datetime import datetime
from airflow.models import Variable

log = logging.getLogger(__name__)

FINAL_OUTPUT_FILE = "/tmp/final_data.parquet"

def send_discord_warning(msg_content):
    """ฟังก์ชันส่ง Warning เข้า Discord """
    if Variable:
        webhook = Variable.get("discord_webhook", default_var=None)
        if webhook:
            try:
                data = {"username": "Airflow Data Quality", "content": msg_content}
                requests.post(webhook, json=data, timeout=5)
            except Exception as e:
                log.error(f"Discord Warning Error: {e}")

def run_transform_and_clean(mysql_file, api_file):
    """ฟังก์ชันหลักสำหรับ Merge และ Clean Data"""
    log.info("Starting Transformation Logic...")
    
    # 1. Read Data
    tx_df = pd.read_parquet(mysql_file)
    rate_df = pd.read_parquet(api_file)

    if tx_df.empty:
        log.warning("Transaction Data is empty!")
        tx_df.to_parquet(FINAL_OUTPUT_FILE, index=False)
        return FINAL_OUTPUT_FILE

    # 2. Merge Data
    final_df = tx_df.merge(rate_df, how="left", left_on="join_date", right_on="date")
    
    # Fill NA & Calculate
    final_df['gbp_thb'] = final_df['gbp_thb'].fillna(42.0)
    final_df['thb_amount'] = final_df['total_amount'] * final_df['gbp_thb']
    
    # Rename & Format Date
    final_df = final_df.rename(columns={'date_x': 'date'})
    if pd.api.types.is_datetime64_any_dtype(final_df['date']):
        final_df['date'] = final_df['date'].dt.strftime('%Y-%m-%d')
        
    # Select Columns
    target_columns = [
        'transaction_id', 'date', 'product_id', 'price', 'quantity', 
        'customer_id', 'product_name', 'customer_country', 'customer_name', 
        'total_amount', 'thb_amount'
    ]
    final_df = final_df[[c for c in target_columns if c in final_df.columns]]

    # DATA QUALITY CHECKS
    initial_rows = len(final_df)

    # Check 1: Duplicates (Line Item)
    if final_df.duplicated(subset=['transaction_id', 'product_id']).any():
        dup_count = final_df.duplicated(subset=['transaction_id', 'product_id']).sum()
        msg = f"⚠️ DQ Warning: Found {dup_count} duplicate line items. Deduplicating..."
        log.warning(msg)
        send_discord_warning(f"🟠 **DQ WARNING:** {msg}")
        final_df.drop_duplicates(subset=['transaction_id', 'product_id'], keep='first', inplace=True)

    # Check 2: Quantity > 0
    final_df = final_df[final_df['quantity'] > 0]

    # Check 3: Negative Amount & Null Date
    final_df = final_df[final_df['thb_amount'] >= 0]
    final_df = final_df[final_df['date'].notnull()]

    # Clean Summary
    cleaned_rows = len(final_df)
    removed_rows = initial_rows - cleaned_rows
    
    if removed_rows > 0:
        msg = f"⚠️ Cleaned Data: Removed {removed_rows} bad rows (Negative amount / Zero Qty / Nulls)."
        log.warning(msg)
        send_discord_warning(f"🟠 **DQ WARNING:** {msg}")

    log.info(f"✅ Data Cleaned. {cleaned_rows} rows ready for Upload.")
    
    # 3. Save Output
    final_df.to_parquet(FINAL_OUTPUT_FILE, index=False)
    
    return FINAL_OUTPUT_FILE
