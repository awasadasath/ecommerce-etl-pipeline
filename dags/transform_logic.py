import pandas as pd
import logging
import requests
<<<<<<< HEAD
=======
import os
from datetime import datetime
>>>>>>> a4fea9fe09fcec3df45131c3a7d6e7b386cd7bf6
from airflow.models import Variable

log = logging.getLogger(__name__)

def send_discord_warning(msg_content):
    """ฟังก์ชันส่ง Warning เข้า Discord"""
    webhook = Variable.get("discord_webhook", default_var=None)
    if webhook:
        try:
            data = {"username": "Airflow Data Quality", "content": msg_content}
            requests.post(webhook, json=data, timeout=5)
        except Exception as e:
            log.error(f"Discord Warning Error: {e}")

def run_transform_and_clean(mysql_file, api_file, output_path):
    """ฟังก์ชันหลักสำหรับ Merge และ Clean Data"""
    log.info("Starting Transformation Logic...")
    
    # 1. Read Data
    tx_df = pd.read_parquet(mysql_file)
    rate_df = pd.read_parquet(api_file)

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
    final_df = final_df[target_columns]

    # DATA QUALITY CHECKS
    
    # 1. เก็บค่าเริ่มต้น
    initial_rows = len(final_df)
    dup_count = 0
    
    # 2. Check Duplicates
    if final_df.duplicated(subset=['transaction_id', 'product_id']).any():
        dup_count = final_df.duplicated(subset=['transaction_id', 'product_id']).sum()
        final_df = final_df.drop_duplicates(subset=['transaction_id', 'product_id'], keep='first')
        log.warning(f"Found {dup_count} duplicates.")

    rows_after_dedup = len(final_df) 

    # 3. Filter Bad Data
    condition_good = (
        (final_df['price'] >= 0) &                 
        (final_df['transaction_id'].notnull()) &   
        (final_df['date'].notnull())               
    )
    final_df = final_df[condition_good]

    # 4. Summary
    cleaned_rows = len(final_df)
    removed_bad_rows = rows_after_dedup - cleaned_rows
    dq_msg = f"""
📊 **DQ Process Summary**
------------------------
📥 **Initial Input:** {initial_rows}
⚠️ Duplicates Dropped: {dup_count}
🗑️ Bad Rows Removed: {removed_bad_rows}
✅ **Final Rows:** {cleaned_rows}
    """
    
    # Send Message to Discord
    send_discord_warning(dq_msg)
    
    log.info(f"✅ Data Cleaned. {cleaned_rows} rows ready for Upload.")
    
    # 5. Save Output
    log.info(f"Saving to {output_path}...")
    final_df.to_parquet(output_path, index=False)
    
    return output_path