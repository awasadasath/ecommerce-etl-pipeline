import pandas as pd
import pytest

def test_data_cleaning_logic():
    """
    ทดสอบ Logic การกรองข้อมูล:
    1. ต้องกรองยอดติดลบออก
    2. ต้องกรอง Quantity ที่เป็น 0 หรือติดลบออก
    """
    # 1. Mock Data (จำลองข้อมูลดิบที่มีของเสีย)
    data = {
        'transaction_id': [1, 2, 3, 4],
        'date': ['2025-01-01', '2025-01-01', '2025-01-01', '2025-01-01'],
        'thb_amount': [100.0, -50.0, 200.0, 50.0],  # แถว 2 เสีย (ติดลบ)
        'quantity':   [1, 1, 0, -5]                 # แถว 3, 4 เสีย (0 และติดลบ)
    }
    df = pd.DataFrame(data)

    # 2. Apply Logic
    # กรองยอดเงิน
    df = df[df['thb_amount'] >= 0]
    # กรอง Quantity
    df = df[df['quantity'] > 0]

    # 3. Assertions (ยืนยันผลลัพธ์)
    # จาก 4 แถว ต้องเหลือแค่แถวแรกแถวเดียวที่สมบูรณ์
    assert len(df) == 1 
    assert df.iloc[0]['transaction_id'] == 1
    
    # ต้องไม่มีค่าติดลบเหลือ
    assert (df['thb_amount'] < 0).sum() == 0
    assert (df['quantity'] <= 0).sum() == 0
