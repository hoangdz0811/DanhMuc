"""
fetch_prices.py — Chạy bởi GitHub Actions
Lấy giá đóng cửa mới nhất + phân ngành cho các mã CP trong stocks.json,
lưu vào prices.json để trang HTML đọc.
"""

import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from vnstock import Listing, Quote
from supabase import create_client, Client

VN_TZ = timezone(timedelta(hours=7))
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(SCRIPT_DIR, "prices.json")

# Supabase config
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_ANON_KEY")

def load_stock_list():
    """Lấy danh sách các mã cổ phiếu ĐANG CÓ trong bảng portfolios trên Supabase."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("[WARN] Chưa cấu hình URL/KEY Supabase trong Github Secrets. Fallback về file stocks.json cũ.")
        # Fallback cũ
        stocks_file = os.path.join(SCRIPT_DIR, "stocks.json")
        if os.path.exists(stocks_file):
            with open(stocks_file, "r", encoding="utf-8") as f:
                return json.load(f).get("symbols", [])
        return []

    try:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        # Select distict mã cổ phiếu từ profiles
        response = supabase.table("portfolios").select("ma_cp").execute()
        
        # Trích xuất danh sách duy nhất không trùng lặp
        symbols_set = set()
        for row in response.data:
            if row.get("ma_cp"):
                symbols_set.add(row["ma_cp"].upper())
                
        # Trả về list
        return list(symbols_set)
    except Exception as e:
        print(f"[ERROR] Không thể lấy danh sách mã từ Supabase: {e}")
        return []



def fetch_industry_map():
    """Lấy bảng phân ngành ICB cho tất cả mã CP."""
    try:
        ls = Listing()
        df = ls.symbols_by_industries()
        if df is not None and not df.empty:
            return dict(zip(df["symbol"], df["industry_name"]))
    except Exception as e:
        print(f"[WARN] Không lấy được phân ngành: {e}")
    return {}


def fetch_price(symbol: str) -> tuple[str, float | None]:
    """Lấy giá đóng cửa mới nhất của 1 mã (đơn vị VND). Trả về (symbol, price)."""
    try:
        quote = Quote(symbol=symbol, source='VCI')
        df = quote.history(length="1M", interval="1D")
        if df is not None and not df.empty:
            raw_price = float(df["close"].iloc[-1])
            return (symbol, raw_price * 1000)  # VCI trả giá theo đơn vị nghìn VND
    except Exception as e:
        print(f"  [ERROR] {symbol}: {e}")
    return (symbol, None)


def main():
    print("=== Bắt đầu cập nhật giá ===")

    # Đọc danh sách mã CP cần fetch
    symbols = load_stock_list()
    if not symbols:
        print("[ERROR] Không có mã CP nào trong stocks.json")
        sys.exit(1)

    print(f"Danh sách mã CP: {', '.join(symbols)}")

    # Đọc file prices.json cũ (nếu có) để giữ giá cũ khi fetch thất bại
    old_prices = {}
    old_industries = {}
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
                old_data = json.load(f)
                old_prices = old_data.get("prices", {})
                old_industries = old_data.get("industries", {})
        except Exception:
            pass

    # Lấy phân ngành
    print("Đang lấy phân ngành...")
    all_industries = fetch_industry_map()
    # Chỉ giữ phân ngành cho các mã cần thiết
    industries = {}
    for sym in symbols:
        if sym in all_industries:
            industries[sym] = all_industries[sym]
        elif sym in old_industries:
            industries[sym] = old_industries[sym]
    print(f"  → {len(industries)}/{len(symbols)} mã có phân ngành")

    # Fetch giá từng mã (tuần tự, có delay để tránh rate limit)
    print(f"Đang fetch giá cho {len(symbols)} mã...")
    prices = {}
    success = 0
    for i, sym in enumerate(symbols):
        print(f"  [{i+1}/{len(symbols)}] {sym}...", end=" ")
        sym_result, price = fetch_price(sym)
        if price is not None:
            prices[sym] = price
            success += 1
            print(f"✓ {price:,.0f} VND")
        elif sym in old_prices:
            prices[sym] = old_prices[sym]
            print(f"✗ (giữ giá cũ: {old_prices[sym]:,.0f} VND)")
        else:
            print("✗ (không có giá)")

        # Delay giữa các request để tránh rate limit
        if i < len(symbols) - 1:
            time.sleep(1)

    print(f"\n  → Lấy giá thành công: {success}/{len(symbols)}")

    # Tạo output
    output = {
        "updated_at": datetime.now(VN_TZ).strftime("%Y-%m-%dT%H:%M:%S+07:00"),
        "total_symbols": len(prices),
        "prices": prices,
        "industries": industries,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"=== Hoàn tất! Đã lưu {len(prices)} giá vào prices.json ===")


if __name__ == "__main__":
    main()
