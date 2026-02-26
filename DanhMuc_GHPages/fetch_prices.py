"""
fetch_prices.py — Chạy bởi GitHub Actions
Lấy giá đóng cửa mới nhất + phân ngành cho tất cả mã CP trên sàn,
lưu vào prices.json để trang HTML đọc.

Sử dụng ThreadPoolExecutor để fetch song song, tăng tốc đáng kể.
"""

import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from vnstock import Listing, Quote

VN_TZ = timezone(timedelta(hours=7))
OUTPUT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "prices.json")
MAX_WORKERS = 10  # Số luồng song song


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
    except Exception:
        pass
    return (symbol, None)


def main():
    print("=== Bắt đầu cập nhật giá ===")

    # Đọc file prices.json cũ (nếu có) để giữ giá cũ khi fetch thất bại
    old_prices = {}
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
                old_data = json.load(f)
                old_prices = old_data.get("prices", {})
        except Exception:
            pass

    # Lấy phân ngành
    print("Đang lấy phân ngành...")
    industries = fetch_industry_map()
    print(f"  → {len(industries)} mã có phân ngành")

    # Danh sách mã CP cần fetch giá
    symbols = list(industries.keys()) if industries else []
    if not symbols:
        print("[ERROR] Không có danh sách mã CP")
        sys.exit(1)

    print(f"Tổng số mã CP: {len(symbols)}")
    print(f"Đang fetch giá song song ({MAX_WORKERS} luồng)...")

    # Fetch giá song song bằng ThreadPoolExecutor
    prices = {}
    success = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_price, sym): sym for sym in symbols}
        done_count = 0
        for future in as_completed(futures):
            done_count += 1
            sym, price = future.result()
            if price is not None:
                prices[sym] = price
                success += 1
            elif sym in old_prices:
                prices[sym] = old_prices[sym]

            if done_count % 100 == 0:
                print(f"  Đã xử lý {done_count}/{len(symbols)}...")

    print(f"  → Lấy giá thành công: {success}/{len(symbols)}")

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
