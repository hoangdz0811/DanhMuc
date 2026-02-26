"""
fetch_prices.py — Chạy bởi GitHub Actions
Lấy giá đóng cửa mới nhất + phân ngành cho TẤT CẢ mã CP trên sàn,
lưu vào prices.json để trang HTML đọc.

Xử lý rate limit bằng cách fetch tuần tự + retry khi bị giới hạn.
"""

import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from vnstock import Listing, Quote

VN_TZ = timezone(timedelta(hours=7))
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(SCRIPT_DIR, "prices.json")

# Config
DELAY_BETWEEN_REQUESTS = 1.5  # Giây giữa mỗi request
RATE_LIMIT_WAIT = 65          # Giây chờ khi bị rate limit
MAX_RETRIES = 3               # Số lần retry tối đa cho mỗi mã


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


def fetch_price(symbol: str, retry=0) -> tuple[str, float | None]:
    """Lấy giá đóng cửa mới nhất của 1 mã (đơn vị VND)."""
    try:
        quote = Quote(symbol=symbol, source='VCI')
        df = quote.history(length="1M", interval="1D")
        if df is not None and not df.empty:
            raw_price = float(df["close"].iloc[-1])
            return (symbol, raw_price * 1000)
    except Exception as e:
        error_msg = str(e).lower()
        # Nếu bị rate limit, chờ rồi retry
        if "rate limit" in error_msg or "429" in error_msg or "giới hạn" in error_msg:
            if retry < MAX_RETRIES:
                print(f"  ⏳ Rate limit! Chờ {RATE_LIMIT_WAIT}s rồi retry ({retry+1}/{MAX_RETRIES})...")
                time.sleep(RATE_LIMIT_WAIT)
                return fetch_price(symbol, retry + 1)
            else:
                print(f"  ✗ {symbol}: vẫn bị rate limit sau {MAX_RETRIES} lần retry")
        else:
            print(f"  ✗ {symbol}: {e}")
    return (symbol, None)


def main():
    print("=== Bắt đầu cập nhật giá ===")
    start_time = time.time()

    # Đọc file prices.json cũ (nếu có)
    old_prices = {}
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
                old_data = json.load(f)
                old_prices = old_data.get("prices", {})
        except Exception:
            pass

    # Lấy phân ngành (1 API call cho tất cả mã)
    print("Đang lấy phân ngành...")
    industries = fetch_industry_map()
    print(f"  → {len(industries)} mã có phân ngành")

    symbols = list(industries.keys()) if industries else []
    if not symbols:
        print("[ERROR] Không có danh sách mã CP")
        sys.exit(1)

    # Fetch giá tuần tự với delay
    print(f"Tổng số mã CP: {len(symbols)}")
    print(f"Fetch tuần tự (delay {DELAY_BETWEEN_REQUESTS}s giữa mỗi request)...")
    print(f"Ước tính thời gian: ~{len(symbols) * DELAY_BETWEEN_REQUESTS / 60:.0f} phút\n")

    prices = {}
    success = 0
    rate_limited = 0

    for i, sym in enumerate(symbols):
        sym_result, price = fetch_price(sym)

        if price is not None:
            prices[sym] = price
            success += 1
        elif sym in old_prices:
            # Giữ giá cũ nếu fetch thất bại
            prices[sym] = old_prices[sym]

        # Log tiến trình mỗi 50 mã
        if (i + 1) % 50 == 0:
            elapsed = time.time() - start_time
            print(f"  → {i+1}/{len(symbols)} ({success} thành công, {elapsed:.0f}s)")

        # Delay giữa các request
        if i < len(symbols) - 1:
            time.sleep(DELAY_BETWEEN_REQUESTS)

    elapsed = time.time() - start_time
    print(f"\n  → Lấy giá thành công: {success}/{len(symbols)} ({elapsed:.0f}s)")

    # Tạo output
    output = {
        "updated_at": datetime.now(VN_TZ).strftime("%Y-%m-%dT%H:%M:%S+07:00"),
        "total_symbols": len(prices),
        "prices": prices,
        "industries": industries,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"=== Hoàn tất! Đã lưu {len(prices)} giá vào prices.json ({elapsed:.0f}s) ===")


if __name__ == "__main__":
    main()
