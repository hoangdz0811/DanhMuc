import streamlit as st
import pandas as pd
import json
import os
import base64
from pathlib import Path
from datetime import datetime, date
from vnstock import Quote, Listing

# ============================================================
# CẤU HÌNH TRANG
# ============================================================
st.set_page_config(
    page_title="Danh Mục Đầu Tư - KAFI SAIGON",
    page_icon="📊",
    layout="wide",
)

# ============================================================
# CSS GIAO DIỆN DARK THEME + GLASSMORPHISM
# ============================================================
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&display=swap');

    /* ===== ANIMATIONS ===== */
    @keyframes fadeInUp {
        from { opacity: 0; transform: translateY(16px); }
        to   { opacity: 1; transform: translateY(0); }
    }
    @keyframes shimmer {
        0%   { background-position: -200% 0; }
        100% { background-position: 200% 0; }
    }

    /* ===== TOÀN BỘ TRANG ===== */
    .stApp {
        background: #ffffff !important;
        font-family: 'Inter', sans-serif;
    }

    /* ===== HEADER ===== */
    .main-header {
        text-align: center;
        padding: 24px 0 12px 0;
        animation: fadeInUp 0.5s ease-out;
        position: relative;
    }
    .main-header .logo-img {
        position: absolute;
        left: 0;
        top: 0;
        height: 210px;
    }
    .main-header h1 {
        font-size: 2rem;
        font-weight: 900;
        background: linear-gradient(135deg, #00897B 0%, #26A69A 40%, #4DB6AC 70%, #00897B 100%);
        background-size: 200% auto;
        animation: shimmer 4s linear infinite;
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        letter-spacing: 1.5px;
        margin: 0;
    }
    .main-header .sub {
        color: #78909C;
        font-size: 0.82rem;
        font-weight: 500;
        letter-spacing: 2.5px;
        text-transform: uppercase;
        margin-top: 6px;
    }
    .main-header .divider {
        width: 60px;
        height: 3px;
        background: linear-gradient(90deg, transparent, #00897B, transparent);
        margin: 12px auto 0;
        border-radius: 2px;
    }

    /* ===== TABLE CARD (chứa bảng) ===== */
    .glass-card {
        background: white;
        border: 1px solid #e0e0e0;
        border-radius: 16px;
        padding: 0;
        margin-bottom: 20px;
        box-shadow: 0 2px 12px rgba(0,0,0,0.06);
        overflow: hidden;
        animation: fadeInUp 0.6s ease-out;
    }

    /* ===== KPI CARDS ===== */
    .kpi-row {
        display: grid;
        grid-template-columns: repeat(4, 1fr);
        gap: 16px;
        margin-bottom: 24px;
        animation: fadeInUp 0.5s ease-out;
    }
    .kpi-card {
        background: linear-gradient(145deg, #00897B 0%, #00796B 100%);
        border: none;
        border-radius: 16px;
        padding: 22px 18px 18px;
        text-align: center;
        position: relative;
        overflow: hidden;
        transition: all 0.35s cubic-bezier(0.4, 0, 0.2, 1);
        box-shadow: 0 4px 16px rgba(0,137,123,0.25);
    }
    .kpi-card::before {
        content: '';
        position: absolute;
        top: 0; left: 0; right: 0;
        height: 3px;
        background: linear-gradient(90deg, transparent, rgba(255,255,255,0.5), transparent);
    }
    .kpi-card:hover {
        transform: translateY(-4px);
        box-shadow: 0 8px 30px rgba(0,137,123,0.35);
    }
    .kpi-card .kpi-title-row {
        display: flex;
        align-items: center;
        justify-content: center;
        gap: 8px;
        margin-bottom: 10px;
    }
    .kpi-card .kpi-icon {
        font-size: 1.1rem;
        line-height: 1;
    }
    .kpi-card .label {
        color: rgba(255,255,255,0.9);
        font-size: 1rem;
        font-weight: 700;
        letter-spacing: 0.5px;
    }
    .kpi-card .value {
        font-size: 1.6rem;
        font-weight: 800;
        line-height: 1;
    }
    .kpi-card .value.positive { color: #B9F6CA; }
    .kpi-card .value.negative { color: #FF8A80; }
    .kpi-card .value.neutral  { color: #ffffff; }

    /* ===== BẢNG DỮ LIỆU ===== */
    .portfolio-table {
        width: 100%;
        border-collapse: collapse;
    }
    .portfolio-table thead th {
        background: #00897B;
        color: white;
        font-weight: 700;
        font-size: 0.73rem;
        text-transform: uppercase;
        letter-spacing: 1px;
        padding: 14px 16px;
        text-align: center;
    }
    .portfolio-table tbody td {
        padding: 16px 16px;
        color: #37474F;
        font-size: 0.93rem;
        text-align: center;
        border-bottom: 1px solid #f0f0f0;
        font-weight: 500;
    }
    .portfolio-table tbody tr {
        transition: background 0.2s;
    }
    .portfolio-table tbody tr:nth-child(even) {
        background: #f9fdf9;
    }
    .portfolio-table tbody tr:hover {
        background: #e8f5e9;
    }
    .portfolio-table .symbol {
        font-weight: 800;
        color: #00897B;
        font-size: 1.05rem;
        letter-spacing: 0.5px;
    }
    .portfolio-table .profit-positive {
        color: #2E7D32;
        font-weight: 800;
    }
    .portfolio-table .profit-negative {
        color: #D32F2F;
        font-weight: 800;
    }

    /* ===== SIDEBAR ===== */
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #00897B 0%, #00796B 100%) !important;
        border-right: none;
    }
    section[data-testid="stSidebar"] .stMarkdown h2 {
        color: white;
        font-weight: 700;
        font-size: 1rem;
        letter-spacing: 0.5px;
    }
    section[data-testid="stSidebar"] label {
        color: rgba(255,255,255,0.85) !important;
        font-size: 0.82rem !important;
        font-weight: 500 !important;
    }
    section[data-testid="stSidebar"] p,
    section[data-testid="stSidebar"] span {
        color: rgba(255,255,255,0.9) !important;
    }

    /* ===== FORM INPUTS ===== */
    .stNumberInput input, .stTextInput input, .stDateInput input {
        background: white !important;
        border: 1px solid #e0e0e0 !important;
        border-radius: 10px !important;
        color: #37474F !important;
        font-weight: 500 !important;
    }
    .stNumberInput input:focus, .stTextInput input:focus, .stDateInput input:focus {
        border-color: #00897B !important;
        box-shadow: 0 0 8px rgba(0,137,123,0.15) !important;
    }

    /* ===== NÚT BẤM ===== */
    .stButton > button {
        background: linear-gradient(135deg, #00897B 0%, #26A69A 100%);
        color: white;
        border: none;
        border-radius: 12px;
        padding: 10px 24px;
        font-weight: 700;
        font-size: 0.88rem;
        letter-spacing: 0.3px;
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        box-shadow: 0 3px 12px rgba(0,137,123,0.25);
    }
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 24px rgba(0,137,123,0.35);
    }
    .stButton > button:active {
        transform: translateY(0);
    }

    /* ===== TIMESTAMP ===== */
    .timestamp {
        text-align: center;
        color: #90A4AE;
        font-size: 0.75rem;
        font-weight: 400;
        letter-spacing: 0.5px;
        margin-top: 20px;
        padding-top: 16px;
        border-top: 1px solid #e0e0e0;
    }

    /* ===== FORM STYLING ===== */
    [data-testid="stForm"] {
        background: white !important;
        border: 1px solid #e0e0e0 !important;
        border-radius: 14px !important;
        padding: 20px !important;
        box-shadow: 0 2px 8px rgba(0,0,0,0.04) !important;
    }

    /* Ẩn hamburger menu & footer mặc định */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# ============================================================
# DỮ LIỆU - LƯU/ĐỌC JSON
# ============================================================
DATA_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "portfolio_data.json")

DEFAULT_PORTFOLIO = [
    {"ngay_mua": "2026-02-03", "ma_cp": "MWG", "gia_von": 76000, "ty_trong": 25},
    {"ngay_mua": "2026-02-04", "ma_cp": "ANV", "gia_von": 26000, "ty_trong": 25},
    {"ngay_mua": "2026-02-05", "ma_cp": "BMP", "gia_von": 145000, "ty_trong": 25},
]


def load_portfolio():
    """Đọc danh mục từ file JSON, nếu không có thì dùng mặc định."""
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return DEFAULT_PORTFOLIO.copy()


def save_portfolio(data):
    """Lưu danh mục ra file JSON."""
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ============================================================
# DỮ LIỆU - VỊ THẾ ĐÃ ĐÓNG (Chốt lời / Cắt lỗ)
# ============================================================
CLOSED_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "closed_positions.json")


def load_closed():
    """Đọc danh sách vị thế đã đóng."""
    if os.path.exists(CLOSED_FILE):
        with open(CLOSED_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return []


def save_closed(data):
    """Lưu danh sách vị thế đã đóng."""
    with open(CLOSED_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ============================================================
# LẤY GIÁ THỊ TRƯỜNG TỪ VNSTOCK
# ============================================================
@st.cache_data(ttl=300, show_spinner=False)
def get_market_price(symbol: str) -> float | None:
    """Lấy giá đóng cửa mới nhất của 1 mã cổ phiếu (đơn vị: VND)."""
    try:
        quote = Quote(symbol=symbol, source='VCI')
        df = quote.history(length="1M", interval="1D")
        if df is not None and not df.empty:
            # vnstock trả giá theo đơn vị nghìn VND (VD: 92.6 = 92,600 VND)
            raw_price = float(df["close"].iloc[-1])
            return raw_price * 1000
    except Exception:
        pass
    return None


@st.cache_data(ttl=86400, show_spinner=False)
def get_industry_map() -> dict:
    """Lấy bảng phân ngành ICB cấp 2 cho tất cả mã CP (cache 1 ngày)."""
    try:
        ls = Listing()
        df = ls.symbols_by_industries()
        if df is not None and not df.empty:
            return dict(zip(df["symbol"], df["industry_name"]))
    except Exception:
        pass
    return {}


# ============================================================
# SESSION STATE
# ============================================================
if "portfolio" not in st.session_state:
    st.session_state.portfolio = load_portfolio()
if "closed_positions" not in st.session_state:
    st.session_state.closed_positions = load_closed()

# ============================================================
# MAIN CONTENT
# ============================================================
# Load logo
LOGO_PATH = Path("D:/Python file/Twise/Logo/logo.png")
logo_b64 = ""
if LOGO_PATH.exists():
    logo_b64 = base64.b64encode(LOGO_PATH.read_bytes()).decode()

# Nút trên cùng: Thêm CP + Cập nhật giá
_, col_add, col_refresh = st.columns([3, 1, 1])
with col_add:
    add_clicked = st.button("➕ Thêm cổ phiếu", use_container_width=True)
with col_refresh:
    refresh = st.button("🔄 Cập nhật giá thị trường", use_container_width=True)

if refresh:
    st.cache_data.clear()

# Dialog thêm cổ phiếu
@st.dialog("➕ Thêm cổ phiếu mới")
def add_stock_dialog():
    col_s1, col_s2 = st.columns(2)
    with col_s1:
        new_symbol = st.text_input("Mã CP", placeholder="VD: FPT").upper().strip()
    with col_s2:
        new_date = st.date_input("Ngày mua lần 1", value=date.today(), format="DD/MM/YYYY")
    col_s3, col_s4 = st.columns(2)
    with col_s3:
        new_price = st.number_input("Giá vốn lần 1 (₫)", min_value=0, step=1000, value=0)
    with col_s4:
        new_weight = st.number_input("Tỷ trọng (%)", min_value=0, max_value=100, step=5, value=25)

    # Mua lần 2 (tuỳ chọn)
    buy_twice = st.checkbox("🔄 Mua 2 lần")
    new_date_2 = None
    new_price_2 = 0
    if buy_twice:
        col_d2, col_p2 = st.columns(2)
        with col_d2:
            new_date_2 = st.date_input("Ngày mua lần 2", value=date.today(), format="DD/MM/YYYY")
        with col_p2:
            new_price_2 = st.number_input("Giá vốn lần 2 (₫)", min_value=0, step=1000, value=0)

    if st.button("✅ Thêm vào danh mục", use_container_width=True):
        if new_symbol and new_price > 0:
            entry = {
                "ngay_mua": new_date.strftime("%Y-%m-%d"),
                "ma_cp": new_symbol,
                "gia_von": new_price,
                "ty_trong": new_weight,
            }
            if buy_twice and new_price_2 > 0 and new_date_2:
                entry["ngay_mua_2"] = new_date_2.strftime("%Y-%m-%d")
                entry["gia_von_2"] = new_price_2
            st.session_state.portfolio.append(entry)
            save_portfolio(st.session_state.portfolio)
            st.rerun()

if add_clicked:
    add_stock_dialog()

if logo_b64:
    header_html = f"""
<div class="main-header">
    <img src="data:image/png;base64,{logo_b64}" class="logo-img" alt="KAFI SAIGON">
    <h1>BÁO CÁO DANH MỤC ĐẦU TƯ</h1>
    <div class="sub">Cập nhật hàng ngày</div>
    <div class="divider"></div>
</div>
"""
else:
    header_html = """
<div class="main-header">
    <h1>BÁO CÁO DANH MỤC ĐẦU TƯ</h1>
    <div class="sub">Cập nhật hàng ngày</div>
    <div class="divider"></div>
</div>
"""
st.markdown(header_html, unsafe_allow_html=True)

# ============================================================
# DANH MỤC
# ============================================================
portfolio = st.session_state.portfolio

if not portfolio:
    st.info("📭 Danh mục trống. Nhấn **➕ Thêm cổ phiếu** ở trên để bắt đầu.")
    st.stop()

# Session state cho chế độ chỉnh sửa / bán
if "editing_idx" not in st.session_state:
    st.session_state.editing_idx = None
if "selling_idx" not in st.session_state:
    st.session_state.selling_idx = None

with st.spinner("Đang lấy giá thị trường..."):
    industry_map = get_industry_map()
    rows = []
    for i, item in enumerate(portfolio):
        market_price = get_market_price(item["ma_cp"])
        # Giá vốn trung bình nếu mua 2 lần
        gia_von_avg = item["gia_von"]
        if item.get("gia_von_2"):
            gia_von_avg = (item["gia_von"] + item["gia_von_2"]) / 2
        profit_pct = None
        if market_price is not None:
            profit_pct = (market_price - gia_von_avg) / gia_von_avg * 100
        rows.append({
            "stt": i + 1,
            "ngay_mua": item["ngay_mua"],
            "ngay_mua_2": item.get("ngay_mua_2"),
            "ma_cp": item["ma_cp"],
            "gia_von": item["gia_von"],
            "gia_von_2": item.get("gia_von_2"),
            "gia_tt": market_price,
            "profit_pct": profit_pct,
            "ty_trong": item["ty_trong"],
            "nganh": industry_map.get(item["ma_cp"], "—"),
        })

# TÍNH TỔNG TỶ SUẤT SINH LỜI DANH MỤC
total_profit = 0
total_weight = 0
count_valid = 0
for r in rows:
    if r["profit_pct"] is not None:
        total_profit += r["profit_pct"] * r["ty_trong"]
        total_weight += r["ty_trong"]
        count_valid += 1

portfolio_return = total_profit / total_weight if total_weight > 0 else 0

# KPI CARDS
profit_cls = "positive" if portfolio_return >= 0 else "negative"
profit_sign = "+" if portfolio_return >= 0 else ""

kpi_html = f"""
<div class="kpi-row">
    <div class="kpi-card">
        <div class="kpi-title-row"><span class="kpi-icon">📈</span><div class="label">Tỷ suất sinh lời danh mục</div></div>
        <div class="value {profit_cls}">{profit_sign}{portfolio_return:.2f}%</div>
    </div>
    <div class="kpi-card">
        <div class="kpi-title-row"><span class="kpi-icon">💼</span><div class="label">Số lượng cổ phiếu</div></div>
        <div class="value neutral">{len(portfolio)}</div>
    </div>
    <div class="kpi-card">
        <div class="kpi-title-row"><span class="kpi-icon">⚖️</span><div class="label">Tổng tỷ trọng</div></div>
        <div class="value neutral">{sum(r['ty_trong'] for r in rows)}%</div>
    </div>
    <div class="kpi-card">
        <div class="kpi-title-row"><span class="kpi-icon">📅</span><div class="label">Ngày cập nhật</div></div>
        <div class="value neutral" style="font-size:1.15rem;">{datetime.now().strftime('%d/%m/%Y')}</div>
    </div>
</div>
"""
st.markdown(kpi_html, unsafe_allow_html=True)

# BẢNG DANH MỤC (HTML) + NÚT CHỈNH SỬA / XÓA
table_rows_html = ""
for r in rows:
    ngay_display = datetime.strptime(r["ngay_mua"], "%Y-%m-%d").strftime("%d/%m/%Y")
    if r.get("ngay_mua_2"):
        ngay_display += "<br>" + datetime.strptime(r["ngay_mua_2"], "%Y-%m-%d").strftime("%d/%m/%Y")

    # Giá vốn trung bình nếu mua 2 lần
    gia_von = r["gia_von"]
    if r.get("gia_von_2"):
        gia_von = (r["gia_von"] + r["gia_von_2"]) / 2

    gia_von_fmt = f"{gia_von:,.0f}".replace(",", ".")
    gia_tt_fmt = f"{r['gia_tt']:,.0f}".replace(",", ".") if r["gia_tt"] else "—"

    if r["profit_pct"] is not None:
        p_cls = "profit-positive" if r["profit_pct"] >= 0 else "profit-negative"
        p_sign = "+" if r["profit_pct"] >= 0 else ""
        p_icon = "▲" if r["profit_pct"] >= 0 else "▼"
        profit_display = f'<span class="{p_cls}">{p_icon} {p_sign}{r["profit_pct"]:.2f}%</span>'
    else:
        profit_display = '<span style="color:#6b7280;">N/A</span>'

    table_rows_html += (f'<tr><td>{r["stt"]}</td><td>{ngay_display}</td>'
                        f'<td class="symbol">{r["ma_cp"]}</td><td>{gia_von_fmt}</td>'
                        f'<td>{gia_tt_fmt}</td><td>{profit_display}</td>'
                        f'<td>{r["ty_trong"]}%</td><td>{r["nganh"]}</td></tr>')

table_html = ('<div class="glass-card"><table class="portfolio-table">'
              '<thead><tr><th>STT</th><th>Ngày mua</th><th>Mã cổ phiếu</th>'
              '<th>Giá vốn</th><th>Giá thị trường</th><th>% Lợi nhuận</th>'
              '<th>Tỷ trọng</th><th>Ngành</th></tr></thead>'
              f'<tbody>{table_rows_html}</tbody></table></div>')
st.markdown(table_html, unsafe_allow_html=True)

# CHỈNH SỬA / XÓA TỪNG CỔ PHIẾU
st.markdown("")  # spacer

for i, item in enumerate(portfolio):
    idx = i
    col_name, col_edit, col_sell, col_del = st.columns([3, 1, 1, 1])
    with col_name:
        ngay = datetime.strptime(item["ngay_mua"], "%Y-%m-%d").strftime("%d/%m/%Y")
        info_text = f'Mua {ngay} · Giá {item["gia_von"]:,.0f}₫'
        if item.get("ngay_mua_2") and item.get("gia_von_2"):
            ngay2 = datetime.strptime(item["ngay_mua_2"], "%Y-%m-%d").strftime("%d/%m/%Y")
            info_text += f' | Lần 2: {ngay2} · {item["gia_von_2"]:,.0f}₫'
        info_text += f' · {item["ty_trong"]}%'
        st.markdown(
            f'<span style="color:#00897B;font-weight:700;font-size:1rem;">'
            f'{item["ma_cp"]}</span>'
            f'<span style="color:#78909C;font-size:0.85rem;margin-left:12px;">'
            f'{info_text}</span>',
            unsafe_allow_html=True,
        )
    with col_edit:
        if st.button("✏️ Sửa", key=f"edit_{idx}", use_container_width=True):
            st.session_state.editing_idx = idx
            st.session_state.selling_idx = None
    with col_sell:
        if st.button("💰 Bán", key=f"sell_{idx}", use_container_width=True):
            st.session_state.selling_idx = idx
            st.session_state.editing_idx = None
    with col_del:
        if st.button("🗑️ Xóa", key=f"del_{idx}", use_container_width=True):
            removed = st.session_state.portfolio.pop(idx)
            save_portfolio(st.session_state.portfolio)
            st.session_state.editing_idx = None
            st.toast(f"Đã xóa **{removed['ma_cp']}**", icon="🗑️")
            st.rerun()

    # Form bán cổ phiếu (chốt lời / cắt lỗ)
    if st.session_state.get("selling_idx") == idx:
        with st.form(f"sell_form_{idx}"):
            st.markdown(
                f'<span style="color:#FF6F00;font-weight:600;">💰 Bán {item["ma_cp"]}</span>',
                unsafe_allow_html=True,
            )
            sc1, sc2 = st.columns(2)
            with sc1:
                sell_date = st.date_input("Ngày bán", value=date.today(), format="DD/MM/YYYY", key=f"sdate_{idx}")
            with sc2:
                sell_price = st.number_input("Giá bán (₫)", min_value=0, step=1000, value=0, key=f"sprice_{idx}")
            sb1, sb2 = st.columns(2)
            with sb1:
                confirm_sell = st.form_submit_button("✅ Xác nhận bán", use_container_width=True)
            with sb2:
                cancel_sell = st.form_submit_button("↩️ Hủy", use_container_width=True)

            if confirm_sell and sell_price > 0:
                # Tính giá vốn trung bình
                gia_von_avg = item["gia_von"]
                if item.get("gia_von_2"):
                    gia_von_avg = (item["gia_von"] + item["gia_von_2"]) / 2
                profit_pct = (sell_price - gia_von_avg) / gia_von_avg * 100

                closed_entry = {
                    "ma_cp": item["ma_cp"],
                    "ngay_mua": item["ngay_mua"],
                    "gia_von": item["gia_von"],
                    "ty_trong": item["ty_trong"],
                    "ngay_ban": sell_date.strftime("%Y-%m-%d"),
                    "gia_ban": sell_price,
                    "profit_pct": round(profit_pct, 2),
                    "loai": "chot_loi" if profit_pct >= 0 else "cat_lo",
                }
                if item.get("ngay_mua_2"):
                    closed_entry["ngay_mua_2"] = item["ngay_mua_2"]
                    closed_entry["gia_von_2"] = item["gia_von_2"]

                st.session_state.closed_positions.append(closed_entry)
                save_closed(st.session_state.closed_positions)
                st.session_state.portfolio.pop(idx)
                save_portfolio(st.session_state.portfolio)
                st.session_state.selling_idx = None
                label = "Chốt lời" if profit_pct >= 0 else "Cắt lỗ"
                st.toast(f"{label} **{item['ma_cp']}** ({profit_pct:+.2f}%)", icon="💰")
                st.rerun()
            if cancel_sell:
                st.session_state.selling_idx = None
                st.rerun()

    # Form chỉnh sửa inline
    if st.session_state.editing_idx == idx:
        with st.form(f"edit_form_{idx}"):
            st.markdown(
                f'<span style="color:#00897B;font-weight:600;">Chỉnh sửa {item["ma_cp"]}</span>',
                unsafe_allow_html=True,
            )
            st.markdown("**Lần mua 1**")
            ec1, ec2, ec3 = st.columns(3)
            with ec1:
                edit_date = st.date_input(
                    "Ngày mua 1",
                    value=datetime.strptime(item["ngay_mua"], "%Y-%m-%d").date(),
                    format="DD/MM/YYYY",
                    key=f"edate_{idx}",
                )
            with ec2:
                edit_price = st.number_input(
                    "Giá vốn 1 (₫)",
                    min_value=0,
                    step=1000,
                    value=int(item["gia_von"]),
                    key=f"eprice_{idx}",
                )
            with ec3:
                edit_weight = st.number_input(
                    "Tỷ trọng (%)",
                    min_value=0,
                    max_value=100,
                    step=5,
                    value=int(item["ty_trong"]),
                    key=f"eweight_{idx}",
                )

            # Lần mua 2 (tuỳ chọn)
            has_buy2 = bool(item.get("ngay_mua_2"))
            st.markdown("**Lần mua 2** *(tuỳ chọn)*")
            ed2_1, ed2_2 = st.columns(2)
            with ed2_1:
                edit_date_2 = st.date_input(
                    "Ngày mua 2",
                    value=datetime.strptime(item["ngay_mua_2"], "%Y-%m-%d").date() if has_buy2 else date.today(),
                    format="DD/MM/YYYY",
                    key=f"edate2_{idx}",
                )
            with ed2_2:
                edit_price_2 = st.number_input(
                    "Giá vốn 2 (₫)",
                    min_value=0,
                    step=1000,
                    value=int(item.get("gia_von_2", 0)),
                    key=f"eprice2_{idx}",
                )

            if has_buy2:
                fc1, fc2, fc3 = st.columns(3)
            else:
                fc1, fc2 = st.columns(2)
                fc3 = None
            with fc1:
                save_btn = st.form_submit_button("💾 Lưu", use_container_width=True)
            with fc2:
                cancel_btn = st.form_submit_button("↩️ Hủy", use_container_width=True)
            if fc3 is not None:
                with fc3:
                    del_buy2_btn = st.form_submit_button("🗑️ Xóa lần mua 2", use_container_width=True)
            else:
                del_buy2_btn = False

            if save_btn:
                st.session_state.portfolio[idx]["ngay_mua"] = edit_date.strftime("%Y-%m-%d")
                st.session_state.portfolio[idx]["gia_von"] = edit_price
                st.session_state.portfolio[idx]["ty_trong"] = edit_weight
                # Lần mua 2
                if edit_price_2 > 0:
                    st.session_state.portfolio[idx]["ngay_mua_2"] = edit_date_2.strftime("%Y-%m-%d")
                    st.session_state.portfolio[idx]["gia_von_2"] = edit_price_2
                else:
                    st.session_state.portfolio[idx].pop("ngay_mua_2", None)
                    st.session_state.portfolio[idx].pop("gia_von_2", None)
                save_portfolio(st.session_state.portfolio)
                st.session_state.editing_idx = None
                st.cache_data.clear()
                st.toast(f"Đã cập nhật **{item['ma_cp']}**", icon="✅")
                st.rerun()
            if cancel_btn:
                st.session_state.editing_idx = None
                st.rerun()
            if del_buy2_btn:
                st.session_state.portfolio[idx].pop("ngay_mua_2", None)
                st.session_state.portfolio[idx].pop("gia_von_2", None)
                save_portfolio(st.session_state.portfolio)
                st.session_state.editing_idx = None
                st.cache_data.clear()
                st.toast(f"Đã xóa lần mua 2 của **{item['ma_cp']}**", icon="🗑️")
                st.rerun()

# ============================================================
# THỐNG KÊ VỊ THẾ ĐÃ ĐÓNG (Chốt lời / Cắt lỗ)
# ============================================================
closed = st.session_state.closed_positions
if closed:
    st.markdown("---")
    st.markdown("### 📊 Lịch sử giao dịch đã đóng")

    # Phân loại
    chot_loi = [c for c in closed if c["loai"] == "chot_loi"]
    cat_lo = [c for c in closed if c["loai"] == "cat_lo"]

    # Thống kê tổng quan
    total_closed = len(closed)
    win_rate = len(chot_loi) / total_closed * 100 if total_closed > 0 else 0
    avg_profit = sum(c["profit_pct"] for c in chot_loi) / len(chot_loi) if chot_loi else 0
    avg_loss = sum(c["profit_pct"] for c in cat_lo) / len(cat_lo) if cat_lo else 0

    stats_html = f"""
<div class="kpi-row">
    <div class="kpi-card" style="background: linear-gradient(145deg, #2E7D32 0%, #1B5E20 100%); box-shadow: 0 4px 16px rgba(46,125,50,0.25);">
        <div class="kpi-title-row"><span class="kpi-icon">✅</span><div class="label">Chốt lời</div></div>
        <div class="value neutral">{len(chot_loi)}</div>
    </div>
    <div class="kpi-card" style="background: linear-gradient(145deg, #C62828 0%, #B71C1C 100%); box-shadow: 0 4px 16px rgba(198,40,40,0.25);">
        <div class="kpi-title-row"><span class="kpi-icon">❌</span><div class="label">Cắt lỗ</div></div>
        <div class="value neutral">{len(cat_lo)}</div>
    </div>
    <div class="kpi-card">
        <div class="kpi-title-row"><span class="kpi-icon">🎯</span><div class="label">Win Rate</div></div>
        <div class="value neutral">{win_rate:.1f}%</div>
    </div>
    <div class="kpi-card">
        <div class="kpi-title-row"><span class="kpi-icon">📈</span><div class="label">TB Lãi / Lỗ</div></div>
        <div class="value neutral" style="font-size:1rem;">{avg_profit:+.2f}% / {avg_loss:+.2f}%</div>
    </div>
</div>
"""
    st.markdown(stats_html, unsafe_allow_html=True)

    # Bảng chi tiết
    closed_rows_html = ""
    for ci, c in enumerate(closed):
        ngay_mua = datetime.strptime(c["ngay_mua"], "%Y-%m-%d").strftime("%d/%m/%Y")
        ngay_ban = datetime.strptime(c["ngay_ban"], "%Y-%m-%d").strftime("%d/%m/%Y")
        gia_von = c["gia_von"]
        if c.get("gia_von_2"):
            gia_von = (c["gia_von"] + c["gia_von_2"]) / 2
        gia_von_fmt = f"{gia_von:,.0f}".replace(",", ".")
        gia_ban_fmt = f"{c['gia_ban']:,.0f}".replace(",", ".")
        p = c["profit_pct"]
        if p >= 0:
            p_cls = "profit-positive"
            p_icon = "▲"
            p_sign = "+"
        else:
            p_cls = "profit-negative"
            p_icon = "▼"
            p_sign = ""
        profit_display = f'<span class="{p_cls}">{p_icon} {p_sign}{p:.2f}%</span>'
        loai_badge = '<span style="background:#2E7D32;color:#fff;padding:2px 8px;border-radius:8px;font-size:0.75rem;">Chốt lời</span>' if c["loai"] == "chot_loi" else '<span style="background:#C62828;color:#fff;padding:2px 8px;border-radius:8px;font-size:0.75rem;">Cắt lỗ</span>'

        closed_rows_html += (f'<tr><td>{ci+1}</td><td class="symbol">{c["ma_cp"]}</td>'
                             f'<td>{ngay_mua}</td><td>{gia_von_fmt}</td>'
                             f'<td>{ngay_ban}</td><td>{gia_ban_fmt}</td>'
                             f'<td>{profit_display}</td><td>{loai_badge}</td></tr>')

    closed_table = ('<div class="glass-card"><table class="portfolio-table">'
                    '<thead><tr><th>STT</th><th>Mã CP</th><th>Ngày mua</th>'
                    '<th>Giá vốn</th><th>Ngày bán</th><th>Giá bán</th>'
                    '<th>% Lợi nhuận</th><th>Loại</th></tr></thead>'
                    f'<tbody>{closed_rows_html}</tbody></table></div>')
    st.markdown(closed_table, unsafe_allow_html=True)

    # Nút xóa từng giao dịch đã đóng
    for ci, c in enumerate(closed):
        cc_label, cc_btn = st.columns([4, 1])
        with cc_label:
            st.markdown(
                f'<span style="color:#78909C;font-size:0.85rem;">'
                f'{ci+1}. {c["ma_cp"]} — bán {datetime.strptime(c["ngay_ban"], "%Y-%m-%d").strftime("%d/%m/%Y")}</span>',
                unsafe_allow_html=True,
            )
        with cc_btn:
            if st.button("🗑️ Xóa", key=f"del_closed_{ci}", use_container_width=True):
                st.session_state.closed_positions.pop(ci)
                save_closed(st.session_state.closed_positions)
                st.toast(f"Đã xóa giao dịch **{c['ma_cp']}**", icon="🗑️")
                st.rerun()

# Timestamp
st.markdown("")
st.markdown(
    f'<div class="timestamp">Cập nhật lúc {datetime.now().strftime("%H:%M:%S %d/%m/%Y")} '
    f'&nbsp;|&nbsp; Giá được cache 5 phút</div>',
    unsafe_allow_html=True,
)