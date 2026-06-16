"""
app.py — RakuFlow Analytics Streamlit Dashboard.

Provides real-time e-commerce analytics powered by the RakuFlow data pipeline:
    - KPI cards: Total Orders, Total GMV, Avg Delivery Days, Active Sellers
    - Daily GMV trend (line chart)
    - Top 10 sellers by revenue (bar chart)
    - Order status distribution (pie chart)
    - Avg delivery days by state (horizontal bar chart)

Data source: PostgreSQL mart tables populated by dbt.
Cloud deployment: reads DB credentials from st.secrets (Streamlit Cloud)
                  or environment variables (local / Docker).
"""

from __future__ import annotations

import os
from datetime import date, datetime, timezone
from typing import Optional

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine, text

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="RakuFlow Analytics",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Styling ────────────────────────────────────────────────────────────────────
st.markdown(
    """
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

        html, body, [class*="css"] {
            font-family: 'Inter', sans-serif;
        }

        /* Dark gradient background */
        .stApp {
            background: linear-gradient(135deg, #0f0c29, #302b63, #24243e);
        }

        /* Metric cards */
        .metric-card {
            background: rgba(255,255,255,0.05);
            border: 1px solid rgba(255,255,255,0.10);
            border-radius: 16px;
            padding: 1.5rem;
            backdrop-filter: blur(12px);
            transition: transform 0.2s ease, box-shadow 0.2s ease;
        }
        .metric-card:hover {
            transform: translateY(-4px);
            box-shadow: 0 12px 40px rgba(132, 90, 223, 0.3);
        }
        .metric-label {
            font-size: 0.78rem;
            font-weight: 600;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: rgba(255,255,255,0.55);
            margin-bottom: 0.4rem;
        }
        .metric-value {
            font-size: 2rem;
            font-weight: 700;
            color: #ffffff;
            line-height: 1.1;
        }
        .metric-delta {
            font-size: 0.8rem;
            color: #4ade80;
            margin-top: 0.3rem;
        }

        /* Section headers */
        .section-header {
            font-size: 1.1rem;
            font-weight: 600;
            color: rgba(255,255,255,0.9);
            margin-bottom: 0.5rem;
            padding-bottom: 0.5rem;
            border-bottom: 1px solid rgba(255,255,255,0.1);
        }

        /* Sidebar */
        section[data-testid="stSidebar"] {
            background: rgba(15, 12, 41, 0.9);
            border-right: 1px solid rgba(255,255,255,0.1);
        }

        /* Remove default padding */
        .block-container { padding-top: 1.5rem; }

        /* Chart container */
        .chart-container {
            background: rgba(255,255,255,0.04);
            border: 1px solid rgba(255,255,255,0.08);
            border-radius: 16px;
            padding: 1rem;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

PLOTLY_THEME = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color="rgba(255,255,255,0.85)", family="Inter"),
    margin=dict(l=10, r=10, t=40, b=10),
    colorway=["#845adf", "#4ade80", "#f59e0b", "#38bdf8", "#fb7185", "#a78bfa"],
)


# ── Database connection ────────────────────────────────────────────────────────

def _get_db_url() -> str:
    """
    Resolve PostgreSQL connection URL from st.secrets (Streamlit Cloud)
    or environment variables (local / Docker).

    Priority: st.secrets["postgres"] > st.secrets["POSTGRES_URL"] > env vars
    """
    # Option 1: Full URL in secrets
    try:
        if "POSTGRES_URL" in st.secrets:
            return st.secrets["POSTGRES_URL"]
    except Exception:
        pass

    # Option 2: Individual fields in secrets [postgres] section
    try:
        pg = st.secrets.get("postgres", {})
        if pg:
            return (
                f"postgresql+psycopg2://"
                f"{pg.get('user', 'rakuflow')}:"
                f"{pg.get('password', 'rakuflow')}"
                f"@{pg.get('host', 'localhost')}:"
                f"{pg.get('port', 5432)}/"
                f"{pg.get('dbname', 'rakuflow')}"
            )
    except Exception:
        pass

    # Option 3: Environment variables (local dev / Docker)
    return (
        f"postgresql+psycopg2://"
        f"{os.getenv('POSTGRES_USER', 'rakuflow')}:"
        f"{os.getenv('POSTGRES_PASSWORD', 'rakuflow')}"
        f"@{os.getenv('POSTGRES_HOST', 'localhost')}:"
        f"{os.getenv('POSTGRES_PORT', '5432')}/"
        f"{os.getenv('POSTGRES_DB', 'rakuflow')}"
    )


@st.cache_resource
def get_engine():
    """
    Build and cache a SQLAlchemy engine.

    Returns:
        SQLAlchemy Engine connected to PostgreSQL, or None if unavailable.
    """
    try:
        engine = create_engine(_get_db_url(), pool_pre_ping=True, connect_args={"connect_timeout": 5})
        # Test connection
        with engine.connect():
            pass
        return engine
    except Exception:
        return None


def _db_available() -> bool:
    """Return True if a live DB connection is available."""
    return get_engine() is not None


# ── Data queries ───────────────────────────────────────────────────────────────

# ── Demo data (shown on Streamlit Cloud when no DB is configured) ──────────────

def _demo_daily_gmv() -> pd.DataFrame:
    """Return synthetic daily GMV data for the demo mode."""
    import numpy as np
    rng = np.random.default_rng(42)
    dates = pd.date_range("2017-01-01", "2018-09-30", freq="D")
    gmv = rng.uniform(2000, 18000, len(dates)).cumsum() / len(dates) * 200 + rng.uniform(1000, 5000, len(dates))
    orders = rng.integers(3, 25, len(dates))
    return pd.DataFrame({"order_date": dates.date, "total_orders": orders,
                         "total_gmv": gmv.round(2), "avg_payment_value": (gmv / orders).round(2),
                         "avg_delivery_days": rng.uniform(5, 20, len(dates)).round(1)})


def _demo_top_sellers() -> pd.DataFrame:
    """Return synthetic seller data for demo mode."""
    import uuid, numpy as np
    rng = np.random.default_rng(42)
    states = ["SP", "RJ", "MG", "RS", "PR", "SC", "BA", "GO", "PE", "CE"]
    cities = ["São Paulo", "Rio de Janeiro", "Belo Horizonte", "Porto Alegre",
              "Curitiba", "Florianópolis", "Salvador", "Goiânia", "Recife", "Fortaleza"]
    return pd.DataFrame({
        "seller_id": [str(uuid.UUID(int=rng.integers(0, 2**128))) for _ in range(10)],
        "city": cities, "state": states,
        "total_orders": rng.integers(20, 200, 10),
        "total_revenue": rng.uniform(5000, 80000, 10).round(2),
    }).sort_values("total_revenue", ascending=False)


def _demo_order_status() -> pd.DataFrame:
    """Return synthetic order status distribution for demo mode."""
    return pd.DataFrame({
        "order_status": ["delivered", "shipped", "canceled", "processing", "invoiced", "approved"],
        "order_count": [7023, 1107, 625, 301, 194, 150],
    })


def _demo_delivery_by_state() -> pd.DataFrame:
    """Return synthetic delivery days by state for demo mode."""
    import numpy as np
    rng = np.random.default_rng(42)
    states = ["AM", "PA", "RR", "AP", "MA", "RN", "CE", "BA", "MG", "SP",
              "RJ", "PR", "RS", "SC", "GO", "DF", "MT", "MS", "PE", "AL"]
    return pd.DataFrame({
        "customer_state": states,
        "avg_delivery_days": rng.uniform(8, 28, len(states)).round(1),
        "total_orders": rng.integers(50, 800, len(states)),
    }).sort_values("avg_delivery_days", ascending=False)


def _demo_kpis() -> dict:
    """Return demo KPI values."""
    return {"total_orders": 9400, "total_gmv": 1_247_832.50,
            "avg_delivery_days": 12.3, "active_sellers": 50}


# ── Data queries ───────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def load_daily_gmv(start_date: date, end_date: date) -> pd.DataFrame:
    """
    Load daily GMV data filtered by date range.

    Args:
        start_date: Inclusive start date for the filter.
        end_date:   Inclusive end date for the filter.

    Returns:
        DataFrame with columns: order_date, total_orders, total_gmv, avg_payment_value.
    """
    if not _db_available():
        df = _demo_daily_gmv()
        return df[(df["order_date"] >= start_date) & (df["order_date"] <= end_date)]
    query = text(
        """
        SELECT order_date, total_orders, total_gmv, avg_payment_value, avg_delivery_days
        FROM marts.agg_daily_gmv
        WHERE order_date BETWEEN :start AND :end
        ORDER BY order_date
        """
    )
    with get_engine().connect() as conn:
        return pd.read_sql(query, conn, params={"start": start_date, "end": end_date})


@st.cache_data(ttl=300)
def load_top_sellers(top_n: int = 10, state_filter: Optional[str] = None) -> pd.DataFrame:
    """
    Load top N sellers by total revenue, optionally filtered by state.

    Args:
        top_n:        Number of top sellers to return.
        state_filter: Optional 2-letter state code to filter by seller state.

    Returns:
        DataFrame with columns: seller_id, city, state, total_orders, total_revenue.
    """
    if not _db_available():
        df = _demo_top_sellers()
        if state_filter:
            df = df[df["state"] == state_filter]
        return df.head(top_n)
    state_clause = "AND state = :state" if state_filter else ""
    query = text(
        f"""
        SELECT seller_id, city, state, total_orders, total_revenue
        FROM marts.dim_sellers
        WHERE total_revenue > 0
        {state_clause}
        ORDER BY total_revenue DESC
        LIMIT :top_n
        """
    )
    params: dict = {"top_n": top_n}
    if state_filter:
        params["state"] = state_filter
    with get_engine().connect() as conn:
        return pd.read_sql(query, conn, params=params)


@st.cache_data(ttl=300)
def load_order_status_dist(start_date: date, end_date: date) -> pd.DataFrame:
    """
    Load order status distribution for a date range.

    Args:
        start_date: Inclusive start date.
        end_date:   Inclusive end date.

    Returns:
        DataFrame with columns: order_status, order_count.
    """
    if not _db_available():
        return _demo_order_status()
    query = text(
        """
        SELECT order_status, COUNT(*) as order_count
        FROM marts.fact_orders
        WHERE order_purchase_timestamp::date BETWEEN :start AND :end
        GROUP BY order_status
        ORDER BY order_count DESC
        """
    )
    with get_engine().connect() as conn:
        return pd.read_sql(query, conn, params={"start": start_date, "end": end_date})


@st.cache_data(ttl=300)
def load_delivery_by_state(state_filter: Optional[str] = None) -> pd.DataFrame:
    """
    Load average delivery days grouped by customer state.

    Args:
        state_filter: Optional 2-letter state code to filter results.

    Returns:
        DataFrame with columns: customer_state, avg_delivery_days, total_orders.
    """
    if not _db_available():
        df = _demo_delivery_by_state()
        if state_filter:
            df = df[df["customer_state"] == state_filter]
        return df
    state_clause = "AND customer_state = :state" if state_filter else ""
    query = text(
        f"""
        SELECT
            customer_state,
            ROUND(AVG(delivery_days)::numeric, 1) AS avg_delivery_days,
            COUNT(*) AS total_orders
        FROM marts.fact_orders
        WHERE delivery_days IS NOT NULL
            AND customer_state IS NOT NULL
            {state_clause}
        GROUP BY customer_state
        ORDER BY avg_delivery_days DESC
        """
    )
    params: dict = {}
    if state_filter:
        params["state"] = state_filter
    with get_engine().connect() as conn:
        return pd.read_sql(query, conn, params=params)


@st.cache_data(ttl=300)
def load_kpis(start_date: date, end_date: date) -> dict:
    """
    Load KPI summary metrics for the selected date range.

    Args:
        start_date: Inclusive start date.
        end_date:   Inclusive end date.

    Returns:
        Dictionary with keys: total_orders, total_gmv, avg_delivery_days, active_sellers.
    """
    if not _db_available():
        return _demo_kpis()
    query = text(
        """
        SELECT
            COUNT(DISTINCT fo.order_id)                       AS total_orders,
            ROUND(SUM(fo.payment_value)::numeric, 2)          AS total_gmv,
            ROUND(AVG(fo.delivery_days)::numeric, 1)          AS avg_delivery_days,
            COUNT(DISTINCT fo.seller_key)                     AS active_sellers
        FROM marts.fact_orders fo
        WHERE fo.order_purchase_timestamp::date BETWEEN :start AND :end
        """
    )
    with get_engine().connect() as conn:
        result = conn.execute(query, {"start": start_date, "end": end_date}).fetchone()
    return {
        "total_orders": int(result[0] or 0),
        "total_gmv": float(result[1] or 0),
        "avg_delivery_days": float(result[2] or 0),
        "active_sellers": int(result[3] or 0),
    }


# ── Sidebar ────────────────────────────────────────────────────────────────────

def render_sidebar() -> tuple[date, date, Optional[str]]:
    """
    Render the sidebar with date range and state filter controls.

    Returns:
        Tuple of (start_date, end_date, state_filter).
    """
    with st.sidebar:
        st.markdown(
            """
            <div style="text-align:center; padding: 1rem 0 1.5rem;">
                <div style="font-size:2.5rem;">⚡</div>
                <div style="font-size:1.4rem; font-weight:700; color:white;">RakuFlow</div>
                <div style="font-size:0.78rem; color:rgba(255,255,255,0.5); letter-spacing:0.1em;">
                    ANALYTICS PLATFORM
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        st.markdown("### 🗓️ Date Range")
        start_date = st.date_input(
            "From",
            value=date(2017, 1, 1),
            min_value=date(2016, 1, 1),
            max_value=date.today(),
            key="start_date",
        )
        end_date = st.date_input(
            "To",
            value=date(2018, 12, 31),
            min_value=date(2016, 1, 1),
            max_value=date.today(),
            key="end_date",
        )

        st.markdown("---")
        st.markdown("### 🗺️ State Filter")
        states = [
            "All States", "SP", "RJ", "MG", "RS", "PR", "SC", "BA",
            "GO", "PE", "CE", "ES", "PA", "MT", "MS", "DF", "RN",
            "AM", "MA", "PB", "AL", "PI", "SE", "RO", "AC", "AP", "RR", "TO",
        ]
        selected_state = st.selectbox("State", states, index=0, key="state_filter")
        state_filter: Optional[str] = None if selected_state == "All States" else selected_state

        st.markdown("---")
        st.markdown(
            """
            <div style="font-size:0.72rem; color:rgba(255,255,255,0.35); text-align:center;">
                Powered by Apache Kafka · PySpark · dbt<br>
                Data refreshes every 5 minutes
            </div>
            """,
            unsafe_allow_html=True,
        )

    return start_date, end_date, state_filter


# ── KPI Cards ──────────────────────────────────────────────────────────────────

def render_kpis(kpis: dict) -> None:
    """
    Render the four KPI metric cards at the top of the dashboard.

    Args:
        kpis: Dictionary with KPI values (total_orders, total_gmv, etc.)
    """
    c1, c2, c3, c4 = st.columns(4)

    def kpi_card(col, icon: str, label: str, value: str, delta: str = "") -> None:
        col.markdown(
            f"""
            <div class="metric-card">
                <div class="metric-label">{icon} {label}</div>
                <div class="metric-value">{value}</div>
                {"<div class='metric-delta'>" + delta + "</div>" if delta else ""}
            </div>
            """,
            unsafe_allow_html=True,
        )

    with c1:
        kpi_card(c1, "📦", "Total Orders", f"{kpis['total_orders']:,}", "↑ Live data")
    with c2:
        gmv = kpis["total_gmv"]
        kpi_card(c2, "💰", "Total GMV", f"R${gmv:,.0f}", "Gross Merchandise Value")
    with c3:
        kpi_card(c3, "🚚", "Avg Delivery Days", f"{kpis['avg_delivery_days']:.1f}d")
    with c4:
        kpi_card(c4, "🏪", "Active Sellers", f"{kpis['active_sellers']:,}")


# ── Charts ─────────────────────────────────────────────────────────────────────

def chart_daily_gmv(df: pd.DataFrame) -> go.Figure:
    """
    Build a dual-axis line chart for daily GMV and order count.

    Args:
        df: DataFrame with order_date, total_gmv, total_orders columns.

    Returns:
        Plotly Figure.
    """
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=df["order_date"],
            y=df["total_gmv"],
            name="Daily GMV (R$)",
            line=dict(color="#845adf", width=2.5),
            fill="tozeroy",
            fillcolor="rgba(132,90,223,0.12)",
            hovertemplate="<b>%{x}</b><br>GMV: R$%{y:,.0f}<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df["order_date"],
            y=df["total_orders"],
            name="Order Count",
            line=dict(color="#4ade80", width=1.8, dash="dot"),
            yaxis="y2",
            hovertemplate="<b>%{x}</b><br>Orders: %{y:,}<extra></extra>",
        )
    )
    fig.update_layout(
        title="Daily GMV & Order Volume",
        yaxis=dict(title="GMV (R$)", gridcolor="rgba(255,255,255,0.07)"),
        yaxis2=dict(
            title="Orders",
            overlaying="y",
            side="right",
            gridcolor="rgba(0,0,0,0)",
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            bgcolor="rgba(0,0,0,0)",
        ),
        hovermode="x unified",
        **PLOTLY_THEME,
    )
    return fig


def chart_top_sellers(df: pd.DataFrame) -> go.Figure:
    """
    Build a horizontal bar chart for top sellers by revenue.

    Args:
        df: DataFrame with seller_id, total_revenue, city columns.

    Returns:
        Plotly Figure.
    """
    df = df.sort_values("total_revenue")
    fig = go.Figure(
        go.Bar(
            x=df["total_revenue"],
            y=df["seller_id"].str[:8] + "…",
            orientation="h",
            marker=dict(
                color=df["total_revenue"],
                colorscale=[[0, "#302b63"], [1, "#845adf"]],
                showscale=False,
            ),
            text=df["total_revenue"].apply(lambda v: f"R${v:,.0f}"),
            textposition="outside",
            customdata=df[["city", "state", "total_orders"]].values,
            hovertemplate=(
                "<b>Seller %{y}</b><br>"
                "City: %{customdata[0]}, %{customdata[1]}<br>"
                "Revenue: R$%{x:,.0f}<br>"
                "Orders: %{customdata[2]:,}<extra></extra>"
            ),
        )
    )
    fig.update_layout(
        title="Top 10 Sellers by Revenue",
        xaxis=dict(title="Total Revenue (R$)", gridcolor="rgba(255,255,255,0.07)"),
        yaxis=dict(title=""),
        **PLOTLY_THEME,
    )
    return fig


def chart_order_status(df: pd.DataFrame) -> go.Figure:
    """
    Build a donut pie chart for order status distribution.

    Args:
        df: DataFrame with order_status, order_count columns.

    Returns:
        Plotly Figure.
    """
    colors = ["#845adf", "#4ade80", "#f59e0b", "#38bdf8", "#fb7185", "#a78bfa", "#6b7280"]
    fig = go.Figure(
        go.Pie(
            labels=df["order_status"],
            values=df["order_count"],
            hole=0.55,
            marker=dict(colors=colors[: len(df)], line=dict(color="#0f0c29", width=2)),
            textinfo="label+percent",
            hovertemplate="<b>%{label}</b><br>Orders: %{value:,}<br>Share: %{percent}<extra></extra>",
        )
    )
    fig.update_layout(
        title="Order Status Distribution",
        showlegend=True,
        legend=dict(orientation="v", bgcolor="rgba(0,0,0,0)"),
        **PLOTLY_THEME,
    )
    return fig


def chart_delivery_by_state(df: pd.DataFrame) -> go.Figure:
    """
    Build a color-coded bar chart of average delivery days by state.

    Args:
        df: DataFrame with customer_state, avg_delivery_days, total_orders columns.

    Returns:
        Plotly Figure.
    """
    fig = go.Figure(
        go.Bar(
            x=df["customer_state"],
            y=df["avg_delivery_days"],
            marker=dict(
                color=df["avg_delivery_days"],
                colorscale=[[0, "#4ade80"], [0.5, "#f59e0b"], [1, "#fb7185"]],
                showscale=True,
                colorbar=dict(title="Days", tickfont=dict(color="white")),
            ),
            customdata=df["total_orders"].values,
            hovertemplate=(
                "<b>%{x}</b><br>"
                "Avg Delivery: %{y:.1f} days<br>"
                "Total Orders: %{customdata:,}<extra></extra>"
            ),
        )
    )
    fig.update_layout(
        title="Avg Delivery Days by State",
        xaxis=dict(title="State", tickangle=-45),
        yaxis=dict(title="Avg Delivery Days", gridcolor="rgba(255,255,255,0.07)"),
        **PLOTLY_THEME,
    )
    return fig


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    """Render the full RakuFlow Analytics Streamlit dashboard."""
    start_date, end_date, state_filter = render_sidebar()

    # Header
    st.markdown(
        """
        <h1 style="
            font-size: 2.2rem;
            font-weight: 800;
            background: linear-gradient(90deg, #845adf, #38bdf8);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            margin-bottom: 0.2rem;
        ">⚡ RakuFlow Analytics</h1>
        <p style="color:rgba(255,255,255,0.5); font-size:0.9rem; margin-bottom: 1.5rem;">
            Real-time e-commerce intelligence · Olist Brazil Dataset · Powered by Kafka + PySpark + dbt
        </p>
        """,
        unsafe_allow_html=True,
    )

    # Demo mode banner
    if not _db_available():
        st.info(
            "📊 **Demo Mode** — Showing sample data. "
            "Connect a PostgreSQL database via [Streamlit secrets](https://docs.streamlit.io/deploy/streamlit-community-cloud/deploy-your-app/secrets-management) "
            "to see live pipeline data.",
            icon="ℹ️",
        )

    # Load KPIs
    with st.spinner("Loading analytics…"):
        kpis = load_kpis(start_date, end_date)
        render_kpis(kpis)

        st.markdown("<div style='height:1.5rem'></div>", unsafe_allow_html=True)

        # Row 1: GMV trend + Order status
        col1, col2 = st.columns([3, 2])
        with col1:
            df_gmv = load_daily_gmv(start_date, end_date)
            if df_gmv.empty:
                st.info("No GMV data for selected date range.")
            else:
                st.plotly_chart(
                    chart_daily_gmv(df_gmv), use_container_width=True, key="gmv_chart"
                )

        with col2:
            df_status = load_order_status_dist(start_date, end_date)
            if df_status.empty:
                st.info("No order status data.")
            else:
                st.plotly_chart(
                    chart_order_status(df_status),
                    use_container_width=True,
                    key="status_chart",
                )

        st.markdown("<div style='height:0.5rem'></div>", unsafe_allow_html=True)

        # Row 2: Top sellers + Delivery by state
        col3, col4 = st.columns(2)
        with col3:
            df_sellers = load_top_sellers(top_n=10, state_filter=state_filter)
            if df_sellers.empty:
                st.info("No seller data available.")
            else:
                st.plotly_chart(
                    chart_top_sellers(df_sellers),
                    use_container_width=True,
                    key="sellers_chart",
                )

        with col4:
            df_delivery = load_delivery_by_state(state_filter=state_filter)
            if df_delivery.empty:
                st.info("No delivery data available.")
            else:
                st.plotly_chart(
                    chart_delivery_by_state(df_delivery),
                    use_container_width=True,
                    key="delivery_chart",
                )

        # Footer
        st.markdown(
            f"""
            <div style="
                margin-top: 2rem;
                padding: 1rem;
                border-top: 1px solid rgba(255,255,255,0.08);
                font-size: 0.75rem;
                color: rgba(255,255,255,0.3);
                text-align: center;
            ">
                RakuFlow · Built for Rakuten Japan Internship Application ·
                Last refreshed: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}
            </div>
            """,
            unsafe_allow_html=True,
        )


if __name__ == "__main__":
    main()
