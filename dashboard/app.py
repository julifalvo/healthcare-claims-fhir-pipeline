"""Revenue cycle & clinical quality dashboard over the gold Delta tables (delta-rs, no Spark)."""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from deltalake import DeltaTable

from claims_pipeline.config import SILVER_DIR, table_path
from claims_pipeline.jobs.exports import denials_worklist, read_gold

st.set_page_config(
    page_title="Claims Lakehouse", page_icon=":material/monitor_heart:", layout="wide"
)

# Validated reference palette (light). Categorical order is fixed; color follows the payer.
SURFACE, INK, INK_2, MUTED, GRID, AXIS = (
    "#fcfcfb",
    "#0b0b0b",
    "#52514e",
    "#898781",
    "#e1e0d9",
    "#c3c2b7",
)
CATEGORICAL = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300"]
SEQUENTIAL = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"]
AGING_RAMP = {"0-30": "#86b6ef", "31-60": "#3987e5", "61-90": "#256abf", "90+": "#184f95"}
ACCENT = CATEGORICAL[0]
FONT = 'system-ui, -apple-system, "Segoe UI", sans-serif'
CLASS_LABELS = {"AMB": "Ambulatory", "EMER": "Emergency", "IMP": "Inpatient"}

st.markdown(
    f"""
    <style>
      .block-container {{ padding-top: 2rem; max-width: 1400px; }}
      [data-testid="stMetric"] {{
        background: {SURFACE}; border: 1px solid rgba(11,11,11,0.10);
        border-radius: 10px; padding: 14px 16px;
      }}
      [data-testid="stMetricLabel"] p {{ color: {INK_2}; font-size: 0.8rem; }}
      [data-testid="stMetricValue"] {{ font-size: 1.6rem; }}
      .takeaway {{ color: {INK_2}; font-size: 0.9rem; margin: -0.4rem 0 0.4rem 0; }}
      h3 {{ font-size: 1.05rem !important; margin-bottom: 0.2rem !important; }}
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_data(ttl=60, show_spinner=False)
def gold(name: str) -> pd.DataFrame:
    return read_gold(name)


@st.cache_data(ttl=60, show_spinner=False)
def quarantine_sample() -> pd.DataFrame:
    df = DeltaTable(table_path(SILVER_DIR, "_quarantine")).to_pandas(
        columns=["entity", "record_id", "failed_rules", "export_date", "raw"]
    )
    df["failed_rules"] = df["failed_rules"].map(lambda r: ", ".join(r))
    df["raw"] = df["raw"].str.slice(0, 160) + "…"
    return df.sort_values("export_date", ascending=False).head(200)


@st.cache_data(ttl=60, show_spinner=False)
def worklist(as_of) -> pd.DataFrame:
    return denials_worklist(as_of)


def style(fig: go.Figure, height: int = 340, percent_axis: str | None = None) -> go.Figure:
    fig.update_layout(
        template="none",
        height=height,
        margin={"l": 8, "r": 8, "t": 8, "b": 8},
        paper_bgcolor=SURFACE,
        plot_bgcolor=SURFACE,
        font={"family": FONT, "color": INK_2, "size": 12},
        legend={
            "orientation": "h",
            "yanchor": "bottom",
            "y": 1.02,
            "x": 0,
            "font": {"color": INK_2},
        },
        hoverlabel={
            "bgcolor": "white",
            "bordercolor": GRID,
            "font": {"color": INK, "family": FONT},
        },
        bargap=0.35,
    )
    fig.update_xaxes(showgrid=False, linecolor=AXIS, tickfont={"color": MUTED}, zeroline=False)
    fig.update_yaxes(gridcolor=GRID, linecolor=AXIS, tickfont={"color": MUTED}, zeroline=False)
    if percent_axis == "x":
        fig.update_xaxes(tickformat=".0%", showgrid=True, gridcolor=GRID)
    elif percent_axis == "y":
        fig.update_yaxes(tickformat=".0%")
    return fig


def chart(fig: go.Figure, data: pd.DataFrame, key: str) -> None:
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False}, key=key)
    with st.expander("View data"):
        st.dataframe(data, hide_index=True, use_container_width=True)


def heading(title: str, takeaway: str) -> None:
    st.markdown(f"### {title}")
    st.markdown(f"<div class='takeaway'>{takeaway}</div>", unsafe_allow_html=True)


def rate(numerator: pd.Series, denominator: pd.Series) -> float:
    total = denominator.sum()
    return float(numerator.sum() / total) if total else 0.0


try:
    fact = gold("fact_claim")
except Exception:
    st.title("Claims Lakehouse")
    st.info(
        "No gold data yet. Trigger the `claims_lakehouse` DAG in Airflow (http://localhost:8080)."
    )
    st.stop()

payers = gold("dim_payer").sort_values("payer_id").reset_index(drop=True)
payer_color = dict(zip(payers["payer_name"], CATEGORICAL, strict=False))
fact = fact.merge(payers[["payer_id", "payer_name"]], on="payer_id")
fact["encounter_class"] = fact["encounter_class"].map(CLASS_LABELS).fillna("Unknown")
fact["month"] = pd.to_datetime(fact["submitted_month"])
as_of = pd.to_datetime(fact["submitted_date"]).max().date()

st.title("Revenue Cycle & Clinical Quality")
st.caption(
    f"Regional health system · FHIR R4 claims lakehouse (Airflow · Spark · Delta Lake) · "
    f"data as of **{as_of:%b %d, %Y}** · synthetic data"
)

months = sorted(fact["month"].dt.strftime("%Y-%m").unique())
f1, f2, f3 = st.columns([2, 2, 3])
chosen_payers = f1.multiselect("Payer", list(payer_color), placeholder="All payers")
chosen_classes = f2.multiselect("Setting", list(CLASS_LABELS.values()), placeholder="All settings")
month_from, month_to = f3.select_slider("Submission month", months, value=(months[0], months[-1]))

view = fact[fact["month"].dt.strftime("%Y-%m").between(month_from, month_to)]
if chosen_payers:
    view = view[view["payer_name"].isin(chosen_payers)]
if chosen_classes:
    view = view[view["encounter_class"].isin(chosen_classes)]
adjudicated = view[view["claim_status"] != "pending"]
pending = view[view["claim_status"] == "pending"]

k = st.columns(6)
k[0].metric("Billed", f"${view['billed_amount'].sum() / 1e6:,.1f}M", help="Gross charges submitted")
k[1].metric("Collected", f"${view['paid_amount'].sum() / 1e6:,.1f}M", help="Paid by payers")
k[2].metric(
    "Denial rate",
    f"{rate(adjudicated['claim_status'].eq('denied'), adjudicated['claim_status'].notna()):.1%}",
    help="Fully denied / adjudicated claims",
)
k[3].metric(
    "Clean claim rate",
    f"{rate(adjudicated['is_clean_claim'], adjudicated['claim_status'].notna()):.1%}",
    help="Paid in full on first submission",
)
k[4].metric("Days to adjudicate", f"{adjudicated['days_to_adjudication'].mean():.1f}")
k[5].metric(
    f"Open A/R · {len(pending):,} claims",
    f"${pending['billed_amount'].sum() / 1e6:,.2f}M",
    help="Submitted claims still waiting for a payer decision",
)

# The as-of month is still being adjudicated; trending it would show a false drop.
open_month = pd.Timestamp(as_of).to_period("M").to_timestamp()
mature = view[view["month"] < open_month]
mature_adjudicated = mature[mature["claim_status"] != "pending"]

tabs = st.tabs(["Revenue cycle", "Denials", "A/R aging", "Readmissions", "Data quality"])

with tabs[0]:
    monthly = (
        mature_adjudicated.groupby(["month", "payer_name"])
        .agg(
            adjudicated=("claim_id", "count"),
            denied=("claim_status", lambda s: (s == "denied").sum()),
        )
        .reset_index()
    )
    monthly["denial_rate"] = monthly["denied"] / monthly["adjudicated"]
    by_payer = monthly.groupby("payer_name")[["denied", "adjudicated"]].sum()
    by_payer = (by_payer["denied"] / by_payer["adjudicated"]).sort_values()
    left, right = st.columns([3, 2])
    with left:
        heading(
            "Denial rate by payer",
            f"<b>{by_payer.index[-1]}</b> denies {by_payer.iloc[-1]:.1%} of claims, "
            f"{by_payer.iloc[-1] / max(by_payer.iloc[0], 1e-9):.1f}x the best payer ({by_payer.index[0]}).",
        )
        fig = go.Figure()
        for name in payer_color:
            series = monthly[monthly["payer_name"] == name]
            if series.empty:
                continue
            fig.add_scatter(
                x=series["month"],
                y=series["denial_rate"],
                name=name,
                mode="lines",
                line={"color": payer_color[name], "width": 2},
                customdata=series[["denied", "adjudicated"]],
                hovertemplate="%{y:.1%} (%{customdata[0]} of %{customdata[1]})<extra>"
                + name
                + "</extra>",
            )
        fig.update_layout(hovermode="x unified")
        chart(style(fig, percent_axis="y"), monthly, "denial_trend")
    with right:
        cash = (
            mature.groupby("month")[["billed_amount", "allowed_amount", "paid_amount"]]
            .sum()
            .reset_index()
        )
        heading(
            "Billed vs. collected",
            f"Payers pay {rate(mature['paid_amount'], mature['billed_amount']):.0%} of gross charges; "
            f"the gap is mostly contractual allowances. {open_month:%B} is excluded until adjudicated.",
        )
        fig = go.Figure()
        for column, label, color in [
            ("billed_amount", "Billed", CATEGORICAL[0]),
            ("paid_amount", "Collected", CATEGORICAL[1]),
        ]:
            fig.add_bar(
                x=cash["month"],
                y=cash[column],
                name=label,
                marker={"color": color, "cornerradius": 4},
                hovertemplate="$%{y:,.0f}<extra>" + label + "</extra>",
            )
        fig.update_layout(barmode="group", bargroupgap=0.08)
        fig.update_yaxes(tickprefix="$", tickformat="~s")
        chart(style(fig), cash, "cash")

with tabs[1]:
    denied = view[view["claim_status"].isin(["denied", "partially_denied"])]
    reasons = gold("dim_denial_reason")
    by_reason = (
        denied.groupby("denial_reason_code")
        .agg(claims=("claim_id", "count"), denied_amount=("denied_amount", "sum"))
        .reset_index()
        .merge(reasons, on="denial_reason_code")
        .sort_values("denied_amount")
    )
    recoverable_share = rate(
        by_reason.loc[by_reason["recoverable"], "denied_amount"], by_reason["denied_amount"]
    )
    left, right = st.columns([3, 2])
    with left:
        top = by_reason.iloc[-1] if not by_reason.empty else None
        heading(
            "Denied dollars by reason",
            f"{recoverable_share:.0%} of denied dollars are recoverable through rework or appeal"
            + (
                f"; <b>{top['category']}</b> alone is ${top['denied_amount'] / 1e3:,.0f}K."
                if top is not None
                else "."
            ),
        )
        labels = by_reason["denial_reason_code"] + " · " + by_reason["category"]
        fig = go.Figure(
            go.Bar(
                x=by_reason["denied_amount"],
                y=labels,
                orientation="h",
                marker={"color": ACCENT, "cornerradius": 4},
                text=(by_reason["denied_amount"] / 1e3).map("${:,.0f}K".format),
                textposition="outside",
                textfont={"color": INK_2},
                cliponaxis=False,
                customdata=by_reason[["description", "claims", "recommended_action"]],
                hovertemplate="<b>%{customdata[0]}</b><br>%{customdata[1]} claims · $%{x:,.0f}"
                "<br>Next step: %{customdata[2]}<extra></extra>",
            )
        )
        fig.update_xaxes(tickprefix="$", tickformat="~s", showgrid=True, gridcolor=GRID)
        chart(style(fig), by_reason, "denial_reasons")
    with right:
        grid = (
            adjudicated[adjudicated["encounter_class"] != "Unknown"]
            .groupby(["payer_name", "encounter_class"])["claim_status"]
            .apply(lambda s: (s == "denied").mean())
            .unstack()
            .reindex(index=[p for p in payer_color if p in adjudicated["payer_name"].unique()])
        )
        heading(
            "Denial rate by payer and setting",
            "Inpatient claims are denied most, driven by missing prior authorization.",
        )
        fig = go.Figure(
            go.Heatmap(
                z=grid.values,
                x=grid.columns,
                y=grid.index,
                colorscale=[[i / (len(SEQUENTIAL) - 1), c] for i, c in enumerate(SEQUENTIAL)],
                text=[[f"{v:.0%}" if pd.notna(v) else "" for v in row] for row in grid.values],
                texttemplate="%{text}",
                xgap=2,
                ygap=2,
                showscale=False,
                hovertemplate="%{y} · %{x}: %{z:.1%}<extra></extra>",
            )
        )
        fig.update_yaxes(autorange="reversed", showgrid=False)
        chart(style(fig), grid.reset_index(), "denial_heatmap")

    wl = worklist(as_of)
    if chosen_payers:
        wl = wl[wl["payer_name"].isin(chosen_payers)]
    heading(
        "Appeals worklist",
        f"{len(wl):,} recoverable denials inside the appeal window, "
        f"<b>${wl['denied_amount'].sum() / 1e3:,.0f}K</b> at stake. Highest value first.",
    )
    st.dataframe(
        wl.head(50),
        hide_index=True,
        use_container_width=True,
        column_config={
            "denied_amount": st.column_config.NumberColumn("Denied", format="$%.2f"),
            "days_since_denial": st.column_config.ProgressColumn(
                "Days since denial", min_value=0, max_value=90, format="%d d"
            ),
        },
    )
    st.download_button(
        "Download worklist (CSV)", wl.to_csv(index=False), f"denials_worklist_{as_of}.csv"
    )

with tabs[2]:
    aging = pending.assign(
        bucket=pd.cut(pending["ar_age_days"], [-1, 30, 60, 90, 10_000], labels=list(AGING_RAMP))
    )
    table = aging.pivot_table(
        index="payer_name", columns="bucket", values="billed_amount", aggfunc="sum", observed=False
    ).fillna(0)
    over_90 = (
        rate(table.get("90+", pd.Series(dtype=float)), table.sum(axis=1)) if not table.empty else 0
    )
    heading(
        "Open accounts receivable by age",
        f"{over_90:.0%} of open A/R is older than 90 days: claims with no payer response need follow-up.",
    )
    fig = go.Figure()
    for bucket, color in AGING_RAMP.items():
        if bucket in table:
            fig.add_bar(
                y=table.index,
                x=table[bucket],
                name=f"{bucket} days",
                orientation="h",
                marker={"color": color, "line": {"color": SURFACE, "width": 2}},
                hovertemplate="%{y}: $%{x:,.0f}<extra>" + bucket + " days</extra>",
            )
    fig.update_layout(barmode="stack")
    fig.update_xaxes(tickprefix="$", tickformat="~s", showgrid=True, gridcolor=GRID)
    style(fig, height=320).update_layout(legend={"traceorder": "normal"})
    chart(fig, table.reset_index(), "aging")

with tabs[3]:
    stays = gold("mart_readmissions")
    by_dx = stays.groupby("diagnosis")[["readmissions", "index_stays"]].sum().reset_index()
    by_dx["rate"] = by_dx["readmissions"] / by_dx["index_stays"]
    by_dx = by_dx.sort_values("rate")
    trend = stays.groupby("discharge_month")[["readmissions", "index_stays"]].sum().reset_index()
    trend["rate"] = trend["readmissions"] / trend["index_stays"]
    overall = rate(by_dx["readmissions"], by_dx["index_stays"])
    left, right = st.columns([3, 2])
    with left:
        worst = by_dx.iloc[-1]
        heading(
            "30-day readmission rate by principal diagnosis",
            f"<b>{worst['diagnosis']}</b> patients return within 30 days {worst['rate']:.0%} of the time "
            f"(all conditions: {overall:.1%}).",
        )
        fig = go.Figure(
            go.Bar(
                x=by_dx["rate"],
                y=by_dx["diagnosis"],
                orientation="h",
                marker={"color": ACCENT, "cornerradius": 4},
                text=by_dx["rate"].map("{:.0%}".format),
                textposition="outside",
                textfont={"color": INK_2},
                cliponaxis=False,
                customdata=by_dx[["readmissions", "index_stays"]],
                hovertemplate="%{x:.1%} (%{customdata[0]} of %{customdata[1]} stays)<extra></extra>",
            )
        )
        chart(style(fig, percent_axis="x"), by_dx, "readmit_dx")
    with right:
        heading("Monthly trend", "Index stays with a complete 30-day follow-up window.")
        fig = go.Figure(
            go.Scatter(
                x=pd.to_datetime(trend["discharge_month"]),
                y=trend["rate"],
                mode="lines+markers",
                line={"color": ACCENT, "width": 2},
                marker={"size": 8, "line": {"color": SURFACE, "width": 2}},
                customdata=trend[["readmissions", "index_stays"]],
                hovertemplate="%{y:.1%} (%{customdata[0]} of %{customdata[1]})<extra></extra>",
            )
        )
        fig.update_yaxes(rangemode="tozero")
        chart(style(fig, percent_axis="y"), trend, "readmit_trend")

with tabs[4]:
    dq = gold("mart_data_quality")
    records = dq[dq["rule"] == "__records__"]
    rules = (
        dq[dq["rule"] != "__records__"]
        .groupby(["entity", "rule"])[["failed", "checked"]]
        .sum()
        .reset_index()
    )
    rules = rules[rules["failed"] > 0].sort_values("failed")
    q = st.columns(4)
    q[0].metric("Records validated", f"{records['checked'].sum():,}")
    q[1].metric("Quarantined", f"{records['failed'].sum():,}")
    q[2].metric(
        "Quarantine rate",
        f"{rate(records['failed'], records['checked']):.2%}",
        help="Gate fails above 5%",
    )
    q[3].metric("Rules enforced", f"{dq.loc[dq['rule'] != '__records__', 'rule'].nunique()}")
    left, right = st.columns([2, 3])
    with left:
        heading(
            "Quarantined records by rule",
            "Nothing is dropped silently: every rejection names its rule.",
        )
        fig = go.Figure(
            go.Bar(
                x=rules["failed"],
                y=rules["entity"] + " · " + rules["rule"],
                orientation="h",
                marker={"color": ACCENT, "cornerradius": 4},
                text=rules["failed"],
                textposition="outside",
                textfont={"color": INK_2},
                cliponaxis=False,
                hovertemplate="%{x} records<extra></extra>",
            )
        )
        chart(style(fig, height=380), rules, "dq_rules")
    with right:
        heading("Quarantine sample", "Raw FHIR lines kept for triage and replay after a fix.")
        st.dataframe(quarantine_sample(), hide_index=True, use_container_width=True, height=380)
