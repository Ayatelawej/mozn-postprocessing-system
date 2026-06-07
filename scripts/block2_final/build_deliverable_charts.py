from __future__ import annotations

import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


OUTPUT_DIR = Path("reports/block2_final")
FIGURE_DIR = OUTPUT_DIR / "figures"


WINNING_CONFIG = {
    "temperature":       {"model": "HGB",   "color": "#1f77b4"},
    "relative_humidity": {"model": "HGB",   "color": "#1f77b4"},
    "dew_point":         {"model": "HGB",   "color": "#1f77b4"},
    "wind_speed":        {"model": "Ridge", "color": "#ff7f0e"},
    "wind_gust":         {"model": "Ridge", "color": "#ff7f0e"},
    "pressure":          {"model": "Ridge", "color": "#ff7f0e"},
    "uv":                {"model": "HGB",   "color": "#1f77b4"},
    "wind_direction":    {"model": "Ridge", "color": "#ff7f0e"},
}


VALIDATION_MODES = ("LOSO", "within_station", "april")
VALIDATION_LABELS = {"LOSO": "LOSO", "within_station": "Within-station", "april": "April"}
MODE_STYLES = {
    "LOSO": {"marker": "o", "ls": "-", "color": "#444444"},
    "within_station": {"marker": "s", "ls": "--", "color": "#3a7d3a"},
    "april": {"marker": "^", "ls": ":", "color": "#a02c2c"},
}


LEADS = (1, 6, 24, 48, 72)


def setup_style():
    plt.rcParams.update({
        "figure.dpi": 100,
        "savefig.dpi": 300,
        "font.size": 10,
        "axes.titlesize": 11,
        "axes.labelsize": 10,
        "xtick.labelsize": 9,
        "ytick.labelsize": 9,
        "legend.fontsize": 9,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.grid": True,
        "grid.alpha": 0.3,
    })


def load_master():
    return pd.read_csv(OUTPUT_DIR / "master_metrics.csv")


def get_metric_for(master, target, mode, lead):
    cell = master[
        (master["target"] == target)
        & (master["validation_mode"] == mode)
        & (master["lead_hours"] == lead)
    ]
    if cell.empty:
        return None
    if target == "uv":
        col = "absolute_mae"
    else:
        col = "mae_reduction_pct"
    if col not in cell.columns:
        return None
    val = cell[col].iloc[0]
    if pd.isna(val):
        return None
    return float(val)


def markdown_table(df):
    try:
        return df.to_markdown(index=False, floatfmt=".2f")
    except Exception:
        formatted = df.copy()
        for col in formatted.columns:
            if pd.api.types.is_numeric_dtype(formatted[col]):
                formatted[col] = formatted[col].map(lambda v: "" if pd.isna(v) else f"{v:.2f}")
        headers = list(formatted.columns)
        rows = formatted.astype(str).values.tolist()
        lines = ["| " + " | ".join(headers) + " |"]
        lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
        for row in rows:
            lines.append("| " + " | ".join(row) + " |")
        return "\n".join(lines)


def fig_horizon_curves(master):
    targets = list(WINNING_CONFIG.keys())
    fig, axes = plt.subplots(2, 4, figsize=(16, 8), constrained_layout=True)
    for idx, target in enumerate(targets):
        ax = axes[idx // 4, idx % 4]
        for mode in VALIDATION_MODES:
            ys = [get_metric_for(master, target, mode, l) for l in LEADS]
            x_plot = [l for l, y in zip(LEADS, ys) if y is not None]
            y_plot = [y for y in ys if y is not None]
            if not y_plot:
                continue
            style = MODE_STYLES[mode]
            ax.plot(x_plot, y_plot,
                    marker=style["marker"], linestyle=style["ls"],
                    color=style["color"], label=VALIDATION_LABELS[mode],
                    linewidth=1.6, markersize=5)
        if target == "uv":
            ax.set_ylabel("Absolute MAE\n(UV-index points)")
            ax.set_title(f"{target}  ({WINNING_CONFIG[target]['model']})")
        elif target == "wind_direction":
            ax.set_ylabel("Circular MAE\nReduction (%)")
            ax.set_title(f"{target}  ({WINNING_CONFIG[target]['model']})")
            ax.axhline(0, color="grey", linewidth=0.6, alpha=0.6)
        else:
            ax.set_ylabel("MAE Reduction (%)")
            ax.set_title(f"{target}  ({WINNING_CONFIG[target]['model']})")
            ax.axhline(0, color="grey", linewidth=0.6, alpha=0.6)
        ax.set_xlabel("Lead time (hours)")
        ax.set_xticks(LEADS)
        if idx == 0:
            ax.legend(loc="best", framealpha=0.9)

    fig.suptitle("Horizon curves by target and validation mode", fontsize=13, y=1.02)
    out = FIGURE_DIR / "G1_horizon_curves.png"
    fig.savefig(out, bbox_inches="tight")
    plt.close(fig)
    print(f"  G1: {out}")


def fig_winner_bars(master):
    targets = list(WINNING_CONFIG.keys())
    headlines = []
    labels = []
    colors = []
    for target in targets:
        if target == "uv":
            v = get_metric_for(master, target, "LOSO", 1)
            v_april = get_metric_for(master, target, "april", 1)
            label_top = f"abs MAE\n{v:.2f} / {v_april:.2f}" if v is not None and v_april is not None else "abs MAE\nN/A"
            value_for_plot = (1.0 - v / 1.5) * 100.0 if v is not None else 0
        else:
            v = get_metric_for(master, target, "LOSO", 1)
            label_top = f"{v:+.1f}%" if v is not None else "N/A"
            value_for_plot = v if v is not None else 0
        headlines.append(value_for_plot)
        labels.append(label_top)
        colors.append(WINNING_CONFIG[target]["color"])

    fig, ax = plt.subplots(figsize=(11, 5.5), constrained_layout=True)
    x = np.arange(len(targets))
    bars = ax.bar(x, headlines, color=colors)
    ax.set_xticks(x)
    ax.set_xticklabels([t.replace("_", "\n") for t in targets], rotation=0)
    ax.set_ylabel("Headline performance (LOSO L1)")
    ax.set_title("Per-target L1 LOSO performance (HGB = blue, Ridge = orange)")
    for bar, label in zip(bars, labels):
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width() / 2., height + 1,
                label, ha="center", va="bottom", fontsize=9)
    legend_handles = [
        plt.Rectangle((0, 0), 1, 1, color="#1f77b4", label="HGB"),
        plt.Rectangle((0, 0), 1, 1, color="#ff7f0e", label="Ridge"),
    ]
    ax.legend(handles=legend_handles, loc="upper right")
    ax.set_ylim(bottom=min(0, min(headlines) - 5))
    out = FIGURE_DIR / "G2_winner_bars.png"
    fig.savefig(out, bbox_inches="tight")
    plt.close(fig)
    print(f"  G2: {out}")


def fig_baseline_vs_corrected_paired(master):
    targets_with_units = [
        ("temperature", "C"),
        ("relative_humidity", "%"),
        ("dew_point", "C"),
        ("wind_speed", "km/h"),
        ("wind_gust", "km/h"),
        ("pressure", "hPa"),
        ("wind_direction", "deg"),
    ]
    fig, axes = plt.subplots(2, 4, figsize=(16, 8), constrained_layout=True)
    for idx, (target, unit) in enumerate(targets_with_units):
        ax = axes[idx // 4, idx % 4]
        labels = ["L1", "L72"]
        baselines = []
        correcteds = []
        for lead in (1, 72):
            cell = master[
                (master["target"] == target)
                & (master["validation_mode"] == "LOSO")
                & (master["lead_hours"] == lead)
            ]
            if cell.empty:
                baselines.append(0)
                correcteds.append(0)
                continue
            baselines.append(float(cell["baseline_mae"].iloc[0]) if "baseline_mae" in cell.columns else 0)
            correcteds.append(float(cell["corrected_mae"].iloc[0]) if "corrected_mae" in cell.columns else 0)
        x = np.arange(len(labels))
        width = 0.35
        ax.bar(x - width / 2, baselines, width, label="Baseline (Open-Meteo)", color="#888888")
        ax.bar(x + width / 2, correcteds, width, label="Corrected (model)", color=WINNING_CONFIG[target]["color"])
        ax.set_xticks(x)
        ax.set_xticklabels(labels)
        ax.set_ylabel(f"MAE ({unit})")
        ax.set_title(target)
        if idx == 0:
            ax.legend(loc="best", framealpha=0.9)

    if len(targets_with_units) < 8:
        axes.flat[-1].axis("off")
    fig.suptitle("Baseline vs corrected MAE at L1 and L72 (LOSO)", fontsize=13, y=1.02)
    out = FIGURE_DIR / "G3_baseline_vs_corrected.png"
    fig.savefig(out, bbox_inches="tight")
    plt.close(fig)
    print(f"  G3: {out}")


def fig_bias_heatmap(master):
    residual_targets = ["temperature", "relative_humidity", "dew_point",
                        "wind_speed", "wind_gust", "pressure"]
    matrix = np.zeros((len(residual_targets), len(LEADS)))
    for i, target in enumerate(residual_targets):
        for j, lead in enumerate(LEADS):
            cell = master[
                (master["target"] == target)
                & (master["validation_mode"] == "LOSO")
                & (master["lead_hours"] == lead)
            ]
            if cell.empty or "bias_correction_pct" not in cell.columns:
                matrix[i, j] = np.nan
                continue
            v = cell["bias_correction_pct"].iloc[0]
            matrix[i, j] = float(v) if not pd.isna(v) else np.nan

    fig, ax = plt.subplots(figsize=(8, 5), constrained_layout=True)
    cmap = plt.cm.RdYlGn
    im = ax.imshow(matrix, aspect="auto", cmap=cmap, vmin=0, vmax=100)
    ax.set_xticks(np.arange(len(LEADS)))
    ax.set_xticklabels([f"L{l}" for l in LEADS])
    ax.set_yticks(np.arange(len(residual_targets)))
    ax.set_yticklabels(residual_targets)
    ax.set_title("Bias correction (%) by target x lead (LOSO)")
    for i in range(matrix.shape[0]):
        for j in range(matrix.shape[1]):
            v = matrix[i, j]
            if pd.isna(v):
                ax.text(j, i, "N/A", ha="center", va="center", fontsize=8, color="black")
            else:
                ax.text(j, i, f"{v:.0f}%", ha="center", va="center", fontsize=9,
                        color="black" if 30 < v < 70 else "white")
    fig.colorbar(im, ax=ax, label="bias correction %")
    out = FIGURE_DIR / "G4_bias_heatmap.png"
    fig.savefig(out, bbox_inches="tight")
    plt.close(fig)
    print(f"  G4: {out}")


def fig_validation_modes(master):
    targets = list(WINNING_CONFIG.keys())
    fig, ax = plt.subplots(figsize=(12, 6), constrained_layout=True)
    x = np.arange(len(targets))
    width = 0.25
    mode_offsets = {"LOSO": -width, "within_station": 0, "april": width}
    mode_colors = {m: MODE_STYLES[m]["color"] for m in VALIDATION_MODES}

    for mode in VALIDATION_MODES:
        ys = []
        for target in targets:
            v = get_metric_for(master, target, mode, 1)
            if target == "uv" and v is not None:
                v = (1.0 - v / 1.5) * 100
            ys.append(v if v is not None else 0)
        ax.bar(x + mode_offsets[mode], ys, width, label=VALIDATION_LABELS[mode],
               color=mode_colors[mode], alpha=0.85)

    ax.set_xticks(x)
    ax.set_xticklabels([t.replace("_", "\n") for t in targets])
    ax.set_ylabel("L1 metric (% or normalized for UV)")
    ax.set_title("L1 performance across validation modes (UV normalized)")
    ax.legend()
    ax.axhline(0, color="grey", linewidth=0.6, alpha=0.6)
    out = FIGURE_DIR / "G5_validation_modes.png"
    fig.savefig(out, bbox_inches="tight")
    plt.close(fig)
    print(f"  G5: {out}")


def fig_uv_normalized():
    uv_path = OUTPUT_DIR / "uv_normalized.json"
    if not uv_path.exists():
        print("  G6: skipped (uv_normalized.json missing)")
        return
    with uv_path.open() as f:
        uv = json.load(f)
    rows = uv.get("rows", [])
    if not rows:
        print("  G6: skipped (no rows in uv_normalized.json)")
        return

    df = pd.DataFrame(rows)
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5), constrained_layout=True)

    for mode in ("LOSO", "within_station", "april"):
        sub = df[df["mode"] == mode].sort_values("lead")
        if sub.empty:
            continue
        style = MODE_STYLES.get(mode, {"marker": "o", "ls": "-", "color": "#444"})
        ax1.plot(sub["lead"], sub["absolute_mae"],
                 marker=style["marker"], linestyle=style["ls"], color=style["color"],
                 label=VALIDATION_LABELS.get(mode, mode), linewidth=1.6)
        ax2.plot(sub["lead"], sub["pct_of_mean"],
                 marker=style["marker"], linestyle=style["ls"], color=style["color"],
                 label=VALIDATION_LABELS.get(mode, mode), linewidth=1.6)
    ax1.set_xlabel("Lead (hours)")
    ax1.set_ylabel("Absolute MAE (UV-index points)")
    ax1.set_title("UV: absolute error")
    ax1.legend()
    ax1.set_xticks(LEADS)
    ax2.set_xlabel("Lead (hours)")
    ax2.set_ylabel("MAE as % of typical UV magnitude")
    ax2.set_title("UV: relative error")
    ax2.legend()
    ax2.set_xticks(LEADS)
    fig.suptitle("UV: same skill, different scales", fontsize=13, y=1.04)
    out = FIGURE_DIR / "G6_uv_normalized.png"
    fig.savefig(out, bbox_inches="tight")
    plt.close(fig)
    print(f"  G6: {out}")


def render_summary_table_png():
    summary_path = OUTPUT_DIR / "summary_table.csv"
    if not summary_path.exists():
        print("  summary table PNG: skipped (CSV missing)")
        return
    df = pd.read_csv(summary_path)

    cell_cols = [c for c in df.columns if c.startswith(("LOSO_L", "within_station_L", "april_L"))]
    other_cols = [c for c in df.columns if c not in cell_cols]
    df = df[other_cols + cell_cols]

    def fmt_cell(target, col, val):
        if pd.isna(val):
            return "-"
        if target == "uv":
            return f"{val:.2f}"
        return f"{val:+.1f}%"

    formatted = df.copy()
    for col in cell_cols:
        formatted[col] = formatted.apply(lambda r: fmt_cell(r["target"], col, r[col]), axis=1)

    fig, ax = plt.subplots(figsize=(20, 5.5))
    ax.axis("off")
    short_cols = []
    for c in formatted.columns:
        if c.startswith("LOSO_L"):
            short_cols.append("LOSO " + c.split("_L")[-1] + "h")
        elif c.startswith("within_station_L"):
            short_cols.append("WS " + c.split("_L")[-1] + "h")
        elif c.startswith("april_L"):
            short_cols.append("Apr " + c.split("_L")[-1] + "h")
        else:
            short_cols.append(c)
    tbl = ax.table(cellText=formatted.values, colLabels=short_cols,
                   loc="center", cellLoc="center", colLoc="center")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(9)
    tbl.scale(1.0, 1.6)
    for j, c in enumerate(formatted.columns):
        cell = tbl[0, j]
        cell.set_facecolor("#dddddd")
        cell.set_text_props(weight="bold")
    for i, row in enumerate(formatted.itertuples(index=False), start=1):
        target = row.target
        is_hgb = WINNING_CONFIG[target]["model"] == "HGB"
        row_color = "#e8f0fb" if is_hgb else "#fceee0"
        for j in range(len(formatted.columns)):
            tbl[i, j].set_facecolor(row_color)
    out = FIGURE_DIR / "summary_table.png"
    fig.savefig(out, bbox_inches="tight", dpi=300)
    plt.close(fig)
    print(f"  summary table: {out}")


def write_deliverable_md(master):
    summary_path = OUTPUT_DIR / "summary_table.csv"
    summary = pd.read_csv(summary_path) if summary_path.exists() else None

    n_loso = master[master["validation_mode"] == "LOSO"].shape[0]
    n_ws = master[master["validation_mode"] == "within_station"].shape[0]
    n_apr = master[master["validation_mode"] == "april"].shape[0]

    lines = []
    lines.append("# Mozn AI Weather Post-Processing - Block 2 Deliverable")
    lines.append("")
    lines.append("## Overview")
    lines.append("")
    lines.append("Eight weather targets post-processed via station-residual modelling on top of Open-Meteo baseline forecasts. Targets ship at all 72 horizons (lead 1h to 72h) with per-target winning model class locked from a multi-model sweep.")
    lines.append("")
    lines.append("**Shipping targets:** temperature, relative humidity, dew point, wind speed, wind gust, pressure, UV index, wind direction.")
    lines.append("")
    lines.append("**Excluded from v1:** rain occurrence and rain amount. See `rain_appendix.md`.")
    lines.append("")
    lines.append("## Validation methodology")
    lines.append("")
    lines.append("Three independent validations were performed:")
    lines.append("")
    lines.append("1. **LOSO (Leave-One-Station-Out):** 26-fold per target x lead. Conservative worst-case - deploying at a brand-new station.")
    lines.append("2. **Within-station hourly hold-out:** 10% of hours per station held out in 48-hour blocks. Production-like - known stations, unseen hours.")
    lines.append("3. **April 2026 out-of-sample:** entire month of April held back from training. Validates temporal generalization. Built from same archive_api source as training canonical.")
    lines.append("")
    lines.append("Each validation was run at leads 1h, 6h, 24h, 48h, 72h. Full 72-lead production artifacts trained on the complete canonical (no holdout) for Block 3 inference.")
    lines.append("")
    lines.append("## Summary table")
    lines.append("")
    lines.append("Per-target MAE reduction (%) at each lead under each validation mode. UV reported as absolute MAE in UV-index points. See `summary_table.png` for a colour-coded version.")
    lines.append("")
    if summary is not None:
        lines.append(markdown_table(summary))
        lines.append("")

    lines.append("## Headline findings")
    lines.append("")
    lines.append("- **Strongest performers (LOSO L1):** pressure, wind gust, relative humidity, wind speed, dew point, temperature, and wind direction all improve over baseline. UV gets corrected MAE near 0.30 UV-index points.")
    lines.append("- **Bias correction:** five of six residual targets show large bias correction at L1. Relative humidity baseline bias was already near zero, so percentage bias correction is not always meaningful.")
    lines.append("- **Across all three validations:** April out-of-sample numbers land between LOSO (conservative) and within-station (optimistic) for most targets. UV is consistent across all modes when normalized.")
    lines.append("")
    lines.append("## Limitations and caveats")
    lines.append("")
    lines.append("- **Wind direction LOSO weakens sharply at long leads** under the corrected angle-reconstruction convention. Within-station performance holds, so production behavior at trained stations is fine, but cold-start at new stations expects little useful direction correction beyond short leads.")
    lines.append("- **Pressure long-lead skill drops** at multi-day horizons, consistent with synoptic uncertainty.")
    lines.append("- **April scoring uses archive_api baseline** (same source as training canonical), not true issued historical forecasts. Open-Meteo does not expose hour-resolution archived forecasts. April numbers are training-consistent but not an operational issued-forecast test.")
    lines.append("- **UV reported in two forms:** absolute MAE (UV-index points) and normalized (% of typical UV magnitude). The former is interpretable directly; the latter is comparable across seasons.")
    lines.append("- **Rain investigated separately.** Both rain occurrence and rain amount are persistence-dominated under all tested architectures. See `rain_appendix.md` for three shipping options.")
    lines.append("")
    lines.append("## Block 3 handover")
    lines.append("")
    lines.append("- Locked production artifacts: `models/artifacts/{target}_{model_class}_lead{N}h.joblib`")
    lines.append("- 576 files (8 targets x 72 leads), about 214 MB total")
    lines.append("- Each artifact contains: model, scaler (Ridge only), imputation_stats, feature_columns, target, lead, model_class, hp")
    lines.append("- Wind direction artifacts include output_layout [`winddir_residual_sin`, `winddir_residual_cos`] and reconstruction note `angle_convention`")
    lines.append("- Block 3 inference contract: load by (target, lead), impute features using artifact's imputation_stats, scale (Ridge only), predict, reconstruct residual to absolute using artifact's predicts_residual flag")
    lines.append("")
    lines.append("## Data references")
    lines.append("")
    lines.append(f"- Master metrics CSV: `master_metrics.csv` ({len(master)} rows)")
    lines.append("- Wide summary table: `summary_table.csv` (8 rows)")
    lines.append("- Six charts: `figures/G1_horizon_curves.png` through `figures/G6_uv_normalized.png`")
    lines.append("- Colour-coded summary table image: `figures/summary_table.png`")
    lines.append(f"- Validation row counts: LOSO {n_loso}, within-station {n_ws}, April {n_apr}")
    lines.append("")

    md_path = OUTPUT_DIR / "deliverable_summary.md"
    md_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"  deliverable: {md_path}")


def write_rain_appendix():
    lines = []
    lines.append("# Rain Forecasting - Investigation Appendix")
    lines.append("")
    lines.append("## Decision")
    lines.append("")
    lines.append("Rain occurrence and rain amount are **excluded from v1 shipping models**. They are presented to Mozn as three implementation options for the product team to choose based on business priorities.")
    lines.append("")
    lines.append("## What we found")
    lines.append("")
    lines.append("### Rain occurrence (binary classification, probability output)")
    lines.append("")
    lines.append("- HGB classifier achieves F1=0.868 at L1, 0.385 at L72")
    lines.append("- Naive persistence (predict rain at T+h = rain at T) F1=0.911 at L1, 0.269 at L72")
    lines.append("- **HGB beats persistence at L6+ by 0.03-0.15 F1 points; persistence beats HGB at L1 by 0.04**")
    lines.append("- Binary rain signal is post-processable, just lead-dependent")
    lines.append("")
    lines.append("### Rain amount (conditional regression on positive-rain rows)")
    lines.append("")
    lines.append("- HGB MAE 0.79mm at L1 vs persistence MAE 0.28mm (HGB 2.8x worse than persistence)")
    lines.append("- HGB without persistence features: still loses to persistence at all leads")
    lines.append("- Feature importance showed model relied on rain lags about 100x more than validity-time forecast features")
    lines.append("- **No tested model architecture beats naive persistence at any horizon**")
    lines.append("")
    lines.append("## Root-cause hypothesis")
    lines.append("")
    lines.append("1. ECMWF/Open-Meteo precipitation amount forecasting is genuinely limited in arid regions with convective rain.")
    lines.append("2. Tipping-bucket PWS rain gauges have quantization noise on amount; occurrence is robust to this noise but amount is not.")
    lines.append("3. The positive-rate suggests the canonical's `rain_occurrence` may include drizzle and trace amounts that do not represent meaningful rain events.")
    lines.append("")
    lines.append("Both target and baseline appear compromised by data-quality issues outside the scope of post-processing. A higher-quality rain dataset would be needed for v2 rain models.")
    lines.append("")
    lines.append("## Three shipping options for Mozn")
    lines.append("")
    lines.append("### Option 1 - Don't ship rain in v1")
    lines.append("")
    lines.append("Mozn's app shows rain via Open-Meteo's raw forecast or skips rain entirely. Our system ships the other 8 targets. Simplest and honest about what works.")
    lines.append("")
    lines.append("### Option 2 - Ship persistence for rain (recommended for v1)")
    lines.append("")
    lines.append("Implement on Mozn's side in about 5 lines: `predicted_rain_at_T+h = station_rain_at_T`. No model artifact needed from us. Works well for L1-L6 and degrades gracefully at long leads.")
    lines.append("")
    lines.append("### Option 3 - Hybrid (persistence at L1, HGB at L6+)")
    lines.append("")
    lines.append("For rain occurrence: persistence at lead=1, HGB at longer leads. For rain amount: persistence at all leads. More complex inference but captures HGB's L24-L72 advantage on occurrence.")
    lines.append("")
    lines.append("**Our recommendation: Option 2 (or Option 1).** The hybrid's L6+ wins on rain occurrence are small in operational terms. Simpler shipping plus a future v2 with better data is cleaner.")
    lines.append("")
    lines.append("## What v2 would need")
    lines.append("")
    lines.append("- Cleaner rain amount targets: calibrated tipping-bucket gauges, radar-derived hourly rainfall, or a paid commercial precipitation product")
    lines.append("- Stricter `rain_occurrence` definition, such as >=0.5mm/hour instead of any non-zero amount")
    lines.append("- Possibly switch baseline forecast for rain target only")
    lines.append("")
    lines.append("## Diagnostics retained")
    lines.append("")
    lines.append("- `reports/diagnostics/rain_persistence_diagnostic/`")
    lines.append("- `reports/diagnostics/rain_experiments_ab/`")
    lines.append("- `reports/diagnostics/rain_hgb_classifier_sweep/`")
    lines.append("- `reports/diagnostics/rain_amount_hgb_sweep/`")
    lines.append("")

    md_path = OUTPUT_DIR / "rain_appendix.md"
    md_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"  rain appendix: {md_path}")


def main():
    setup_style()
    FIGURE_DIR.mkdir(parents=True, exist_ok=True)
    master = load_master()
    print(f"Loaded master: {master.shape}")
    print()
    print("Building figures:")
    fig_horizon_curves(master)
    fig_winner_bars(master)
    fig_baseline_vs_corrected_paired(master)
    fig_bias_heatmap(master)
    fig_validation_modes(master)
    fig_uv_normalized()
    render_summary_table_png()
    print()
    print("Building writeups:")
    write_deliverable_md(master)
    write_rain_appendix()
    print()
    print("=== Deliverable complete ===")
    print(f"  Output directory: {OUTPUT_DIR}")
    print(f"  Figures directory: {FIGURE_DIR}")


if __name__ == "__main__":
    main()
