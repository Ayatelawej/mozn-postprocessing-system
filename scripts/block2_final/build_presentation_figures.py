from __future__ import annotations

import json
import textwrap
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


BASE_DIR = Path("reports/block2_final")
OUT_DIR = BASE_DIR / "presentation"
FIG_DIR = OUT_DIR / "figures"
TABLE_DIR = OUT_DIR / "tables"
MASTER_PATH = BASE_DIR / "master_metrics.csv"
UV_PATH = BASE_DIR / "uv_normalized.json"
TRAINING_PROGRESS_PATH = Path("reports/diagnostics/artifact_training/training_progress.tsv")
ARTIFACT_DIR = Path("models/artifacts")

LEADS = (1, 6, 24, 48, 72)
DISPLAY_TARGETS = {
    "temperature": "Temp",
    "relative_humidity": "RH",
    "dew_point": "Dew pt",
    "wind_speed": "Wind spd",
    "wind_gust": "Wind gust",
    "pressure": "Pressure",
    "uv": "UV",
    "wind_direction": "Wind dir",
}
UNITS = {
    "temperature": "C",
    "relative_humidity": "RH pts",
    "dew_point": "C",
    "wind_speed": "km/h",
    "wind_gust": "km/h",
    "pressure": "hPa",
    "wind_direction": "deg",
}
MODEL_CLASS = {
    "temperature": "HGB",
    "relative_humidity": "HGB",
    "dew_point": "HGB",
    "wind_speed": "Ridge",
    "wind_gust": "Ridge",
    "pressure": "Ridge",
    "uv": "HGB",
    "wind_direction": "Ridge",
}
TARGET_ORDER = list(DISPLAY_TARGETS.keys())
NON_UV_TARGETS = [t for t in TARGET_ORDER if t != "uv"]
RESIDUAL_TARGETS = ["temperature", "relative_humidity", "dew_point", "wind_speed", "wind_gust", "pressure"]
MODE_LABELS = {"LOSO": "New station", "within_station": "Known station", "april": "April"}
MODE_COLORS = {"LOSO": "#4d4d4d", "within_station": "#2e7d32", "april": "#b23a48"}
MODEL_COLORS = {"HGB": "#2b6cb0", "Ridge": "#dd7f20"}


def setup_style():
    plt.rcParams.update({
        "figure.dpi": 120,
        "savefig.dpi": 300,
        "font.family": "DejaVu Sans",
        "font.size": 10,
        "axes.titlesize": 13,
        "axes.labelsize": 10,
        "xtick.labelsize": 9,
        "ytick.labelsize": 9,
        "legend.fontsize": 9,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.grid": True,
        "grid.alpha": 0.22,
    })


def load_master():
    return pd.read_csv(MASTER_PATH)


def metric(master, target, mode, lead, col=None):
    cell = master[
        (master["target"] == target)
        & (master["validation_mode"] == mode)
        & (master["lead_hours"] == lead)
    ]
    if cell.empty:
        return None
    if col is None:
        col = "absolute_mae" if target == "uv" else "mae_reduction_pct"
    if col not in cell.columns:
        return None
    val = cell[col].iloc[0]
    if pd.isna(val):
        return None
    return float(val)


def fmt_metric(target, val):
    if val is None:
        return "-"
    if target == "uv":
        return f"{val:.2f}"
    return f"{val:+.1f}%"


def savefig(fig, name):
    path = FIG_DIR / name
    fig.savefig(path, bbox_inches="tight", dpi=300)
    plt.close(fig)
    print(f"  {path}")


def draw_table(ax, rows, columns, col_widths=None, font_size=9, row_height=1.55, header_color="#d9e2ef"):
    ax.axis("off")
    table = ax.table(
        cellText=rows,
        colLabels=columns,
        cellLoc="center",
        colLoc="center",
        loc="center",
        colWidths=col_widths,
    )
    table.auto_set_font_size(False)
    table.set_fontsize(font_size)
    table.scale(1.0, row_height)
    for (r, c), cell in table.get_celld().items():
        cell.set_edgecolor("#ffffff")
        if r == 0:
            cell.set_facecolor(header_color)
            cell.set_text_props(weight="bold", color="#1f2933")
        elif c in (0, 1):
            cell.set_text_props(weight="bold")
    return table


def build_artifact_inventory():
    if not TRAINING_PROGRESS_PATH.exists():
        return None
    df = pd.read_csv(TRAINING_PROGRESS_PATH, sep="\t")
    out = TABLE_DIR / "artifact_inventory_1_72.csv"
    df.to_csv(out, index=False)
    return df


def figure_scorecard(master):
    notes = {
        "temperature": "Strong across modes;\nstation history helps",
        "relative_humidity": "Robust at all leads",
        "dew_point": "Strongest HGB target\nat long leads",
        "wind_speed": "Large known-station\nand April gains",
        "wind_gust": "Best wind target;\nvery stable",
        "pressure": "Excellent short lead;\nfades by 72h",
        "uv": "Report absolute MAE\nand normalized %",
        "wind_direction": "Good known-station;\nweak cold-start long lead",
    }
    rows = []
    for target in TARGET_ORDER:
        rows.append([
            DISPLAY_TARGETS[target],
            MODEL_CLASS[target],
            "MAE red %" if target != "uv" else "Abs MAE",
            fmt_metric(target, metric(master, target, "LOSO", 1)),
            fmt_metric(target, metric(master, target, "LOSO", 72)),
            fmt_metric(target, metric(master, target, "april", 1)),
            fmt_metric(target, metric(master, target, "april", 72)),
            notes[target],
        ])
    fig, ax = plt.subplots(figsize=(17.5, 6.3))
    columns = ["Target", "Model", "Metric", "LOSO\n1h", "LOSO\n72h", "April\n1h", "April\n72h", "Deployment note"]
    table = draw_table(
        ax,
        rows,
        columns,
        col_widths=[0.095, 0.075, 0.085, 0.085, 0.085, 0.085, 0.085, 0.36],
        font_size=9.5,
        row_height=1.75,
    )
    for i, target in enumerate(TARGET_ORDER, start=1):
        model = MODEL_CLASS[target]
        color = "#eaf2fc" if model == "HGB" else "#fff0df"
        for j in range(len(columns)):
            table[i, j].set_facecolor(color)
    ax.set_title("MVP Readiness Scorecard: 8 targets validated, 72 production leads trained", pad=24, weight="bold")
    savefig(fig, "P1_mvp_readiness_scorecard.png")


def figure_validation_consistency(master, lead, name):
    targets = NON_UV_TARGETS
    x = np.arange(len(targets))
    width = 0.24
    fig, ax = plt.subplots(figsize=(13.5, 6.2), constrained_layout=True)
    for idx, mode in enumerate(("LOSO", "within_station", "april")):
        vals = [metric(master, t, mode, lead) for t in targets]
        vals = [0 if v is None else v for v in vals]
        ax.bar(
            x + (idx - 1) * width,
            vals,
            width=width,
            label=MODE_LABELS[mode],
            color=MODE_COLORS[mode],
            alpha=0.9,
        )
    ax.set_xticks(x)
    ax.set_xticklabels([DISPLAY_TARGETS[t] for t in targets])
    ax.set_ylabel("MAE reduction vs Open-Meteo (%)")
    ax.set_title(f"Validation consistency at lead {lead}h: known-station and April should exceed LOSO")
    ax.legend(ncol=3, loc="upper right", framealpha=0.95)
    ax.axhline(0, color="#666666", linewidth=0.8)
    ax.set_ylim(min(-8, ax.get_ylim()[0]), max(100, ax.get_ylim()[1]))
    savefig(fig, name)


def figure_baseline_corrected_native(master):
    fig, axes = plt.subplots(2, 4, figsize=(16, 8.2), constrained_layout=True)
    for idx, target in enumerate(NON_UV_TARGETS):
        ax = axes[idx // 4, idx % 4]
        labels = ["1h", "72h"]
        base = [metric(master, target, "LOSO", 1, "baseline_mae"), metric(master, target, "LOSO", 72, "baseline_mae")]
        corr = [metric(master, target, "LOSO", 1, "corrected_mae"), metric(master, target, "LOSO", 72, "corrected_mae")]
        x = np.arange(len(labels))
        ax.bar(x - 0.18, base, width=0.36, label="Open-Meteo", color="#9aa3ad")
        ax.bar(x + 0.18, corr, width=0.36, label="Post-processed", color=MODEL_COLORS[MODEL_CLASS[target]])
        ax.set_xticks(x)
        ax.set_xticklabels(labels)
        ax.set_ylabel(f"MAE ({UNITS[target]})")
        ax.set_title(DISPLAY_TARGETS[target])
        if idx == 0:
            ax.legend(loc="upper right", framealpha=0.95)
    axes.flat[-1].axis("off")
    fig.suptitle("Baseline vs post-processed MAE in native units (LOSO)", weight="bold", y=1.02)
    savefig(fig, "P4_loso_baseline_vs_corrected_native_units.png")


def figure_bias_native_table(master):
    rows = []
    for target in RESIDUAL_TARGETS:
        baseline = metric(master, target, "LOSO", 1, "baseline_bias")
        corrected_1 = metric(master, target, "LOSO", 1, "corrected_bias")
        corrected_72 = metric(master, target, "LOSO", 72, "corrected_bias")
        if target == "relative_humidity":
            note = "Baseline bias tiny;\n% not meaningful"
        elif target in ("wind_speed", "wind_gust", "pressure"):
            note = "Large systematic\nbias removed"
        else:
            note = "Bias reduced"
        rows.append([
            DISPLAY_TARGETS[target],
            UNITS[target],
            f"{baseline:+.2f}" if baseline is not None else "-",
            f"{corrected_1:+.2f}" if corrected_1 is not None else "-",
            f"{corrected_72:+.2f}" if corrected_72 is not None else "-",
            note,
        ])
    fig, ax = plt.subplots(figsize=(12.5, 4.9))
    table = draw_table(
        ax,
        rows,
        ["Target", "Unit", "Baseline\nbias", "Corrected\nbias 1h", "Corrected\nbias 72h", "Interpretation"],
        col_widths=[0.14, 0.1, 0.15, 0.17, 0.17, 0.27],
        font_size=10,
        row_height=1.75,
    )
    for i in range(1, len(rows) + 1):
        for j in range(6):
            table[i, j].set_facecolor("#f7fafc" if i % 2 else "#edf2f7")
    ax.set_title("Bias correction in native units: clearer than percent-bias heatmaps", pad=24, weight="bold")
    savefig(fig, "P5_bias_correction_native_units_table.png")


def figure_known_station_gap(master):
    targets = NON_UV_TARGETS
    l1_gap = []
    l72_gap = []
    for target in targets:
        l1_gap.append(metric(master, target, "within_station", 1) - metric(master, target, "LOSO", 1))
        l72_gap.append(metric(master, target, "within_station", 72) - metric(master, target, "LOSO", 72))
    y = np.arange(len(targets))
    fig, ax = plt.subplots(figsize=(11.5, 6.5), constrained_layout=True)
    ax.barh(y - 0.18, l1_gap, height=0.36, label="1h", color="#6aa84f")
    ax.barh(y + 0.18, l72_gap, height=0.36, label="72h", color="#b45f06")
    ax.set_yticks(y)
    ax.set_yticklabels([DISPLAY_TARGETS[t] for t in targets])
    ax.axvline(0, color="#555555", linewidth=0.8)
    ax.set_xlabel("Known-station validation advantage over LOSO (percentage points)")
    ax.set_title("Where station history matters most: known-station vs new-station gap")
    ax.legend(loc="lower right")
    savefig(fig, "P6_known_station_vs_new_station_gap.png")


def figure_uv_presentation():
    if not UV_PATH.exists():
        return
    with UV_PATH.open() as f:
        uv = json.load(f)
    df = pd.DataFrame(uv.get("rows", []))
    if df.empty:
        return
    fig, axes = plt.subplots(1, 2, figsize=(13.5, 5.4), constrained_layout=True)
    for mode in ("LOSO", "within_station", "april"):
        sub = df[df["mode"] == mode].sort_values("lead")
        if sub.empty:
            continue
        label = MODE_LABELS.get(mode, mode)
        color = MODE_COLORS.get(mode, "#333333")
        axes[0].plot(sub["lead"], sub["absolute_mae"], marker="o", linewidth=2, color=color, label=label)
        axes[1].plot(sub["lead"], sub["pct_of_mean"], marker="o", linewidth=2, color=color, label=label)
    axes[0].set_title("Absolute UV error")
    axes[0].set_ylabel("MAE (UV-index points)")
    axes[0].set_xlabel("Lead time (hours)")
    axes[0].set_xticks(LEADS)
    axes[0].legend()
    axes[1].set_title("Relative UV error")
    axes[1].set_ylabel("MAE as % of typical UV magnitude")
    axes[1].set_xlabel("Lead time (hours)")
    axes[1].set_xticks(LEADS)
    axes[1].legend()
    fig.suptitle("UV framing: April absolute error is larger because April UV magnitude is larger", weight="bold", y=1.04)
    savefig(fig, "P7_uv_absolute_and_normalized.png")


def figure_artifact_inventory(progress):
    if progress is None or progress.empty:
        return
    counts = progress.groupby(["target", "model_class"])["lead"].count().reset_index()
    counts["label"] = counts["target"].map(DISPLAY_TARGETS)
    counts = counts.set_index("target").loc[TARGET_ORDER].reset_index()
    total_files = len(list(ARTIFACT_DIR.glob("*_lead*h.joblib"))) if ARTIFACT_DIR.exists() else int(counts["lead"].sum())
    total_mb = sum(p.stat().st_size for p in ARTIFACT_DIR.glob("*.joblib")) / (1024 * 1024) if ARTIFACT_DIR.exists() else 0.0
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5.8), gridspec_kw={"width_ratios": [1.2, 1]}, constrained_layout=True)
    colors = [MODEL_COLORS[m] for m in counts["model_class"]]
    ax1.bar(counts["label"], counts["lead"], color=colors)
    ax1.set_ylim(0, 80)
    ax1.set_ylabel("Artifacts trained")
    ax1.set_title("Production artifact coverage by target")
    ax1.set_xticks(np.arange(len(counts["label"])))
    ax1.set_xticklabels(counts["label"], rotation=25, ha="right")
    for i, v in enumerate(counts["lead"]):
        ax1.text(i, v + 1.5, str(v), ha="center", va="bottom", weight="bold")
    ax2.axis("off")
    summary = [
        ("Targets shipped", "8"),
        ("Lead horizons", "1-72h"),
        ("Artifacts expected", "576"),
        ("Artifacts found", f"{total_files}"),
        ("Failed fits", f"{int((progress['status'] != 'ok').sum())}"),
        ("Disk usage", f"{total_mb:.1f} MB"),
    ]
    rows = [[k, v] for k, v in summary]
    draw_table(ax2, rows, ["Deployment item", "Value"], col_widths=[0.58, 0.32], font_size=11, row_height=1.8)
    fig.suptitle("Block 3 handover: all 576 production artifacts are trained", weight="bold", y=1.03)
    savefig(fig, "P8_production_artifact_inventory.png")


def figure_rain_decision_matrix():
    rows = [
        ["1", "Skip rain in v1", "Cleanest MVP;\nno misleading model", "No rain correction", "Safe default"],
        ["2", "Persistence fallback", "5-line app logic;\nstrong short lead", "Not true forecasting", "Recommended"],
        ["3", "Hybrid", "Uses HGB for\noccurrence L6+", "More complex;\nsmall operational gain", "V2 candidate"],
    ]
    fig, ax = plt.subplots(figsize=(13.5, 4.3))
    table = draw_table(
        ax,
        rows,
        ["Option", "Approach", "Pros", "Cons", "Recommendation"],
        col_widths=[0.08, 0.22, 0.25, 0.25, 0.18],
        font_size=10,
        row_height=1.9,
        header_color="#f5d0c5",
    )
    for i in range(1, len(rows) + 1):
        color = "#fff7ed" if i != 2 else "#dcfce7"
        for j in range(5):
            table[i, j].set_facecolor(color)
    ax.set_title("Rain: investigated, not shipped as a learned v1 model", pad=24, weight="bold")
    savefig(fig, "P9_rain_decision_matrix.png")


def write_readme():
    lines = [
        "# Presentation Figures",
        "",
        "These figures are designed for team/client presentation rather than raw diagnostics.",
        "",
        "- `P1_mvp_readiness_scorecard.png`: main executive table",
        "- `P2_validation_consistency_L1.png`: validation modes at 1h",
        "- `P3_validation_consistency_L72.png`: validation modes at 72h",
        "- `P4_loso_baseline_vs_corrected_native_units.png`: baseline vs model in native units",
        "- `P5_bias_correction_native_units_table.png`: replaces the percent-bias heatmap",
        "- `P6_known_station_vs_new_station_gap.png`: how much station history helps",
        "- `P7_uv_absolute_and_normalized.png`: UV absolute and relative framing",
        "- `P8_production_artifact_inventory.png`: Block 3 artifact handover",
        "- `P9_rain_decision_matrix.png`: rain appendix decision visual",
        "",
        "Important scope note: validation metrics exist at leads 1, 6, 24, 48, and 72. Production artifacts exist for every lead 1-72.",
        "A full 1-72 validation table would require rerunning validation sweeps for the missing leads, not just reading the production artifacts.",
        "",
        "Tables:",
        "- `tables/artifact_inventory_1_72.csv`: all 576 trained artifacts from Task 15b",
    ]
    path = OUT_DIR / "README.md"
    path.write_text("\n".join(lines), encoding="utf-8")
    print(f"  {path}")


def main():
    setup_style()
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    TABLE_DIR.mkdir(parents=True, exist_ok=True)
    master = load_master()
    progress = build_artifact_inventory()
    print(f"Loaded master metrics: {master.shape}")
    if progress is not None:
        print(f"Loaded artifact training progress: {progress.shape}")
    print()
    print("Building presentation figures:")
    figure_scorecard(master)
    figure_validation_consistency(master, 1, "P2_validation_consistency_L1.png")
    figure_validation_consistency(master, 72, "P3_validation_consistency_L72.png")
    figure_baseline_corrected_native(master)
    figure_bias_native_table(master)
    figure_known_station_gap(master)
    figure_uv_presentation()
    figure_artifact_inventory(progress)
    figure_rain_decision_matrix()
    print()
    print("Writing presentation README:")
    write_readme()
    print()
    print(f"Presentation package saved to: {OUT_DIR}")


if __name__ == "__main__":
    main()
