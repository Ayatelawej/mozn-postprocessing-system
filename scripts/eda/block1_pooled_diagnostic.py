from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy import stats

from postprocessing.qc.gates import is_trainable
from postprocessing.utils.paths import get_paths

NUMERIC_TARGETS: tuple[tuple[str, str, str], ...] = (
    ("temperature", "temperature_residual_c", "Temperature residual (°C)"),
    ("relative_humidity", "relative_humidity_residual_pct", "RH residual (pct)"),
    ("dew_point", "dew_point_residual_c", "Dew point residual (°C)"),
    ("wind_speed", "wind_speed_residual_kmh", "Wind speed residual (km/h)"),
    ("wind_gust", "wind_gust_residual_kmh", "Wind gust residual (km/h)"),
    ("pressure", "pressure_residual_avg_hpa", "Pressure residual (hPa)"),
    ("wind_direction", "winddir_residual_deg", "Wind direction residual (deg)"),
    ("rain_amount", "rain_amount_log1p", "Rain amount log1p"),
)

BINARY_TARGETS: tuple[tuple[str, str, str], ...] = (
    ("rain_occurrence", "rain_occurrence", "Rain occurrence (binary)"),
)

UV_TARGET = ("uv", "uv_proxy", "UV proxy")


def per_station_stats(df: pd.DataFrame, value_col: str, gate_target: str) -> pd.DataFrame:
    rows = []
    for sid in sorted(df["station_id"].unique()):
        sub = df[df["station_id"] == sid]
        try:
            mask = is_trainable(sub, gate_target, apply_overrides=True)
        except KeyError:
            mask = pd.Series([True] * len(sub), index=sub.index)
        if value_col not in sub.columns:
            continue
        vals = pd.to_numeric(sub.loc[mask, value_col], errors="coerce").dropna()
        elev = sub["elevation_m"].iloc[0] if "elevation_m" in sub.columns else np.nan
        if len(vals) == 0:
            rows.append({"station_id": sid, "elevation_m": elev, "n": 0,
                         "mean": np.nan, "std": np.nan, "p10": np.nan,
                         "p50": np.nan, "p90": np.nan})
            continue
        rows.append({
            "station_id": sid, "elevation_m": elev, "n": len(vals),
            "mean": float(vals.mean()), "std": float(vals.std()),
            "p10": float(vals.quantile(0.1)), "p50": float(vals.quantile(0.5)),
            "p90": float(vals.quantile(0.9)),
        })
    return pd.DataFrame(rows)


def elevation_correlation(per_stat: pd.DataFrame) -> tuple[float, float]:
    df = per_stat.dropna(subset=["mean", "elevation_m"])
    if len(df) < 3:
        return float("nan"), float("nan")
    r, p = stats.pearsonr(df["elevation_m"], df["mean"])
    return float(r), float(p)


def plot_pooled_histogram(values: pd.Series, title: str, xlabel: str, output_path: Path) -> None:
    vals = pd.to_numeric(values, errors="coerce").dropna()
    if len(vals) == 0:
        return
    p1 = vals.quantile(0.01)
    p99 = vals.quantile(0.99)
    plt.figure(figsize=(8, 4))
    plt.hist(vals.clip(p1, p99), bins=80)
    plt.axvline(float(vals.mean()), linestyle="--", color="red",
                label=f"mean={vals.mean():.2f}")
    plt.axvline(float(vals.median()), linestyle=":", color="green",
                label=f"median={vals.median():.2f}")
    plt.title(f"{title}\nn={len(vals):,}, mean={vals.mean():.2f}, std={vals.std():.2f} (clipped to [p1, p99])")
    plt.xlabel(xlabel)
    plt.ylabel("Count")
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path, dpi=80)
    plt.close()


def plot_per_station_overlay(df: pd.DataFrame, value_col: str, gate_target: str,
                             title: str, xlabel: str, output_path: Path) -> None:
    stations = sorted(df["station_id"].unique())
    n = len(stations)
    cols = 6
    rows = (n + cols - 1) // cols
    fig, axes = plt.subplots(rows, cols, figsize=(16, 2.5 * rows), sharex=True, sharey=False)
    axes = np.atleast_2d(axes)

    all_vals = pd.to_numeric(df[value_col], errors="coerce").dropna()
    if len(all_vals) == 0:
        plt.close()
        return
    p1 = all_vals.quantile(0.01)
    p99 = all_vals.quantile(0.99)

    for i, sid in enumerate(stations):
        r, c = divmod(i, cols)
        ax = axes[r, c]
        sub = df[df["station_id"] == sid]
        try:
            mask = is_trainable(sub, gate_target, apply_overrides=True)
        except KeyError:
            mask = pd.Series([True] * len(sub), index=sub.index)
        vals = pd.to_numeric(sub.loc[mask, value_col], errors="coerce").dropna()
        if len(vals) == 0:
            ax.set_title(f"{sid}\n(no data)", fontsize=8)
            ax.axis("off")
            continue
        ax.hist(vals.clip(p1, p99), bins=30)
        elev = int(sub["elevation_m"].iloc[0]) if "elevation_m" in sub.columns else "?"
        ax.set_title(f"{sid} ({elev}m)\nμ={vals.mean():.1f} σ={vals.std():.1f}", fontsize=8)
        ax.tick_params(labelsize=6)

    for i in range(n, rows * cols):
        r, c = divmod(i, cols)
        axes[r, c].axis("off")

    fig.suptitle(title, y=1.0, fontsize=12)
    plt.tight_layout()
    plt.savefig(output_path, dpi=80)
    plt.close()


def plot_elevation_scatter(per_stat: pd.DataFrame, title: str, output_path: Path) -> None:
    df = per_stat.dropna(subset=["mean", "elevation_m"])
    if len(df) < 3:
        return
    r, p = stats.pearsonr(df["elevation_m"], df["mean"])
    plt.figure(figsize=(8, 5))
    plt.scatter(df["elevation_m"], df["mean"])
    for _, row in df.iterrows():
        plt.annotate(row["station_id"], (row["elevation_m"], row["mean"]),
                     fontsize=7, alpha=0.7)
    plt.axhline(0, linestyle="--", color="gray", alpha=0.5)
    plt.title(f"{title}\nPearson r = {r:.3f}, p = {p:.4f}")
    plt.xlabel("Station elevation (m)")
    plt.ylabel("Per-station mean residual")
    plt.tight_layout()
    plt.savefig(output_path, dpi=80)
    plt.close()


def df_to_md_table(df: pd.DataFrame) -> str:
    if len(df) == 0:
        return "_(empty)_"
    cols = list(df.columns)
    lines = ["| " + " | ".join(cols) + " |"]
    lines.append("| " + " | ".join(["---"] * len(cols)) + " |")
    for _, row in df.iterrows():
        cells = []
        for c in cols:
            v = row[c]
            if isinstance(v, float):
                if pd.isna(v):
                    cells.append("nan")
                else:
                    cells.append(f"{v:.2f}")
            else:
                cells.append(str(v))
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def section_for_numeric_target(df: pd.DataFrame, target: str, value_col: str,
                               label: str, fig_dir: Path) -> tuple[str, dict[str, Any]]:
    per_stat = per_station_stats(df, value_col, target).sort_values("mean")
    r, p = elevation_correlation(per_stat)

    pooled_path = fig_dir / f"{target}_pooled_hist.png"
    overlay_path = fig_dir / f"{target}_per_station.png"
    scatter_path = fig_dir / f"{target}_elevation_scatter.png"

    try:
        mask = is_trainable(df, target, apply_overrides=True)
    except KeyError:
        mask = pd.Series([True] * len(df), index=df.index)
    pooled_vals = pd.to_numeric(df.loc[mask, value_col], errors="coerce").dropna()

    plot_pooled_histogram(pooled_vals, f"Pooled: {label}", label, pooled_path)
    plot_per_station_overlay(df, value_col, target, f"Per-station: {label}", label, overlay_path)
    plot_elevation_scatter(per_stat, f"Mean residual vs elevation: {label}", scatter_path)

    summary = {
        "target": target,
        "label": label,
        "n_pooled": int(len(pooled_vals)),
        "pooled_mean": float(pooled_vals.mean()) if len(pooled_vals) else float("nan"),
        "pooled_std": float(pooled_vals.std()) if len(pooled_vals) else float("nan"),
        "elev_r": r,
        "elev_p": p,
        "n_stations": int(len(per_stat)),
        "outlier_count": int(((per_stat["mean"] - per_stat["mean"].median()).abs() > 3 * per_stat["mean"].std()).sum()) if per_stat["mean"].notna().sum() > 3 else 0,
    }

    md_lines = [f"## {label}", ""]
    md_lines.append(f"Pooled n = {summary['n_pooled']:,}, mean = {summary['pooled_mean']:.3f}, std = {summary['pooled_std']:.3f}.")
    md_lines.append(f"Elevation correlation r = {r:.3f} (p = {p:.4f}).")
    if summary["outlier_count"] > 0:
        md_lines.append(f"**{summary['outlier_count']} station(s) with mean > 3σ from network median.**")
    md_lines.append("")
    md_lines.append("### Per-station statistics")
    md_lines.append("")
    md_lines.append(df_to_md_table(per_stat))
    md_lines.append("")
    md_lines.append("### Pooled distribution")
    md_lines.append("")
    md_lines.append(f"![pooled]({pooled_path.relative_to(fig_dir.parent).as_posix()})")
    md_lines.append("")
    md_lines.append("### Per-station distributions")
    md_lines.append("")
    md_lines.append(f"![per-station]({overlay_path.relative_to(fig_dir.parent).as_posix()})")
    md_lines.append("")
    md_lines.append("### Mean residual vs elevation")
    md_lines.append("")
    md_lines.append(f"![elevation]({scatter_path.relative_to(fig_dir.parent).as_posix()})")
    md_lines.append("")
    return "\n".join(md_lines), summary


def section_for_rain_occurrence(df: pd.DataFrame, fig_dir: Path) -> tuple[str, dict[str, Any]]:
    rows = []
    for sid in sorted(df["station_id"].unique()):
        sub = df[df["station_id"] == sid]
        mask = is_trainable(sub, "rain_occurrence", apply_overrides=True)
        vals = sub.loc[mask, "rain_occurrence"].dropna()
        elev = sub["elevation_m"].iloc[0] if "elevation_m" in sub.columns else np.nan
        if len(vals) == 0:
            rows.append({"station_id": sid, "elevation_m": elev, "n": 0,
                         "rain_hours": 0, "rain_fraction": np.nan})
            continue
        rain_hours = int(vals.sum())
        rows.append({
            "station_id": sid, "elevation_m": elev, "n": len(vals),
            "rain_hours": rain_hours,
            "rain_fraction": rain_hours / len(vals),
        })
    per_stat = pd.DataFrame(rows).sort_values("rain_fraction", ascending=False)

    plt.figure(figsize=(10, 5))
    plt.bar(per_stat["station_id"], per_stat["rain_fraction"])
    plt.xticks(rotation=90, fontsize=7)
    plt.ylabel("Fraction of trainable hours with rain")
    plt.title("Rain occurrence rate per station")
    plt.tight_layout()
    out_path = fig_dir / "rain_occurrence_per_station.png"
    plt.savefig(out_path, dpi=80)
    plt.close()

    summary = {
        "target": "rain_occurrence",
        "label": "Rain occurrence (binary)",
        "n_pooled": int(per_stat["n"].sum()),
        "pooled_mean": float(per_stat["rain_hours"].sum() / per_stat["n"].sum()) if per_stat["n"].sum() > 0 else float("nan"),
        "pooled_std": float("nan"),
        "elev_r": float("nan"),
        "elev_p": float("nan"),
        "n_stations": len(per_stat),
        "outlier_count": 0,
    }

    md_lines = ["## Rain occurrence (binary)", ""]
    md_lines.append(f"Pooled rain hours: {int(per_stat['rain_hours'].sum()):,} / {int(per_stat['n'].sum()):,} ({summary['pooled_mean']*100:.2f}%).")
    md_lines.append("")
    md_lines.append("### Per-station rain rate")
    md_lines.append("")
    md_lines.append(df_to_md_table(per_stat))
    md_lines.append("")
    md_lines.append(f"![rain rate]({out_path.relative_to(fig_dir.parent).as_posix()})")
    md_lines.append("")
    return "\n".join(md_lines), summary


def main() -> int:
    paths = get_paths()
    parquet_path = paths.data.processed_dir / "canonical_hourly_v1.parquet"
    if not parquet_path.is_file():
        print(f"FAIL: canonical parquet missing at {parquet_path}", file=sys.stderr)
        return 1

    out_dir = paths.project_root / "reports" / "block1_pooled_diagnostic"
    fig_dir = out_dir / "figures"
    out_dir.mkdir(parents=True, exist_ok=True)
    fig_dir.mkdir(parents=True, exist_ok=True)

    print(f"Loading {parquet_path}...")
    df = pd.read_parquet(parquet_path)
    print(f"  {len(df):,} rows × {len(df.columns)} cols, {df['station_id'].nunique()} stations")

    sections: list[str] = []
    summaries: list[dict[str, Any]] = []

    for target, value_col, label in NUMERIC_TARGETS:
        if value_col not in df.columns:
            print(f"  SKIP {target}: column {value_col} missing")
            continue
        print(f"  Generating section for {target}...")
        section, summary = section_for_numeric_target(df, target, value_col, label, fig_dir)
        sections.append(section)
        summaries.append(summary)

    target, value_col, label = UV_TARGET
    if value_col in df.columns:
        print(f"  Generating section for {target}...")
        section, summary = section_for_numeric_target(df, target, value_col, label, fig_dir)
        sections.append(section)
        summaries.append(summary)

    print(f"  Generating section for rain_occurrence...")
    section, summary = section_for_rain_occurrence(df, fig_dir)
    sections.append(section)
    summaries.append(summary)

    summary_table = pd.DataFrame([
        {
            "target": s["target"],
            "n_pooled": s["n_pooled"],
            "pooled_mean": s["pooled_mean"],
            "pooled_std": s["pooled_std"],
            "elev_r": s["elev_r"],
            "n_stations": s["n_stations"],
            "outliers_3sigma": s["outlier_count"],
        }
        for s in summaries
    ])

    header = [
        "# Block 1 Pooled Diagnostic",
        "",
        f"Generated from `data/processed/canonical_hourly_v1.parquet` ({len(df):,} rows, {df['station_id'].nunique()} stations).",
        "",
        "Per-station residual statistics across all 11 v1 targets, with elevation correlations and distributional inspection. Used to identify problematic stations or distributional issues before Block 2 model training.",
        "",
        "Generated by `scripts/eda/block1_pooled_diagnostic.py`. Re-run any time after rebuilding the canonical parquet.",
        "",
        "## Summary across targets",
        "",
        df_to_md_table(summary_table),
        "",
        "Notes:",
        "",
        "- `n_pooled` is the number of trainable rows for that target (overrides applied).",
        "- `elev_r` is the Pearson correlation between per-station mean residual and station elevation. A high |r| (>0.5) suggests an elevation effect we should investigate before training.",
        "- `outliers_3sigma` counts stations whose mean residual is more than 3σ from the network median.",
        "",
        "---",
        "",
    ]

    body = "\n".join(header) + "\n\n".join(sections)
    out_path = out_dir / "block1_pooled_diagnostic.md"
    out_path.write_text(body, encoding="utf-8")

    print()
    print(f"Report written: {out_path}")
    print(f"Figures: {fig_dir}")
    print()
    print("Summary across targets:")
    print(summary_table.to_string(index=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
