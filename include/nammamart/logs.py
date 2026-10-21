"""
include/nammamart/logs.py
-------------------------
Human-readable task logs for both DAGs: titled sections, aligned tables, formatted numbers.
Everything is printed to stdout, which Airflow shows in the task's Logs tab.
"""

WIDTH = 78


def section(title: str) -> None:
    print("\n" + "═" * WIDTH)
    print(f"  {title}")
    print("═" * WIDTH)


def subsection(title: str) -> None:
    print(f"\n── {title} " + "─" * max(0, WIDTH - len(title) - 4))


def num(n) -> str:
    return f"{int(n):,}"


def money(x) -> str:
    x = float(x)
    return f"{'-' if x < 0 else ''}₹{abs(x):,.2f}"


def kv(pairs: dict) -> None:
    """Aligned 'label : value' lines."""
    width = max(len(k) for k in pairs)
    for k, v in pairs.items():
        print(f"  {k:<{width}} : {v}")


def table(rows, columns: list[str] | None = None, max_rows: int | None = None) -> None:
    """Print a list of dicts or a DataFrame as an aligned table (text left, numbers right)."""
    import pandas as pd

    df = rows if isinstance(rows, pd.DataFrame) else pd.DataFrame(rows, columns=columns)
    if columns:
        df = df[columns]
    if df.empty:
        print("  (no rows)")
        return
    shown = df if max_rows is None else df.head(max_rows)
    cells = shown.astype(object).where(shown.notna(), "").astype(str)
    numeric = [pd.api.types.is_numeric_dtype(df[c]) or cells[c].str.match(r"^[₹\-\d,.\s/%]+$").all()
               for c in cells.columns]
    widths = [max(len(str(c)), cells[c].str.len().max()) for c in cells.columns]

    def line(values):
        return "  " + "  ".join(f"{v:>{w}}" if right else f"{v:<{w}}"
                                for v, w, right in zip(values, widths, numeric))

    print(line(cells.columns))
    print("  " + "  ".join("─" * w for w in widths))
    for _, r in cells.iterrows():
        print(line(r.tolist()))
    if max_rows is not None and len(df) > max_rows:
        print(f"  … {len(df) - max_rows:,} more rows (full table in the warehouse)")
