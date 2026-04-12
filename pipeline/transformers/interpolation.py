"""
Linear interpolation for PriceCharting price_history gaps.

Fills gaps of up to `max_gap_days` between known data points using
linear interpolation, marking inserted rows with interpolated=1.
"""

from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

PRICE_COLS = ["raw_price", "psa_7_price", "psa_8_price", "psa_9_price",
              "psa_10_price", "psa_10_vs_raw", "psa_10_vs_raw_pct",
              "sales_volume"]


def interpolate_price_history(db, card_id, max_gap_days=2):
    """Fill small date gaps in price_history with linearly interpolated rows.

    Parameters
    ----------
    db : sqlite3.Connection
        Open database connection (row_factory = sqlite3.Row).
    card_id : str
        Card identifier.
    max_gap_days : int
        Maximum gap size (in days) to fill.  Gaps larger than this are
        left alone and reported in unfilled_gaps.

    Returns
    -------
    dict
        {
            "gaps_filled": int,
            "rows_inserted": int,
            "unfilled_gaps": list[dict]   # [{start, end, gap_days}, ...]
        }
    """
    rows = db.execute(
        """SELECT date, raw_price, psa_7_price, psa_8_price,
                  psa_9_price, psa_10_price, psa_10_vs_raw,
                  psa_10_vs_raw_pct, sales_volume
             FROM price_history
            WHERE card_id = ? AND interpolated = 0
            ORDER BY date""",
        (card_id,),
    ).fetchall()

    if len(rows) < 2:
        return {"gaps_filled": 0, "rows_inserted": 0, "unfilled_gaps": []}

    gaps_filled = 0
    rows_inserted = 0
    unfilled_gaps = []

    for i in range(len(rows) - 1):
        d_start = datetime.strptime(rows[i]["date"], "%Y-%m-%d")
        d_end = datetime.strptime(rows[i + 1]["date"], "%Y-%m-%d")
        gap_days = (d_end - d_start).days

        # No gap
        if gap_days <= 1:
            continue

        # Gap too large — record for _meta warnings
        if gap_days - 1 > max_gap_days:
            unfilled_gaps.append({
                "start": rows[i]["date"],
                "end": rows[i + 1]["date"],
                "gap_days": gap_days - 1,
            })
            continue

        # Interpolate each missing day
        gaps_filled += 1
        for step in range(1, gap_days):
            frac = step / gap_days
            fill_date = (d_start + timedelta(days=step)).strftime("%Y-%m-%d")

            values = {}
            for col in PRICE_COLS:
                v_start = rows[i][col]
                v_end = rows[i + 1][col]
                if v_start is not None and v_end is not None:
                    values[col] = round(v_start + (v_end - v_start) * frac, 4)
                else:
                    values[col] = v_start if v_start is not None else v_end

            db.execute(
                """INSERT OR IGNORE INTO price_history
                   (card_id, date, raw_price, psa_7_price, psa_8_price,
                    psa_9_price, psa_10_price, psa_10_vs_raw,
                    psa_10_vs_raw_pct, sales_volume,
                    interpolated, interpolation_source)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1,
                           'pricecharting-gap-fill')""",
                (card_id, fill_date,
                 values["raw_price"], values["psa_7_price"],
                 values["psa_8_price"], values["psa_9_price"],
                 values["psa_10_price"], values["psa_10_vs_raw"],
                 values["psa_10_vs_raw_pct"], values["sales_volume"]),
            )
            rows_inserted += 1

    logger.info(
        "interpolate card=%s  gaps_filled=%d  rows_inserted=%d  unfilled=%d",
        card_id, gaps_filled, rows_inserted, len(unfilled_gaps),
    )
    return {
        "gaps_filled": gaps_filled,
        "rows_inserted": rows_inserted,
        "unfilled_gaps": unfilled_gaps,
    }
