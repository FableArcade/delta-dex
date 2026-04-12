"""Sealed product leaderboard endpoint."""

from collections import defaultdict
from fastapi import APIRouter, Depends
from api.deps import get_db_conn

router = APIRouter()


@router.get("/sealed_leaderboard")
def sealed_leaderboard(db=Depends(get_db_conn)):
    """Sealed products grouped by sealed_type with latest prices."""

    rows = db.execute("""
        SELECT
            c.id, c.product_name, c.set_code, c.sealed_type,
            c.image_url,
            ph.date, ph.raw_price, ph.psa_10_price,
            ph.sales_volume
        FROM cards c
        LEFT JOIN price_history ph
            ON ph.card_id = c.id
            AND ph.date = (SELECT MAX(date) FROM price_history WHERE card_id = c.id)
        WHERE c.sealed_product = 'Y'
        ORDER BY c.sealed_type, c.product_name
    """).fetchall()

    # Get generation date from latest price_history entry among sealed
    generated_at = None
    grouped: dict[str, list] = defaultdict(list)

    for r in rows:
        if r["date"] and (generated_at is None or r["date"] > generated_at):
            generated_at = r["date"]

        grouped[r["sealed_type"]].append({
            "id": r["id"],
            "product-name": r["product_name"],
            "set-code": r["set_code"],
            "sealed-type": r["sealed_type"],
            "image-url": r["image_url"],
            "date": r["date"],
            "raw-price": r["raw_price"],
            "psa-10-price": r["psa_10_price"],
            "sales-volume": r["sales_volume"],
        })

    sealed_type_result = {}
    for stype, items in sorted(grouped.items()):
        sealed_type_result[stype] = {
            "count": len(items),
            "rows": items,
        }

    return {
        "generated-at": generated_at,
        "sealed-type": sealed_type_result,
    }
