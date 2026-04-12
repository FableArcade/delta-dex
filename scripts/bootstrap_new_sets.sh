#!/bin/zsh
# Bootstrap price history + images for all newly-seeded 2016-2022 cards.
# Uses --card-id to force processing (--resume would skip these since they
# already have pc_canonical_url set).

cd "$(dirname "$0")/.."

# Get all card IDs from new sets
IDS=$(python3 -c "
from db.connection import get_db
with get_db() as db:
    rows = db.execute('''
        SELECT id FROM cards
        WHERE set_code IN ('EVO','GEN','BUS','HIF','SHL','COE','DRM','VIV','SHF','CRE','EVS','CEL','FST','BRS','ASR','LOR','SIT')
          AND sealed_product = 'N'
        ORDER BY id
    ''').fetchall()
    print(' '.join(r['id'] for r in rows))
")

TOTAL=$(echo "$IDS" | wc -w)
echo "Bootstrapping $TOTAL cards..."

COUNTER=0
for ID in ${=IDS}; do
    COUNTER=$((COUNTER + 1))
    python3 -m scripts.bootstrap_pc_history_and_images --card-id "$ID" 2>&1 | grep -E "history_rows|no_match" | head -1 | sed "s/^/[$COUNTER\/$TOTAL] $ID: /"
done

echo "Done. Final stats:"
python3 -c "
from db.connection import get_db
with get_db() as db:
    r = db.execute('''
        SELECT COUNT(DISTINCT card_id) as c FROM price_history
        WHERE card_id IN (SELECT id FROM cards WHERE set_code IN ('EVO','GEN','BUS','HIF','SHL','COE','DRM','VIV','SHF','CRE','EVS','CEL','FST','BRS','ASR','LOR','SIT'))
    ''').fetchone()
    print(f'  Cards with price history: {r[\"c\"]}')
    r2 = db.execute('''
        SELECT COUNT(*) FROM cards
        WHERE set_code IN ('EVO','GEN','BUS','HIF','SHL','COE','DRM','VIV','SHF','CRE','EVS','CEL','FST','BRS','ASR','LOR','SIT')
          AND image_url IS NOT NULL AND image_url != ''
    ''').fetchone()
    print(f'  Cards with images: {r2[0]}')
"
