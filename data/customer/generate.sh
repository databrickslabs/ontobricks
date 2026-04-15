#!/bin/bash
#
# generate.sh - Generate and load Energy Provider Customer Journey data
#
# Wrapper around generate_data.py with preset profiles for common scenarios.
# Credentials are read from environment variables or .env file.
#
# Usage:
#   ./generate.sh                        # default ~4,700 rows
#   ./generate.sh medium                 # ~23,500 rows
#   ./generate.sh large                  # ~235,000 rows (requires --volume)
#   ./generate.sh xlarge                 # ~2,350,000 rows (requires --volume)
#   ./generate.sh custom 5000 7500       # custom: 5000 customers, 7500 contracts, rest scaled

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# ── Configuration ─────────────────────────────────────────────────
# Override these or set them as environment variables / in .env

CATALOG="${CATALOG:-benoit_cayla}"
SCHEMA="${SCHEMA:-ontobricks_cust}"
VOLUME="${VOLUME:-/Volumes/${CATALOG}/${SCHEMA}/staging}"
SEED="${SEED:-42}"

# Load .env from project root if it exists
ENV_FILE="$SCRIPT_DIR/../../.env"
if [ -f "$ENV_FILE" ]; then
    set -a
    source "$ENV_FILE"
    set +a
fi

# ── Size profiles ─────────────────────────────────────────────────

PROFILE="${1:-default}"

case "$PROFILE" in
    default)
        CUSTOMERS=200
        CONTRACTS=300
        SUBSCRIPTIONS=350
        METERS=400
        METER_READINGS=1000
        INVOICES=800
        PAYMENTS=700
        CALLS=300
        CLAIMS=150
        INTERACTIONS=500
        ;;
    medium)
        CUSTOMERS=1000
        CONTRACTS=1500
        SUBSCRIPTIONS=1750
        METERS=2000
        METER_READINGS=5000
        INVOICES=4000
        PAYMENTS=3500
        CALLS=1500
        CLAIMS=750
        INTERACTIONS=2500
        ;;
    large)
        CUSTOMERS=10000
        CONTRACTS=15000
        SUBSCRIPTIONS=17500
        METERS=20000
        METER_READINGS=50000
        INVOICES=40000
        PAYMENTS=35000
        CALLS=15000
        CLAIMS=7500
        INTERACTIONS=25000
        ;;
    xlarge)
        CUSTOMERS=100000
        CONTRACTS=150000
        SUBSCRIPTIONS=175000
        METERS=200000
        METER_READINGS=500000
        INVOICES=400000
        PAYMENTS=350000
        CALLS=150000
        CLAIMS=75000
        INTERACTIONS=250000
        ;;
    custom)
        CUSTOMERS="${2:-200}"
        CONTRACTS="${3:-$(( CUSTOMERS * 3 / 2 ))}"
        SUBSCRIPTIONS="$(( CONTRACTS * 7 / 6 ))"
        METERS="$(( CONTRACTS * 4 / 3 ))"
        METER_READINGS="$(( METERS * 5 / 2 ))"
        INVOICES="$(( CONTRACTS * 8 / 3 ))"
        PAYMENTS="$(( INVOICES * 7 / 8 ))"
        CALLS="$(( CUSTOMERS * 3 / 2 ))"
        CLAIMS="$(( CUSTOMERS * 3 / 4 ))"
        INTERACTIONS="$(( CUSTOMERS * 5 / 2 ))"
        ;;
    *)
        echo "Unknown profile: $PROFILE"
        echo ""
        echo "Usage: $0 [default|medium|large|xlarge|custom <customers> [contracts]]"
        echo ""
        echo "Profiles:"
        echo "  default   ~4,700 rows     (200 customers)"
        echo "  medium    ~23,500 rows    (1,000 customers)"
        echo "  large     ~235,000 rows   (10,000 customers)"
        echo "  xlarge    ~2,350,000 rows (100,000 customers)"
        echo "  custom    scaled from customer/contract counts"
        exit 1
        ;;
esac

# ── Compute total for display ─────────────────────────────────────

TOTAL=$(( CUSTOMERS + CONTRACTS + SUBSCRIPTIONS + METERS + METER_READINGS \
        + INVOICES + PAYMENTS + CALLS + CLAIMS + INTERACTIONS ))

echo "Profile:    $PROFILE"
echo "Target:     $CATALOG.$SCHEMA"
echo "Total rows: ~$(printf "%'d" $TOTAL)"
echo ""

# ── Build command ─────────────────────────────────────────────────

CMD=(
    python3 "$SCRIPT_DIR/generate_data.py"
    --catalog "$CATALOG"
    --schema "$SCHEMA"
    --customers "$CUSTOMERS"
    --contracts "$CONTRACTS"
    --subscriptions "$SUBSCRIPTIONS"
    --meters "$METERS"
    --meter-readings "$METER_READINGS"
    --invoices "$INVOICES"
    --payments "$PAYMENTS"
    --calls "$CALLS"
    --claims "$CLAIMS"
    --interactions "$INTERACTIONS"
    --seed "$SEED"
    --drop-existing
)

if [ "$TOTAL" -ge 50000 ]; then
    CMD+=(--volume "$VOLUME")
fi

# ── Run ───────────────────────────────────────────────────────────

exec "${CMD[@]}"
