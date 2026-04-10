#!/bin/bash
# Create tables in Databricks from CSV files

WAREHOUSE_ID="66e8366e84d57752"
CATALOG="benoit_cayla"
SCHEMA="ontobricks_cust"
VOLUME_PATH="/Volumes/$CATALOG/$SCHEMA/customer_data"

execute_sql() {
    local sql="$1"
    local name="$2"
    echo "Creating $name..."
    result=$(databricks api post /api/2.0/sql/statements --json "{
        \"warehouse_id\": \"$WAREHOUSE_ID\",
        \"statement\": \"$sql\",
        \"wait_timeout\": \"60s\"
    }" 2>&1)
    
    state=$(echo "$result" | grep -o '"state":"[^"]*"' | head -1 | cut -d'"' -f4)
    if [ "$state" = "SUCCEEDED" ]; then
        echo "  ✓ $name created successfully"
    else
        echo "  ✗ $name failed: $result"
    fi
}

# Customer table
execute_sql "CREATE OR REPLACE TABLE $CATALOG.$SCHEMA.customer USING CSV OPTIONS (path '$VOLUME_PATH/customer.csv', header 'true', inferSchema 'true')" "customer"

# Contract table
execute_sql "CREATE OR REPLACE TABLE $CATALOG.$SCHEMA.contract USING CSV OPTIONS (path '$VOLUME_PATH/contract.csv', header 'true', inferSchema 'true')" "contract"

# Subscription table
execute_sql "CREATE OR REPLACE TABLE $CATALOG.$SCHEMA.subscription USING CSV OPTIONS (path '$VOLUME_PATH/subscription.csv', header 'true', inferSchema 'true')" "subscription"

# Meter table
execute_sql "CREATE OR REPLACE TABLE $CATALOG.$SCHEMA.meter USING CSV OPTIONS (path '$VOLUME_PATH/meter.csv', header 'true', inferSchema 'true')" "meter"

# Meter reading table
execute_sql "CREATE OR REPLACE TABLE $CATALOG.$SCHEMA.meter_reading USING CSV OPTIONS (path '$VOLUME_PATH/meter_reading.csv', header 'true', inferSchema 'true')" "meter_reading"

# Invoice table
execute_sql "CREATE OR REPLACE TABLE $CATALOG.$SCHEMA.invoice USING CSV OPTIONS (path '$VOLUME_PATH/invoice.csv', header 'true', inferSchema 'true')" "invoice"

# Payment table
execute_sql "CREATE OR REPLACE TABLE $CATALOG.$SCHEMA.payment USING CSV OPTIONS (path '$VOLUME_PATH/payment.csv', header 'true', inferSchema 'true')" "payment"

# Call table
execute_sql "CREATE OR REPLACE TABLE $CATALOG.$SCHEMA.call USING CSV OPTIONS (path '$VOLUME_PATH/call.csv', header 'true', inferSchema 'true')" "call"

# Claim table
execute_sql "CREATE OR REPLACE TABLE $CATALOG.$SCHEMA.claim USING CSV OPTIONS (path '$VOLUME_PATH/claim.csv', header 'true', inferSchema 'true')" "claim"

# Interaction table
execute_sql "CREATE OR REPLACE TABLE $CATALOG.$SCHEMA.interaction USING CSV OPTIONS (path '$VOLUME_PATH/interaction.csv', header 'true', inferSchema 'true')" "interaction"

echo ""
echo "Done! All tables created in $CATALOG.$SCHEMA"
