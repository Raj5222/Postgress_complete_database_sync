#!/bin/bash
# ============================================
# Complete PostgreSQL Database Sync Script - PRODUCTION PERFECTION
# Features: 
#   - HIGH VISIBILITY Phase Headers
#   - Auto-Clear Terminal & Config Summary
#   - Graceful Cancellation & Robust Cleanup
# ============================================

# Exit on undefined variables and pipe failures.
set -uo pipefail

# ---------------------------
# 1. Configuration
# ---------------------------

# Source Database (MAIN)
MAIN_DB_HOST="${MAIN_DB_HOST:-localhost}"
MAIN_DB_PORT="${MAIN_DB_PORT:-5432}"
MAIN_DB_USER="${MAIN_DB_USER:-postgres}"
MAIN_DB_PASS="${MAIN_DB_PASSWORD:-0206}" 
MAIN_DB_NAME="${MAIN_DB_NAME:-terotam_local}"

# Target Database (ARCHIVE)
ARCHIVE_DB_HOST="${ARCHIVE_DB_HOST:-localhost}"
ARCHIVE_DB_PORT="${ARCHIVE_DB_PORT:-5432}"
ARCHIVE_DB_USER="${ARCHIVE_DB_USER:-postgres}"
ARCHIVE_DB_PASS="${ARCHIVE_DB_PASSWORD:-0206}"
ARCHIVE_DB_NAME="${ARCHIVE_DB_NAME:-0830tero_archive}"

# Performance & Batching
RETRY_COUNT=3
PG_JOBS=4
BATCH_SIZE=1

# Paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
DUMP_DIR="./migration_dumps_${TIMESTAMP}"
SCHEMA_DUMP_FILE="${DUMP_DIR}/schema_dump.sql"

# Export Passwords Globally
export PGPASSWORD="$MAIN_DB_PASS"
export ARCHIVE_PGPASSWORD="$ARCHIVE_DB_PASS" 

# Global flag to track if we are shutting down
SHUTDOWN_TRIGGERED=0

# ---------------------------
# 2. Core Tables List
# ---------------------------
CORE_TABLES=(
    "become_partner" "customer_bound_vendors" "customer_contract" "customer_outlets"
    "customer_outlet_equipment" "default_access_control" "default_countries" "departments"
    "email_preference" "guest_locations" "jwt_token_block" "master_contract"
    "master_users" "login_session" "migrations" "modules"
    "outlet_areas" "outlet_group_binding" "outlet_additional_info" "outlet_group"
    "zone_city" "roles" "custom_module_equipment_map" "staff_tracking_map"
    "sms_preference" "static_default_cities" "smtp_config" "support"
    "support_issues_history" "support_issues" "support_setting" "support_issues_titles"
    "token_approval" "typeorm_metadata" "vendor_jwt_token_block" "vendor_contract"
    "vendor_employe_preferences" "whatsapp_config" "vendors" "vendor_outlets"
    "web_request" "vendor_zone_city" "vendor_staff_tracking" "zones"
    "vendor_zones" "customer_employee" "notification_prefrence" "access_control"
    "vendor_employee" "support_feedback" "default_cities" "default_zones"
    "guest_employee" "service_types" "geo_location_preferences" "department_subscription"
    "staff_additional_info" "vendor_employee_binding" "vendor_customer_requests" "support_issues_attachments"
    "asset_multi_flow" "dashboard_charts_subs" "customer_employee_binding" "customer"
    "customer_preference" "asset_attachment" "asset_history" "asset_depreciation"
    "asset_outlet_history" "asset_remarks" "assets_service_preferences" "assets_tat"
    "asset_status" "asset_type_depreciation" "config_asset_alert" "custom_modules"
    "customer_asset_preference" "equipments_brands" "equipments_sr_no" "nature_of_titles"
    "sub_modules" "sub_module_data" "asset_reminder" "assets_manual"
    "custom_module_data" "default_gm_custom_labels" "default_gm_assets" "gm_assets"
    "gm_config" "gm_default_checklist" "gm_preferences" "gm_frequency"
    "gm_schedule_assets" "gm_scheduler" "wp_preference" "workflow_report_forms"
    "complaint_relation_forms" "complaint_relation_forms_data" "complaints_invoice"
    "complaints_invoice_order_item" "expense_location_asset" "invoice_comments"
    "ratecard_comments" "rating_feedback" "rating_feedback_history" "vendor_complain_ratecard"
    "customer_ratecard" "customer_ratecard_category" "customer_ratecard_item" "complain_forms"
    "complain_logs" "complain_preferences" "complain_priorities" "complain_service_types"
    "complain_status_messages" "complain_workflow_preferences" "complain_customer_preference" "complain_dynamic_forms"
    "complaint_relations" "default_invoice_fields" "default_pm_assets" "pm_assets"
    "pm_config" "pm_default_checklist" "pm_preferences" "pm_frequncy"
    "pm_schedule_assets" "sca_audit_parent_category" "sca_audit_category" "sca_audit_item"
    "sca_audit_item_attachment" "sca_audit_frequency" "sca_audit_scheduler" "sca_finance_forms"
    "sca_stock_forms" "sca_preference" "sca_audit_condition" "sca_audit_priority"
    "performance_rating"
)

# ---------------------------
# 3. Logging & Cleanup (BRIGHT COLORS)
# ---------------------------
L_RED='\033[1;31m' 
L_GREEN='\033[1;32m' 
L_YELLOW='\033[1;33m' 
L_BLUE='\033[1;34m' 
L_MAGENTA='\033[1;35m'
L_CYAN='\033[1;36m' 
L_WHITE='\033[1;37m'
NC='\033[0m'

# --- NEW HIGHLIGHTED HEADER ---
log_header() { 
    echo -e "\n${L_MAGENTA}========================================================================================${NC}"
    echo -e "${L_MAGENTA}#  $1  ${NC}"
    echo -e "${L_MAGENTA}========================================================================================${NC}\n"
}

get_ts() { date '+%H:%M:%S'; }

# $1=State, $2=Table, $3=Process, $4=Details
log_activity() {
    if [ "$SHUTDOWN_TRIGGERED" -eq 1 ]; then return; fi
    local color=$NC
    if [ "$1" == "STARTED" ]; then color=$L_YELLOW; fi
    if [ "$1" == "DONE" ]; then color=$L_GREEN; fi
    if [ "$1" == "FAILED" ]; then color=$L_RED; fi
    
    printf "${L_WHITE}[%s]${NC} ${L_CYAN}[%-6s]${NC} ${color}[%-9s]${NC} %-35s ${L_WHITE}|${NC} %s\n" \
        "$(get_ts)" "$3" "$1" "$2" "$4"
}

log_info()    { echo -e "${L_CYAN}[INFO]    $*${NC}"; }
log_error()   { echo -e "${L_RED}[ERROR]   $*${NC}"; }
log_warn()    { echo -e "${L_YELLOW}[WARN]    $*${NC}"; }

print_config() {
    clear
    log_header "SYNC CONFIGURATION"
    echo -e "${L_WHITE}SOURCE DB (FROM):${NC}   ${L_GREEN}${MAIN_DB_USER}@${MAIN_DB_HOST}:${MAIN_DB_PORT} / ${MAIN_DB_NAME}${NC}"
    echo -e "${L_WHITE}TARGET DB (TO):${NC}     ${L_RED}${ARCHIVE_DB_USER}@${ARCHIVE_DB_HOST}:${ARCHIVE_DB_PORT} / ${ARCHIVE_DB_NAME}${NC}"
    echo -e "${L_WHITE}START TIME:${NC}         $(date)"
    echo -e "${L_WHITE}TEMP DIR:${NC}           ${DUMP_DIR}"
    echo -e "${L_WHITE}MAX JOBS:${NC}           ${L_RED}${PG_JOBS}"
    echo -e "${L_WHITE}MAX TABLES:${NC}         ${BATCH_SIZE}"
    echo "========================================================================================"
}

pre_cleanup() {
    find "$SCRIPT_DIR" -maxdepth 1 -type d -name "migration_dumps_*" -exec rm -rf {} +
    mkdir -p "$DUMP_DIR"
}

cleanup() {
    local exit_code=$?
    SHUTDOWN_TRIGGERED=1
    
    local active_jobs=$(jobs -p)
    if [ -n "$active_jobs" ]; then
        kill $active_jobs 2>/dev/null
        wait $active_jobs 2>/dev/null
    fi

    echo ""
    log_header "FINAL CLEANUP"
    if [ -d "$DUMP_DIR" ]; then
        log_info "Removing temporary directory: $DUMP_DIR"
        rm -rf "$DUMP_DIR"
    fi
    unset PGPASSWORD ARCHIVE_PGPASSWORD
    
    if [ $exit_code -eq 0 ]; then
        echo -e "${L_GREEN}✅ Script finished successfully.${NC}"
    elif [ $exit_code -eq 130 ]; then
        echo -e "${L_YELLOW}⚠️  Script Cancelled (Ctrl+C).${NC}"
    else
        echo -e "${L_RED}❌ Script Failed (Exit Code: $exit_code).${NC}"
    fi
    exit $exit_code
}

trap cleanup EXIT INT TERM

# ---------------------------
# 4. Helper Functions
# ---------------------------

run_with_retry() {
    local cmd="$1"
    local description="$2"
    local attempt=1
    local tmp_out="${DUMP_DIR}/cmd_output_${RANDOM}.tmp"
    
    if [ "$SHUTDOWN_TRIGGERED" -eq 1 ] || [ ! -d "$DUMP_DIR" ]; then return 1; fi
    
    while [ $attempt -le $RETRY_COUNT ]; do
        if eval "$cmd" > "$tmp_out" 2>&1; then
            rm -f "$tmp_out"
            return 0
        fi
        
        if [ "$SHUTDOWN_TRIGGERED" -eq 1 ] || [ ! -f "$tmp_out" ]; then return 1; fi

        if grep -q "connection" "$tmp_out"; then
             log_warn "Connection issue detected on attempt $attempt"
        fi

        ((attempt++))
        sleep 2
    done
    
    if [ "$SHUTDOWN_TRIGGERED" -eq 0 ]; then
        log_error "Failed: $description"
        if [ -f "$tmp_out" ]; then
            echo -e "${L_RED}=========== ERROR DETAILS ============${NC}"
            cat "$tmp_out"
            echo -e "${L_RED}=========== ERROR DETAILS ============${NC}"
            rm -f "$tmp_out"
        fi
    fi
    return 1
}

is_partitioned_table() {
    local table=$1
    PGPASSWORD="$MAIN_DB_PASS" psql -h "$MAIN_DB_HOST" -p "$MAIN_DB_PORT" -U "$MAIN_DB_USER" -d "$MAIN_DB_NAME" -t -A -c "
        SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = '$table' AND relkind = 'p');" 2>/dev/null
}

get_partition_tables() {
    local parent_table=$1
    PGPASSWORD="$MAIN_DB_PASS" psql -h "$MAIN_DB_HOST" -p "$MAIN_DB_PORT" -U "$MAIN_DB_USER" -d "$MAIN_DB_NAME" -t -A -c "
        WITH RECURSIVE partition_tree AS (
            SELECT c.oid, c.relname, c.relkind FROM pg_class c
            JOIN pg_inherits i ON i.inhrelid = c.oid JOIN pg_class p ON p.oid = i.inhparent
            WHERE p.relname = '$parent_table'
            UNION ALL
            SELECT c.oid, c.relname, c.relkind FROM partition_tree pt
            JOIN pg_inherits i ON i.inhparent = pt.oid JOIN pg_class c ON c.oid = i.inhrelid
            WHERE pt.relkind = 'p'
        )
        SELECT DISTINCT relname FROM partition_tree ORDER BY relname;" 2>/dev/null
}

export_table() {
    local table=$1
    local start_time=$(date +%s)
    local cmd=""
    local info=""
    
    if [ "$SHUTDOWN_TRIGGERED" -eq 1 ]; then return 1; fi

    local is_part=$(is_partitioned_table "$table")
    
    if [ "$is_part" = "t" ]; then
        local partitions=$(get_partition_tables "$table")
        local part_count=$(echo "$partitions" | grep -c '^' || echo 0)
        
        if [ "$part_count" -eq 0 ]; then
            info="Partitioned (Empty)"
            cmd="pg_dump --data-only --no-owner --quote-all-identifiers -Fd -j $PG_JOBS -h $MAIN_DB_HOST -p $MAIN_DB_PORT -U $MAIN_DB_USER -d $MAIN_DB_NAME --table=$table -f $DUMP_DIR/$table"
        else
            info="Partitioned ($part_count parts)"
            local table_list="--table=$table"
            while IFS= read -r partition; do
                if [ -n "$partition" ]; then table_list="$table_list --table=$partition"; fi
            done <<< "$partitions"
            cmd="pg_dump --data-only --no-owner --quote-all-identifiers -Fd -j $PG_JOBS -h $MAIN_DB_HOST -p $MAIN_DB_PORT -U $MAIN_DB_USER -d $MAIN_DB_NAME $table_list -f $DUMP_DIR/$table"
        fi
    else
        info="Standard Table"
        cmd="pg_dump --data-only --no-owner --quote-all-identifiers -Fd -j $PG_JOBS -h $MAIN_DB_HOST -p $MAIN_DB_PORT -U $MAIN_DB_USER -d $MAIN_DB_NAME --table=$table -f $DUMP_DIR/$table"
    fi
    
    log_activity "STARTED" "$table" "EXPORT" "$info"
    
    run_with_retry "$cmd" "Export $table"
    local status=$?
    
    if [ $status -eq 0 ]; then
        local duration=$(( $(date +%s) - start_time ))
        local size=$(du -sh "$DUMP_DIR/$table" 2>/dev/null | cut -f1 || echo "0B")
        log_activity "DONE" "$table" "EXPORT" "Size: $size, Time: ${duration}s"
        return 0
    else
        log_activity "FAILED" "$table" "EXPORT" "Check logs"
        return 1
    fi
}

import_table() {
    local table=$1
    local start_time=$(date +%s)
    
    if [ "$SHUTDOWN_TRIGGERED" -eq 1 ] || [ ! -d "$DUMP_DIR/$table" ]; then return 1; fi
    
    log_activity "STARTED" "$table" "IMPORT" "Restoring data..."

    PGPASSWORD="$ARCHIVE_DB_PASS" run_with_retry \
        "pg_restore --data-only --disable-triggers -j $PG_JOBS -h $ARCHIVE_DB_HOST -p $ARCHIVE_DB_PORT -U $ARCHIVE_DB_USER -d $ARCHIVE_DB_NAME $DUMP_DIR/$table" \
        "Import $table"
        
    local status=$?
    local duration=$(( $(date +%s) - start_time ))

    if [ $status -eq 0 ]; then
        log_activity "DONE" "$table" "IMPORT" "Time: ${duration}s"
        return 0
    else
        log_activity "FAILED" "$table" "IMPORT" "Check logs"
        return 1
    fi
}

run_parallel_jobs() {
    local job_function=$1
    local total_tables=${#CORE_TABLES[@]}
    local failed_jobs=()
    
    for ((i=0; i<$total_tables; i+=$BATCH_SIZE)); do
        if [ "$SHUTDOWN_TRIGGERED" -eq 1 ]; then return 1; fi

        batch_end=$((i + BATCH_SIZE))
        if [ $batch_end -gt $total_tables ]; then batch_end=$total_tables; fi
        
        for ((j=i; j<$batch_end; j++)); do
            table="${CORE_TABLES[$j]}"
            "$job_function" "$table" &
            pids[$j]=$!
        done
        
        for ((j=i; j<$batch_end; j++)); do
            wait ${pids[$j]}
            if [ $? -ne 0 ]; then failed_jobs+=("${CORE_TABLES[$j]}"); fi
        done
    done
    
    if [ ${#failed_jobs[@]} -gt 0 ]; then
        if [ "$SHUTDOWN_TRIGGERED" -eq 0 ]; then
            log_error "Failed tables in $job_function: ${failed_jobs[*]}"
        fi
        return 1
    fi
    return 0
}

# ---------------------------
# 5. Main Execution Flow
# ---------------------------

pre_cleanup
print_config

# PHASE 1: SCHEMA
log_header "PHASE 1/4: Schema Synchronization"

if [ "$SHUTDOWN_TRIGGERED" -eq 0 ]; then
    log_info "Terminating active connections..."
    PGPASSWORD="$ARCHIVE_DB_PASS" psql -h "$ARCHIVE_DB_HOST" -p "$ARCHIVE_DB_PORT" -U "$ARCHIVE_DB_USER" -d postgres -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '$ARCHIVE_DB_NAME' AND pid <> pg_backend_pid();" > /dev/null 2>&1
fi

if [ "$SHUTDOWN_TRIGGERED" -eq 0 ]; then
    log_info "Recreating database: $ARCHIVE_DB_NAME"
    PGPASSWORD="$ARCHIVE_DB_PASS" psql -h "$ARCHIVE_DB_HOST" -p "$ARCHIVE_DB_PORT" -U "$ARCHIVE_DB_USER" -d postgres -c "DROP DATABASE IF EXISTS \"$ARCHIVE_DB_NAME\";" > "${DUMP_DIR}/db_drop.log" 2>&1
    PGPASSWORD="$ARCHIVE_DB_PASS" psql -h "$ARCHIVE_DB_HOST" -p "$ARCHIVE_DB_PORT" -U "$ARCHIVE_DB_USER" -d postgres -c "CREATE DATABASE \"$ARCHIVE_DB_NAME\";" > "${DUMP_DIR}/db_create.log" 2>&1

    if [ $? -ne 0 ]; then
        log_error "Database Reset Failed. Check ${DUMP_DIR}/db_create.log"
        echo -e "${L_RED}=========== ERROR DETAILS ============${NC}"
        cat "${DUMP_DIR}/db_create.log"
        echo -e "${L_RED}=========== ERROR DETAILS ============${NC}"
        exit 1
    fi
fi

if [ "$SHUTDOWN_TRIGGERED" -eq 0 ]; then
    log_info "Syncing Schema..."
    PGPASSWORD="$MAIN_DB_PASS" run_with_retry "pg_dump -h $MAIN_DB_HOST -p $MAIN_DB_PORT -U $MAIN_DB_USER -d $MAIN_DB_NAME --schema-only -Fp > $SCHEMA_DUMP_FILE" "Schema Dump" || exit 1
    PGPASSWORD="$ARCHIVE_DB_PASS" run_with_retry "psql -h $ARCHIVE_DB_HOST -p $ARCHIVE_DB_PORT -U $ARCHIVE_DB_USER -d $ARCHIVE_DB_NAME -f $SCHEMA_DUMP_FILE" "Schema Import" || exit 1
fi

# PHASE 2: EXPORT
if [ "$SHUTDOWN_TRIGGERED" -eq 0 ]; then
    log_header "PHASE 2/4: Data Export"
    run_parallel_jobs export_table || exit 1
fi

# PHASE 3: IMPORT
if [ "$SHUTDOWN_TRIGGERED" -eq 0 ]; then
    log_header "PHASE 3/4: Data Import"
    run_parallel_jobs import_table || exit 1
fi

# PHASE 4: VALIDATION
if [ "$SHUTDOWN_TRIGGERED" -eq 0 ]; then
    log_header "PHASE 4/4: Validation"
    FAILED=0
    for table in "${CORE_TABLES[@]}"; do
        c1=$(PGPASSWORD="$MAIN_DB_PASS" psql -h "$MAIN_DB_HOST" -p "$MAIN_DB_PORT" -U "$MAIN_DB_USER" -d "$MAIN_DB_NAME" -t -A -c "SELECT COUNT(*) FROM \"$table\";" || echo "-1")
        c2=$(PGPASSWORD="$ARCHIVE_DB_PASS" psql -h "$ARCHIVE_DB_HOST" -p "$ARCHIVE_DB_PORT" -U "$ARCHIVE_DB_USER" -d "$ARCHIVE_DB_NAME" -t -A -c "SELECT COUNT(*) FROM \"$table\";" || echo "-2")
        
        if [ "$c1" != "$c2" ]; then
            log_activity "FAILED" "$table" "VALIDATE" "Mismatch: $c1 vs $c2"
            FAILED=1
        fi
    done

    if [ $FAILED -eq 0 ]; then
        log_info "✅ Validation Successful. All tables match."
    else
        log_error "❌ Validation Discrepancies Found."
    fi
fi

exit 0