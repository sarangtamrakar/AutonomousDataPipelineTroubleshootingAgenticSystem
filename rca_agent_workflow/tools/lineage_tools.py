"""Tools for checking table freshness and data dependencies.

Connects to the PostgreSQL Lineage database (data_assets and data_lineage).
Falls back to a mock implementation if DB credentials are not provided.
"""

import os
import json
import logging
from typing import Dict, Any, List

from langchain_core.tools import tool

logger = logging.getLogger(__name__)

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    HAS_POSTGRES = True
except ImportError:
    HAS_POSTGRES = False
    logger.warning("psycopg2 not installed. Lineage tools will run in mock mode.")

def _get_db_connection():
    """Create a Postgres connection using env vars."""
    return psycopg2.connect(
        host=os.environ.get("DB_HOST", "localhost"),
        database=os.environ.get("DB_NAME", "postgres"),
        user=os.environ.get("DB_USER", "postgres"),
        password=os.environ.get("DB_PASSWORD", "postgres"),
        port=os.environ.get("DB_PORT", "5432")
    )

def _fuzzy_match_urn(task_or_urn: str) -> str:
    """Helper to map Airflow task_ids (build_fact_sales) to URNs (urn:gold:fact_sales)"""
    if task_or_urn.startswith("urn:"):
        return task_or_urn
    
    # Simple heuristic to map common task names to your exact mock data URNs
    if "fact_sales" in task_or_urn: return "urn:gold:fact_sales"
    if "dim_customer" in task_or_urn: return "urn:gold:dim_customers"
    if "dim_product" in task_or_urn: return "urn:gold:dim_products"
    if "silver_sales" in task_or_urn: return "urn:silver:sales"
    
    return task_or_urn

@tool
def get_upstream_lineage_status(target_asset: str) -> str:
    """Find upstream dependencies for an asset (e.g., 'urn:gold:fact_sales').
    Returns the parent tables, their statuses ('SUCCESS', 'STALE'), and last update times.
    """
    urn = _fuzzy_match_urn(target_asset)
    
    if HAS_POSTGRES and os.environ.get("DB_USER"):
        try:
            conn = _get_db_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Traverse UPSTREAM (where target is the downstream_asset)
                query = """
                    WITH RECURSIVE upstream_lineage AS (
                        SELECT upstream_asset_id, downstream_asset_id, 1 as depth
                        FROM data_lineage
                        WHERE downstream_asset_id = %s
                        UNION ALL
                        SELECT dl.upstream_asset_id, dl.downstream_asset_id, ul.depth + 1
                        FROM data_lineage dl
                        INNER JOIN upstream_lineage ul ON dl.downstream_asset_id = ul.upstream_asset_id
                    )
                    SELECT ul.upstream_asset_id as asset_id, da.asset_name, da.status, da.last_updated_at
                    FROM upstream_lineage ul
                    LEFT JOIN data_assets da ON ul.upstream_asset_id = da.asset_id
                    ORDER BY ul.depth;
                """
                cur.execute(query, (urn,))
                results = cur.fetchall()
            conn.close()
            
            if not results:
                return json.dumps({"target": urn, "upstream_dependencies": "None found."})
            
            for row in results:
                if row.get('last_updated_at'): row['last_updated_at'] = str(row['last_updated_at'])
            return json.dumps(results, indent=2)
            
        except Exception as e:
            return f"Error querying Postgres lineage database: {e}"
    
    # MOCK FALLBACK (Based exactly on your SQL inserts)
    if "fact_sales" in urn:
        return json.dumps([
            {"asset_id": "urn:silver:sales", "asset_name": "silver_sales", "status": "SUCCESS", "last_updated_at": "2026-06-17 19:55:00"},
            {"asset_id": "urn:gold:dim_customers", "asset_name": "gold_dim_customers", "status": "SUCCESS", "last_updated_at": "2026-06-17 20:05:00"},
            {"asset_id": "urn:gold:dim_products", "asset_name": "gold_dim_products", "status": "STALE", "last_updated_at": "2026-06-16 12:00:00"}
        ], indent=2)
    
    return json.dumps([{"asset_id": "unknown", "status": "UNKNOWN"}])


@tool
def get_downstream_impact(source_asset: str) -> str:
    """Find downstream assets impacted by a failure in the source_asset.
    Useful for determining the Blast Radius (e.g. which dashboards or fact tables are stale).
    """
    urn = _fuzzy_match_urn(source_asset)
    
    if HAS_POSTGRES and os.environ.get("DB_USER"):
        try:
            conn = _get_db_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Traverse DOWNSTREAM (where target is the upstream_asset)
                query = """
                    WITH RECURSIVE downstream_lineage AS (
                        SELECT upstream_asset_id, downstream_asset_id, 1 as depth
                        FROM data_lineage
                        WHERE upstream_asset_id = %s
                        UNION ALL
                        SELECT dl.upstream_asset_id, dl.downstream_asset_id, dl_rec.depth + 1
                        FROM data_lineage dl
                        INNER JOIN downstream_lineage dl_rec ON dl.upstream_asset_id = dl_rec.downstream_asset_id
                    )
                    SELECT dl_rec.downstream_asset_id as asset_id, da.asset_name, da.asset_type, da.owner_email
                    FROM downstream_lineage dl_rec
                    LEFT JOIN data_assets da ON dl_rec.downstream_asset_id = da.asset_id
                    ORDER BY dl_rec.depth;
                """
                cur.execute(query, (urn,))
                results = cur.fetchall()
            conn.close()
            
            if not results:
                return json.dumps({"source": urn, "impacted_assets": "None found. This is a terminal node."})
            return json.dumps(results, indent=2)
            
        except Exception as e:
            return f"Error querying Postgres lineage database: {e}"
            
    # MOCK FALLBACK (Based exactly on your SQL inserts)
    if "dim_customers" in urn:
        return json.dumps([
            {"asset_id": "urn:gold:fact_sales", "asset_name": "gold_fact_sales", "asset_type": "table"}
        ], indent=2)
    elif "silver:sales" in urn:
        return json.dumps([
            {"asset_id": "urn:gold:fact_sales", "asset_name": "gold_fact_sales", "asset_type": "table"}
        ], indent=2)
        
    return json.dumps([{"asset_id": "urn:gold:fact_sales", "asset_name": "gold_fact_sales", "asset_type": "table"}])


@tool
def check_asset_status(asset_name: str) -> str:
    """Check the current status ('SUCCESS', 'FAILED', 'STALE') of a specific data asset."""
    urn = _fuzzy_match_urn(asset_name)
    
    if HAS_POSTGRES and os.environ.get("DB_USER"):
        try:
            conn = _get_db_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT asset_id, asset_name, status, last_updated_at FROM data_assets WHERE asset_id = %s OR asset_name = %s", (urn, asset_name))
                result = cur.fetchone()
            conn.close()
            if result:
                if result.get('last_updated_at'): result['last_updated_at'] = str(result['last_updated_at'])
                return json.dumps(result)
            return json.dumps({"error": f"Asset {asset_name} not found."})
        except Exception as e:
            return f"Error querying asset status: {e}"
            
    # Mock fallback
    return json.dumps({"asset_id": urn, "status": "SUCCESS", "last_updated_at": "2026-06-17 20:15:00"})