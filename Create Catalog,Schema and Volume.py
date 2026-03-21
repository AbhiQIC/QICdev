# Databricks notebook source
# MAGIC %sql
# MAGIC drop external location  otif_ctlg_uc;

# COMMAND ----------

# %sql

# CREATE STORAGE CREDENTIAL IF NOT EXISTS cred_o2c_abhi0311sa
#   WITH STORAGE PROVIDER AZURE_MANAGED_IDENTITY
#   COMMENT 'WMI credential for abhi0311sa storage';


# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a location accessed using the abfss_remote_cred credential
# MAGIC CREATE EXTERNAL LOCATION otif_ctlg_uc
# MAGIC URL 'abfss://otif@abhi0311sa.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL storage_credential_external_uc)
# MAGIC COMMENT 'Bronze UC external location';

# COMMAND ----------

# MAGIC %sql
# MAGIC describe external location otif_ctlg_uc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE CATALOG IF NOT EXISTS otif_ctlg
# MAGIC     MANAGED LOCATION 'abfss://otif@abhi0311sa.dfs.core.windows.net/'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE CATALOG otif_ctlg;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS otif_schema
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG otif_ctlg;
# MAGIC USE SCHEMA otif_schema;
# MAGIC CREATE VOLUME IF NOT EXISTS otif_vol;

# COMMAND ----------


from pyspark.sql import functions as F

# ---------------------------
# CONFIGURE THESE TWO LINES
# ---------------------------
volume_root = "/Volumes/otif_ctlg/otif_Schema/otif_vol/raw"   # your volume directory
target_catalog = "otif_ctlg"
target_schema  = "otif_Schema"
# ---------------------------

# Helper to normalize file names to safe table names
def to_table_name(file_path: str) -> str:
    name = file_path.split("/")[-1].rsplit(".", 1)[0]
    return (name.strip()
                 .replace(" ", "_")
                 .replace("-", "_")
                 .replace(".", "_")
                 .lower())

# Collect CSV paths (non-recursive; set recurse=True below if you keep subfolders)
all_entries = dbutils.fs.ls(volume_root)
csv_paths = [e.path for e in all_entries if e.path.lower().endswith(".csv")]

# If your CSVs are in nested subfolders, replace the listing with a small DFS:
def ls_recursive(root: str):
    stack, out = [root], []
    while stack:
        p = stack.pop()
        for e in dbutils.fs.ls(p):
            if e.isDir():
                stack.append(e.path)
            else:
                out.append(e.path)
    return out

# Uncomment this if your files are nested:
# csv_paths = [p for p in ls_recursive(volume_root) if p.lower().endswith(".csv")]

print(f"Found {len(csv_paths)} CSV file(s).")
for p in csv_paths:
    tbl = to_table_name(p)
    full_table = f"{target_catalog}.{target_schema}.{tbl}"
    print(f"→ Loading {p}  ->  {full_table}")

    df = (spark.read
                .format("csv")
                .option("header", True)
                .option("inferSchema", True)   # <- as requested
                .option("multiLine", True)     # safer if any fields contain line breaks
                .option("escape", '"')         # handle quotes/embedded delimiters
                .load(p))

    # Optional: trim column names
    for c in df.columns:
        if c != c.strip():
            df = df.withColumnRenamed(c, c.strip())

    (df.write
       .format("delta")
       .mode("overwrite")                      # idempotent re-runs
       .option("overwriteSchema", "true")      # align schema on re-loads
       .saveAsTable(full_table))

print("✅ All CSVs have been written to Delta tables (Bronze).")


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT table_schema, table_name
# MAGIC FROM system.information_schema.tables
# MAGIC WHERE table_catalog = 'otif_ctlg'
# MAGIC   AND table_schema = 'otif_schema'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from otif_ctlg.otif_Schema.billingheader

# COMMAND ----------


tables = spark.sql("SHOW TABLES IN otif_ctlg.otif_Schema").collect()

row_counts = []
for t in tables:
    tbl = t.tableName
    full_name = f"otif_ctlg.otif_Schema.{tbl}"
    cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {full_name}").collect()[0]['c']
    row_counts.append((tbl, cnt))

row_counts


# COMMAND ----------


# Adjust to your actual Volume path
VOLUME_ROOT = "/Volumes/otif_ctlg/otif_Schema/otif_vol/raw"
META_REL_PATH = f"{VOLUME_ROOT}/relationships.csv"
META_DD_PATH  = f"{VOLUME_ROOT}/OTIF_Data_Dictionary.csv"


# COMMAND ----------


# # relationships.csv -> meta_relationships
# rel_df = (spark.read.option("header", True).option("inferSchema", True)
#           .csv(META_REL_PATH))
# (rel_df.write.format("delta").mode("overwrite")
#  .saveAsTable("otif_ctlg.otif_Schema.meta_relationships"))

# relationships.csv -> meta_relationships
rel_df = (spark.read.option("header", True).option("inferSchema", True)
          .csv(META_REL_PATH))
(rel_df.write.format("delta").mode("overwrite")
 .saveAsTable("otif_ctlg.otif_Schema.ontology_relationships"))

# # relationships.csv -> meta_data_dictionary
# rel_df = (spark.read.option("header", True).option("inferSchema", True)
#           .csv(META_DD_PATH))
# (rel_df.write.format("delta").mode("overwrite")
#  .saveAsTable("otif_ctlg.otif_Schema.meta_data_dictionary"))


# COMMAND ----------

# MAGIC %sql select * from otif_ctlg.otif_Schema.ontology_relationships

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG otif_ctlg;
# MAGIC USE SCHEMA otif_Schema;
# MAGIC
# MAGIC -- 2.1 Vertices (entities) — one row per unique table label
# MAGIC CREATE OR REPLACE TABLE kg_vertices_schema AS
# MAGIC WITH ents AS (
# MAGIC   SELECT FromEntity AS entity FROM meta_relationships
# MAGIC   UNION
# MAGIC   SELECT ToEntity   AS entity FROM meta_relationships
# MAGIC )
# MAGIC SELECT DISTINCT
# MAGIC   entity            AS id,        -- GraphFrames needs 'id'
# MAGIC   entity            AS label      -- human label (table name)
# MAGIC FROM ents;
# MAGIC
# MAGIC -- 2.2 Edges (relationships) — one row per relationship (schema-level)
# MAGIC CREATE OR REPLACE TABLE kg_edges_schema AS
# MAGIC SELECT
# MAGIC   FromEntity        AS src,       -- GraphFrames needs 'src'
# MAGIC   ToEntity          AS dst,       -- GraphFrames needs 'dst'
# MAGIC   Name              AS relationship,
# MAGIC   Cardinality       AS cardinality,
# MAGIC   Join              AS sql_join,
# MAGIC   Description       AS description
# MAGIC FROM meta_relationships;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from  kg_vertices_schema;
# MAGIC select * from  kg_edges_schema

# COMMAND ----------



# === CONFIG ===
CATALOG = "otif_ctlg"
SCHEMA  = "otif_Schema"
REL_TABLE = f"{CATALOG}.{SCHEMA}.meta_relationships"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# Load unique entities & relationships
rels = spark.table(REL_TABLE).select("FromEntity","ToEntity","Name").distinct().collect()

# Collect node set
entities = sorted(set([r["FromEntity"] for r in rels] + [r["ToEntity"] for r in rels]))

def esc(s: str) -> str:
    """Mermaid-safe identifier (no spaces/specials). Only for node IDs in the diagram."""
    return "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in s)

# Build Mermaid flowchart
lines = ["flowchart LR"]
# Nodes with readable labels
for e in entities:
    lines.append(f'  {esc(e)}["{e}"]')
# Edges with relationship labels
for r in rels:
    src, dst, rel = esc(r["FromEntity"]), esc(r["ToEntity"]), r["Name"].replace("_", " ")
    lines.append(f'  {src} -- "{rel}" --> {dst}')

mermaid = "\n".join(lines)

# Render in Databricks (lightweight loader for Mermaid)


rels = spark.table("otif_ctlg.otif_Schema.kg_edges_schema").select("src","dst","relationship").distinct().collect()
nodes = spark.table("otif_ctlg.otif_Schema.kg_vertices_schema").select("id").distinct().collect()

def esc(s: str) -> str:
    return s.replace(" ", "_")

# Build Mermaid flowchart
lines = ["flowchart LR"]
# nodes
for n in nodes:
    nid = esc(n['id'])
    lines.append(f'  {nid}["{n["id"]}"]')
# edges
for r in rels:
    a, b, rel = esc(r['src']), esc(r['dst']), r['relationship']
    lines.append(f'  {a} -- "{rel}" --> {b}')

mermaid = "\n".join(lines)
html = f"""
<div class="mermaid">
{mermaid}
</div>

<!-- Databricks: ensure Mermaid is available or load it -->
<script src="https://cdn.jsdelivr.net/npm/mermaid/dist/mermaid.min.js"></script>
<script>mermaid.initialize({{ startOnLoad: true }})</script>
"""
displayHTML(html)

# COMMAND ----------

# === CONFIG (edit once) ===
CATALOG = "otif_ctlg"
SCHEMA  = "otif_Schema"
REL_TABLE = f"{CATALOG}.{SCHEMA}.meta_relationships"  # Name, FromEntity, ToEntity, Cardinality, Join, Description

# Authoritative table names for each entity label (extend as needed)
CLASS_TABLES = {
    "SalesOrder"      : f"{CATALOG}.{SCHEMA}.salesorderheader",
    "SalesOrderItem"  : f"{CATALOG}.{SCHEMA}.salesorderitem",
    "SalesOrderItemStatus"  : f"{CATALOG}.{SCHEMA}.salesorderitemstatus",
    "SalesOrderStatus"  : f"{CATALOG}.{SCHEMA}.salesorderstatusheader",
    "ScheduleLine"    : f"{CATALOG}.{SCHEMA}.scheduleline",
    "Delivery"        : f"{CATALOG}.{SCHEMA}.deliveryheader",
    "DeliveryItem"    : f"{CATALOG}.{SCHEMA}.deliveryitem",
    "Shipment"        : f"{CATALOG}.{SCHEMA}.shipmentheader",
    "ShipmentItem"    : f"{CATALOG}.{SCHEMA}.shipmentitem",
    "BillingDocument" : f"{CATALOG}.{SCHEMA}.billingheader",
    "BillingItem"     : f"{CATALOG}.{SCHEMA}.billingitem",
    "DocumentFlowLink"     : f"{CATALOG}.{SCHEMA}.documentflow",
    # add more if present
}

VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/otif_vol/ontology"
MANIFEST_PATH = f"{VOLUME_ROOT}/o2c_semantic_manifest.yaml"

# COMMAND ----------


from pyspark.sql import functions as F

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# OPTIONAL: uncomment if you created this from OTIF_Data_Dictionary CSV earlier:
# dict_df = spark.table("meta_data_dictionary")  # expects columns: Table, Column, Description
# Otherwise set dict_df = None
try:
    dict_df = spark.table("meta_data_dictionary").select("Table","Column","Description")
except:
    dict_df = None


# COMMAND ----------


import yaml

# Load relationships
rel_df = (spark.table(REL_TABLE)
          .select("Name","FromEntity","ToEntity","Cardinality","Join","Description")
          .dropna(subset=["FromEntity","ToEntity","Name"])
          .distinct())

# Entity set
entities = sorted(set([r["FromEntity"] for r in rel_df.collect()] + 
                      [r["ToEntity"]   for r in rel_df.collect()]))

manifest = {
    "version": 1,
    "namespace": "http://example.com/o2c#",
    "entities": {},
    "relations": []
}

# --- Build entities ---
for e in entities:
    entry = {
        "label": e.replace("_"," "),
        "table": CLASS_TABLES.get(e, f"{CATALOG}.{SCHEMA}.{e.lower()}")
    }
    # Optionally enrich with keys/columns from dict_df
    if dict_df is not None:
        cols = (dict_df.where(F.col("Table") == e)
                .select("Column","Description")
                .distinct()
                .collect())
        if cols:
            entry["columns"] = {}
            for c in cols:
                entry["columns"][c["Column"]] = {
                    "type": "string",                 # refine if you have types
                    "label": c["Column"],
                    "description": c["Description"]
                }
        # naive key heuristic (override if you know real keys):
        if "keys" not in entry:
            guessed_keys = [c["Column"] for c in cols if "Number" in c["Column"] or "ID" in c["Column"]]
            if guessed_keys:
                entry["keys"] = guessed_keys[:2]
    manifest["entities"][e] = entry

# --- Build relations ---
for r in rel_df.collect():
    manifest["relations"].append({
        "name": r["Name"],
        "from": r["FromEntity"],
        "to":   r["ToEntity"],
        "cardinality": r["Cardinality"],
        "sql_join": r["Join"],
        "description": r["Description"]
    })

# (Optional) add a few synonyms & samples
manifest["synonyms"] = {
    "SO": "SalesOrder",
    "SO Item": "SalesOrderItem",
    "DL": "Delivery",
    "BL": "BillingDocument"
}
manifest["samples"] = [
    "List SalesOrderItem for SalesOrder SO-1001",
    "Count DeliveryItem per SalesOrder",
    "Show Shipment that serves Delivery 80012345",
    "Compare BilledQuantity vs ConfirmedQuantity by SalesOrder"
]

# Save YAML
dbutils.fs.mkdirs(VOLUME_ROOT)
dbutils.fs.put(MANIFEST_PATH, yaml.safe_dump(manifest, sort_keys=False, allow_unicode=True), overwrite=True)
print("✅ Wrote semantic manifest to:", MANIFEST_PATH)


# COMMAND ----------


errors = []

# Check tables exist
for label, ent in manifest["entities"].items():
    table_fqn = ent.get("table")
    try:
        cols = [f.name for f in spark.table(table_fqn).schema.fields]
    except Exception as e:
        errors.append(f"[ENTITY] Table not found: {label} -> {table_fqn} ({e})")
        continue

    # Check key columns exist (if defined)
    for k in ent.get("keys", []):
        if k not in cols:
            errors.append(f"[ENTITY] Key not found in {table_fqn}: {k}")

# Check join columns exist
import re
def parse_cols(expr):
    # crude extractor for patterns like Alias.Column (SalesOrder.SalesOrderNumber)
    return re.findall(r'([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)', expr or "")

for rel in manifest["relations"]:
    join = rel.get("sql_join","")
    pairs = parse_cols(join)
    # Resolve alias -> table from entity labels: left alias is expected to match entity label
    alias_to_table = {
        rel["from"]: manifest["entities"][rel["from"]]["table"],
        rel["to"]:   manifest["entities"][rel["to"]]["table"]
    }
    for alias, col in pairs:
        if alias not in alias_to_table:
            errors.append(f"[JOIN] Alias '{alias}' not recognized in relation '{rel['name']}'")
            continue
        try:
            cols = [f.name for f in spark.table(alias_to_table[alias]).schema.fields]
            if col not in cols:
                errors.append(f"[JOIN] Column '{alias}.{col}' not found in {alias_to_table[alias]}")
        except Exception as e:
            errors.append(f"[JOIN] Table load error for alias {alias} -> {alias_to_table[alias]} ({e})")

print("Validation complete.")
if errors:
    print("❗Issues found:")
    for e in errors: print(" -", e)
else:
    print("✅ No issues detected.")


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE otif_ctlg.otif_schema.ontology_paths (
# MAGIC     start_entity STRING,
# MAGIC     end_entity STRING,
# MAGIC     path STRING
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO otif_ctlg.otif_schema.ontology_paths VALUES
# MAGIC ('salesorderheader','salesorderitem',
# MAGIC 'salesorderheader → salesorderitem'),
# MAGIC  
# MAGIC ('salesorderheader','scheduleline',
# MAGIC 'salesorderheader → salesorderitem → scheduleline'),
# MAGIC  
# MAGIC ('salesorderheader','deliveryheader',
# MAGIC 'salesorderheader → salesorderitem → deliveryitem → deliveryheader'),
# MAGIC  
# MAGIC ('salesorderheader','shipmentheader',
# MAGIC 'salesorderheader → salesorderitem → deliveryitem → shipmentitem → shipmentheader'),
# MAGIC  
# MAGIC ('salesorderheader','billingheader',
# MAGIC 'salesorderheader → salesorderitem → deliveryitem → billingitem → billingheader'),
# MAGIC  
# MAGIC ('deliveryheader','shipmentheader',
# MAGIC 'deliveryheader → shipmentitem → shipmentheader'),
# MAGIC  
# MAGIC ('deliveryheader','billingheader',
# MAGIC 'deliveryheader → billingitem → billingheader'),
# MAGIC  
# MAGIC ('shipmentheader','deliveryheader',
# MAGIC 'shipmentheader → shipmentitem → deliveryheader'),
# MAGIC  
# MAGIC ('billingheader','deliveryheader',
# MAGIC 'billingheader → billingitem → deliveryheader'),
# MAGIC  
# MAGIC ('salesorderitem','deliveryitem',
# MAGIC 'salesorderitem → deliveryitem'),
# MAGIC  
# MAGIC ('salesorderitem','shipmentitem',
# MAGIC 'salesorderitem → deliveryitem → shipmentitem'),
# MAGIC  
# MAGIC ('salesorderitem','billingitem',
# MAGIC 'salesorderitem → deliveryitem → billingitem');

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from otif_ctlg.otif_schema.ontology_paths