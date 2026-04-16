"""
Fairfax County Public Real Estate → Snowflake
===============================================
从 Fairfax County ArcGIS REST API 拉取 8 张房产公开数据表，
写入 HOUSING_DB.FAIRFAX_RAW。

表和策略：
    FAIRFAX_SALES        增量（SALEDT）   每周两次更新
    FAIRFAX_PARCELS      全量             每月更新
    FAIRFAX_ASSESSMENT   全量             每年更新（1月1日）
    FAIRFAX_LAND         全量             每年更新
    FAIRFAX_DWELLING     全量             每年更新
    FAIRFAX_LEGAL        全量             每年更新
    FAIRFAX_ADDITION     全量             每年更新
    FAIRFAX_MARKET_RATIO 全量             每年更新

运行方法：
    python3 fairfax_public.py

依赖：
    pip3 install requests snowflake-connector-python cryptography
"""

import csv, io, gzip, sys, logging, requests, snowflake.connector
from datetime import datetime, timezone
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend


# ── 日志 ──────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


# ── 配置 ──────────────────────────────────────────────────────────────────────

SNOWFLAKE_ACCOUNT   = "ARVSEUQ-RKC69324"
SNOWFLAKE_USER      = "HOUSING_PIPELINE_USER"
SNOWFLAKE_DATABASE  = "HOUSING_DB"
SNOWFLAKE_SCHEMA    = "FAIRFAX_RAW"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
SNOWFLAKE_ROLE      = "HOUSING_PIPELINE_ROLE"
PRIVATE_KEY_PATH    = "private_key.pem"

# ArcGIS Feature Server 基础 URL
FAIRFAX_BASE = "https://services1.arcgis.com/ioennV6PpG5Xodq0/ArcGIS/rest/services"



# Watermark 表：记录每张表上次增量同步到的日期
WATERMARK_TABLE = "PIPELINE_WATERMARK"

# Stage：临时存放上传文件
STAGE_NAME = "PIPELINE_STAGE"

# ArcGIS 每页最多返回 2000 条
PAGE_SIZE = 1000

# ── Fairfax 表定义 ─────────────────────────────────────────────────────────────
# incremental=True  → 用 date_field 做增量拉取，MERGE 写入
# incremental=False → 全量拉取，TRUNCATE + COPY INTO 写入

FAIRFAX_TABLES = [
    {
        "table":       "FAIRFAX_SALES",
        "api":         "OpenData_A5/FeatureServer/1",
        "date_field":  "SALEDT",       # 成交日期，有新成交就拉
        "incremental": True,
        "desc":        "成交记录（每周两次更新）",
    },
    {
        "table":       "FAIRFAX_PARCELS",
        "api":         "OpenData_A6/FeatureServer/1",
        "date_field":  None,
        "incremental": False,
        "desc":        "地块信息 - LIVUNIT/LUC_DESC/ZONING_DESC（每月更新）",
    },
    {
        "table":       "FAIRFAX_ASSESSMENT",
        "api":         "OpenData_A6/FeatureServer/2",
        "date_field":  None,
        "incremental": False,
        "desc":        "评估价值 - APRTOT/APRLAND/APRBLDG（每年1月更新）",
    },
    {
        "table":       "FAIRFAX_LAND",
        "api":         "OpenData_A6/FeatureServer/3",
        "date_field":  None,
        "incremental": False,
        "desc":        "土地信息 - SF/ACRES/CODE_DESC（每年更新）",
    },
    {
        "table":       "FAIRFAX_DWELLING",
        "api":         "OpenData_A7/FeatureServer/2",
        "date_field":  None,
        "incremental": False,
        "desc":        "住宅特征 - YRBLT/RMBED/SFLA/STYLE_DESC（每年更新）",
    },
    {
        "table":       "FAIRFAX_LEGAL",
        "api":         "OpenData_A7/FeatureServer/1",
        "date_field":  None,
        "incremental": False,
        "desc":        "法律信息 - 街道地址/ZIP/ACRES（每年更新）",
    },
    {
        "table":       "FAIRFAX_ADDITION",
        "api":         "OpenData_A4/FeatureServer/1",
        "date_field":  None,
        "incremental": False,
        "desc":        "附属建筑 - 露台/甲板/门廊面积（每年更新）",
    },
    {
        "table":       "FAIRFAX_MARKET_RATIO",
        "api":         "OpenData_S4/FeatureServer/1",
        "date_field":  None,
        "incremental": False,
        "desc":        "市场价/评估价比值 - MARKE_SALE_RATIO/MARKE_VALUE/ASSES_VALUE（每年更新）",
    },
]


# ── Snowflake 连接 ─────────────────────────────────────────────────────────────

def get_conn():
    """读取私钥文件，建立 Snowflake 连接，连接到 FAIRFAX_RAW schema。"""
    with open(PRIVATE_KEY_PATH, "rb") as f:
        private_key = serialization.load_pem_private_key(
            f.read(), password=None, backend=default_backend()
        )
    pk = private_key.private_bytes(
        serialization.Encoding.DER,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption()
    )
    return snowflake.connector.connect(
        account     = SNOWFLAKE_ACCOUNT,
        user        = SNOWFLAKE_USER,
        private_key = pk,
        database    = SNOWFLAKE_DATABASE,
        schema      = SNOWFLAKE_SCHEMA,
        warehouse   = SNOWFLAKE_WAREHOUSE,
        role        = SNOWFLAKE_ROLE,
    )


# ── Watermark 管理 ─────────────────────────────────────────────────────────────

def init_watermark_table(cur):
    """建 watermark 表（如果不存在）。"""
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {WATERMARK_TABLE} (
            TABLE_NAME  VARCHAR(128) PRIMARY KEY,
            LAST_DATE   TIMESTAMP_TZ,
            LAST_RUN_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
        )
    """)


def get_watermark(cur, table_name):
    """读取某张表上次增量同步到的日期，没有记录返回 None（触发全量）。"""
    cur.execute(f"""
        SELECT LAST_DATE FROM {WATERMARK_TABLE}
        WHERE TABLE_NAME = %s
    """, (table_name,))
    row = cur.fetchone()
    return row[0] if row else None


def set_watermark(cur, table_name, last_date):
    """更新 watermark，用 MERGE 保证幂等。"""
    cur.execute(f"""
        MERGE INTO {WATERMARK_TABLE} AS tgt
        USING (
            SELECT %s AS TABLE_NAME, %s::TIMESTAMP_TZ AS LAST_DATE
        ) AS src ON tgt.TABLE_NAME = src.TABLE_NAME
        WHEN MATCHED THEN
            UPDATE SET LAST_DATE = src.LAST_DATE,
                       LAST_RUN_AT = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
            INSERT (TABLE_NAME, LAST_DATE)
            VALUES (src.TABLE_NAME, src.LAST_DATE)
    """, (table_name, last_date.isoformat()))


# ── ArcGIS REST API 拉取 ───────────────────────────────────────────────────────

def fetch_arcgis(api_path, date_field, since):
    """
    从 ArcGIS Feature Server 拉取数据，支持增量和全量。
    since=None  → where=1=1 全量
    since=日期  → where=date_field > 'since' 增量
    用 resultOffset 分页，每页 2000 条。
    """
    url = f"{FAIRFAX_BASE}/{api_path}/query"

    # 构造 WHERE 条件
    if since and date_field:
        since_str = since.strftime("%Y-%m-%d %H:%M:%S")
        where = f"{date_field} > TIMESTAMP '{since_str}'"
    else:
        where = "1=1"

    all_records = []
    offset = 0

    while True:
        params = {
            "where":             where,
            "outFields":         "*",
            "returnGeometry":    "false",
            "resultRecordCount": PAGE_SIZE,
            "resultOffset":      offset,
            "orderByFields":     date_field if date_field else "OBJECTID",
            "f":                 "json",
        }

        resp = requests.get(url, params=params, timeout=120)
        resp.raise_for_status()
        data = resp.json()

        if "error" in data:
            log.error(f"ArcGIS API 返回错误: {data['error']}")
            raise RuntimeError(f"ArcGIS API 错误: {data['error']}")

        features = data.get("features", [])
        all_records.extend([f["attributes"] for f in features])

        log.info(f"    offset={offset} 拿到 {len(features)} 条，累计 {len(all_records)} 条")

        if len(features) < PAGE_SIZE or not data.get("exceededTransferLimit", True):
            break

        offset += PAGE_SIZE

    return all_records


# ── 写入 Snowflake（增量：MERGE）─────────────────────────────────────────────

def load_incremental(records, table_name, cur, conn):
    """
    增量写入：list of dict → CSV → gzip → PUT → COPY INTO 临时表 → MERGE 目标表
    用 OBJECTID 去重，重跑不产生重复数据。
    """
    if not records:
        log.info(f"    无新记录，跳过")
        return 0

    # list of dict → CSV（内存）
    csv_buf = io.StringIO()
    writer = csv.DictWriter(csv_buf, fieldnames=records[0].keys())
    writer.writeheader()
    writer.writerows(records)

    # CSV → gzip
    gz_buf = io.BytesIO()
    with gzip.GzipFile(fileobj=gz_buf, mode="wb") as gz:
        gz.write(csv_buf.getvalue().encode("utf-8"))
    gz_buf.seek(0)

    stage_file = f"{table_name}_inc.csv.gz"

    # PUT
    cur.execute(
        f"PUT file:///dev/stdin @{STAGE_NAME}/{stage_file} "
        f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE",
        file_stream=gz_buf
    )

    # 建命名 file format
    fmt = f"_FMT_{table_name}"
    cur.execute(f"""
        CREATE OR REPLACE TEMPORARY FILE FORMAT {fmt}
        TYPE = CSV
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        PARSE_HEADER = TRUE
        NULL_IF = ('', 'NULL')
    """)

    # 建目标表（如果不存在）
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        USING TEMPLATE (
            SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
            FROM TABLE(INFER_SCHEMA(
                LOCATION    => '@{STAGE_NAME}/{stage_file}',
                FILE_FORMAT => '{fmt}'
            ))
        )
    """)

    # COPY INTO 临时表
    tmp = f"_TMP_{table_name}"
    cur.execute(f"CREATE OR REPLACE TEMPORARY TABLE {tmp} LIKE {table_name}")
    cur.execute(f"""
        COPY INTO {tmp}
        FROM @{STAGE_NAME}/{stage_file}
        FILE_FORMAT = (FORMAT_NAME = '{fmt}')
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        PURGE = TRUE
    """)

    # MERGE 到目标表（OBJECTID 去重）
    cols = list(records[0].keys())
    set_clause  = ", ".join([f"tgt.{c} = src.{c}" for c in cols])
    insert_cols = ", ".join(cols)
    insert_vals = ", ".join([f"src.{c}" for c in cols])

    cur.execute(f"""
        MERGE INTO {table_name} AS tgt
        USING {tmp} AS src ON tgt.OBJECTID = src.OBJECTID
        WHEN MATCHED THEN
            UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols}) VALUES ({insert_vals})
    """)

    rows = cur.rowcount
    conn.commit()
    log.info(f"    MERGE 完成：{rows} 行")
    return rows


# ── 写入 Snowflake（全量：TRUNCATE + COPY INTO）───────────────────────────────

def load_full(records, table_name, cur, conn):
    """
    全量写入：先清空目标表，再全量导入。
    适合数据量不大、没有增量字段的表。
    """
    if not records:
        log.info(f"    无数据，跳过")
        return 0

    # list of dict → CSV → gzip
    csv_buf = io.StringIO()
    writer = csv.DictWriter(csv_buf, fieldnames=records[0].keys())
    writer.writeheader()
    writer.writerows(records)

    gz_buf = io.BytesIO()
    with gzip.GzipFile(fileobj=gz_buf, mode="wb") as gz:
        gz.write(csv_buf.getvalue().encode("utf-8"))
    gz_buf.seek(0)

    stage_file = f"{table_name}_full.csv.gz"

    # PUT
    cur.execute(
        f"PUT file:///dev/stdin @{STAGE_NAME}/{stage_file} "
        f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE",
        file_stream=gz_buf
    )

    # 建命名 file format
    fmt = f"_FMT_{table_name}"
    cur.execute(f"""
        CREATE OR REPLACE TEMPORARY FILE FORMAT {fmt}
        TYPE = CSV
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        PARSE_HEADER = TRUE
        NULL_IF = ('', 'NULL')
    """)

    # 每次全量重建表，确保 schema 与当前数据一致
    cur.execute(f"""
        CREATE OR REPLACE TABLE {table_name}
        USING TEMPLATE (
            SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
            FROM TABLE(INFER_SCHEMA(
                LOCATION    => '@{STAGE_NAME}/{stage_file}',
                FILE_FORMAT => '{fmt}'
            ))
        )
    """)
    cur.execute(f"""
        COPY INTO {table_name}
        FROM @{STAGE_NAME}/{stage_file}
        FILE_FORMAT = (FORMAT_NAME = '{fmt}')
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        PURGE = TRUE
    """)

    results = cur.fetchall()
    rows = sum(r[3] for r in results) if results else 0
    conn.commit()
    log.info(f"    全量导入完成：{rows} 行")
    return rows


# ── Fairfax 主流程 ─────────────────────────────────────────────────────────────

def run_fairfax(cur, conn, only=None):
    """
    遍历 FAIRFAX_TABLES，对每张表：
    - incremental=True  → 读 watermark → 增量拉取 → MERGE → 更新 watermark
    - incremental=False → 全量拉取 → TRUNCATE + COPY INTO
    only: 只跑指定表名（大写），None 表示全部
    """
    log.info("=== Fairfax County → HOUSING_DB.FAIRFAX_RAW ===")

    for t in FAIRFAX_TABLES:
        if only and t["table"] not in only:
            continue
        table      = t["table"]
        api        = t["api"]
        date_field = t["date_field"]
        is_inc     = t["incremental"]

        log.info(f"\n▶ {table}（{t['desc']}）")

        try:
            if is_inc:
                # 增量：读 watermark，只拉新数据
                watermark = get_watermark(cur, table)
                if watermark:
                    log.info(f"  上次同步：{watermark.strftime('%Y-%m-%d')}")
                else:
                    log.info(f"  首次运行，全量拉取")

                records = fetch_arcgis(api, date_field, watermark)
                log.info(f"  共拉取 {len(records)} 条")

                if records:
                    load_incremental(records, table, cur, conn)

                    # 更新 watermark 到本批最新日期
                    # ArcGIS 日期是毫秒时间戳，除以 1000 转成秒
                    latest_ts = max(r[date_field] for r in records if r.get(date_field))
                    latest_dt = datetime.fromtimestamp(latest_ts / 1000, tz=timezone.utc)
                    set_watermark(cur, table, latest_dt)
                    conn.commit()
                    log.info(f"  watermark 更新至：{latest_dt.strftime('%Y-%m-%d')}")
                else:
                    log.info(f"  无新数据")

            else:
                # 全量：直接拉所有数据，覆盖目标表
                records = fetch_arcgis(api, None, None)
                log.info(f"  共拉取 {len(records)} 条")

                if records:
                    load_full(records, table, cur, conn)

        except Exception as e:
            # 单张表失败不中断整个 pipeline，继续跑下一张
            log.error(f"  ✗ {table} 失败: {e}", exc_info=True)
            # exc_info=True 打印完整的 traceback，方便定位具体哪行出错





# ── 入口 ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # 用法: python3 fairfax_public.py [TABLE1 TABLE2 ...]
    # 例如: python3 fairfax_public.py FAIRFAX_LAND
    only = [t.upper() for t in sys.argv[1:]] or None

    log.info(f"连接 Snowflake ({SNOWFLAKE_ACCOUNT})...")
    conn = get_conn()
    cur  = conn.cursor()

    cur.execute(f"CREATE STAGE IF NOT EXISTS {STAGE_NAME}")
    init_watermark_table(cur)
    conn.commit()
    log.info("✓ 连接成功")

    try:
        run_fairfax(cur, conn, only=only)
    except Exception as e:
        log.error(f"Pipeline 失败: {e}", exc_info=True)
        sys.exit(1)
    finally:
        cur.close()
        conn.close()

    log.info("\n✅ 全部完成")
