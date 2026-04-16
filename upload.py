"""
Housing Data → Snowflake Pipeline
==================================
下载 Zillow + Redfin 公开数据，分别上传到对应的 schema。

    HOUSING_DB.ZILLOW_RAW   ← Zillow 28 个数据集
    HOUSING_DB.REDFIN_RAW   ← Redfin 8 个数据集

数据流向:
    互联网（Zillow/Redfin 静态文件）
        → 内存（Python BytesIO）
        → gzip 压缩
        → Snowflake Internal Stage（临时存储区）
        → COPY INTO 目标表
        → Stage 文件自动删除（PURGE=TRUE）

运行方法:
    python3 upload.py             # Zillow + Redfin 全部
    python3 upload.py zillow      # 只跑 Zillow
    python3 upload.py redfin      # 只跑 Redfin

依赖:
    pip3 install requests snowflake-connector-python cryptography
"""

# io      — 内存中读写二进制数据，不需要在磁盘上创建临时文件
# gzip    — 压缩数据，减少传输量，Snowflake COPY INTO 原生支持 gzip
# sys     — 读取命令行参数（argv）
# requests            — 发 HTTP 请求下载文件
# snowflake.connector — 连接 Snowflake 的官方驱动
import io, gzip, sys, requests, snowflake.connector

# serialization  — 把 PEM 格式私钥转换成 Snowflake 要求的 DER 格式
# default_backend — cryptography 库的后端，指定用系统默认的加密实现
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend


# ── 配置 ──────────────────────────────────────────────────────────────────────
# 所有配置集中在顶部，修改时只需要改这里，不用翻整个文件
# 生产环境（GitHub Actions）这些值会改成从环境变量读取，避免硬编码

SNOWFLAKE_ACCOUNT   = "ARVSEUQ-RKC69324"       # Snowflake 账号标识符
SNOWFLAKE_USER      = "HOUSING_PIPELINE_USER"   # pipeline 专用账号，权限最小化
SNOWFLAKE_DATABASE  = "HOUSING_DB"              # 数据库名
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"              # 计算资源，XS 规格，60秒无操作自动挂起
SNOWFLAKE_ROLE      = "HOUSING_PIPELINE_ROLE"   # 专用角色，只有 ZILLOW_RAW/REDFIN_RAW 的写权限
PRIVATE_KEY_PATH    = "private_key.pem"         # RSA 私钥文件路径，用于替代密码认证


# ── 数据集定义 ─────────────────────────────────────────────────────────────────
# 用 tuple list 定义数据集：(Snowflake 表名, 文件相对路径)
# 优点：加新数据集只需在列表里加一行，上传逻辑完全不用改

ZILLOW_BASE = "https://files.zillowstatic.com/research/public_csvs"
# Zillow 把所有公开数据放在这个 CDN 上，URL 已稳定多年
# 完整 URL = ZILLOW_BASE + "/" + 文件路径

ZILLOW_DATASETS = [
    # ── 房价指数 ZHVI ──────────────────────────────────────────────────────────
    # ZHVI = Zillow Home Value Index，基于神经网络 Zestimate 估算的典型房价
    # tier_0.33_0.67 = 中间价位（35th-65th 百分位），最常用的基准指标
    ("ZHVI_METRO_ALL",      "zhvi/Metro_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"),
    ("ZHVI_STATE_ALL",      "zhvi/State_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"),
    ("ZHVI_COUNTY_ALL",     "zhvi/County_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"),
    ("ZHVI_ZIP_ALL",        "zhvi/Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"),
    ("ZHVI_METRO_SFR",      "zhvi/Metro_zhvi_uc_sfr_tier_0.33_0.67_sm_sa_month.csv"),
    ("ZHVI_METRO_1BED",     "zhvi/Metro_zhvi_bdrmcnt_1_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"),
    ("ZHVI_METRO_2BED",     "zhvi/Metro_zhvi_bdrmcnt_2_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"),
    ("ZHVI_METRO_3BED",     "zhvi/Metro_zhvi_bdrmcnt_3_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"),
    ("ZHVI_METRO_TOP",      "zhvi/Metro_zhvi_uc_sfrcondo_tier_0.67_1.0_sm_sa_month.csv"),
    ("ZHVI_METRO_BOTTOM",   "zhvi/Metro_zhvi_uc_sfrcondo_tier_0.0_0.33_sm_sa_month.csv"),

    # ── 房价预测 ZHVF ──────────────────────────────────────────────────────────
    # ZHVF = Zillow Home Value Forecast，提供 1/3/12 个月的预测涨跌幅
    ("ZHVF_METRO",          "zhvf_growth/Metro_zhvf_growth_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"),

    # ── 租金指数 ZORI ──────────────────────────────────────────────────────────
    # ZORI = Zillow Observed Rent Index，实际观测到的市场租金
    ("ZORI_METRO_ALL",      "zori/Metro_zori_uc_sfrcondomfr_sm_month.csv"),
    ("ZORI_ZIP_ALL",        "zori/Zip_zori_uc_sfrcondomfr_sm_month.csv"),
    ("ZORI_METRO_SFR",      "zori/Metro_zori_uc_sfr_sm_month.csv"),
    ("ZORI_METRO_MFR",      "zori/Metro_zori_uc_mfr_sm_month.csv"),

    # ── 在售库存 & 挂牌 ────────────────────────────────────────────────────────
    ("INVENTORY_METRO_MO",  "invt_fs/Metro_invt_fs_uc_sfrcondo_sm_month.csv"),
    ("INVENTORY_METRO_WK",  "invt_fs/Metro_invt_fs_uc_sfrcondo_sm_week.csv"),
    ("NEW_LISTINGS_MO",     "new_listings/Metro_new_listings_uc_sfrcondo_sm_month.csv"),
    ("NEW_LISTINGS_WK",     "new_listings/Metro_new_listings_uc_sfrcondo_sm_week.csv"),
    ("MEDIAN_LIST_PRICE",   "mlp/Metro_mlp_uc_sfrcondo_sm_month.csv"),

    # ── 成交数据 ───────────────────────────────────────────────────────────────
    ("SALES_COUNT",         "sales_count_now/Metro_sales_count_now_uc_sfrcondo_month.csv"),
    ("MEDIAN_SALE_PRICE",   "median_sale_price/Metro_median_sale_price_uc_sfrcondo_month.csv"),
    ("SALE_TO_LIST",        "sale_list_ratio/Metro_sale_list_ratio_uc_sfrcondo_sm_month.csv"),
    ("PCT_SOLD_ABOVE_LIST", "perc_sold_above_list/Metro_perc_sold_above_list_uc_sfrcondo_sm_month.csv"),

    # ── 市场速度 & 热度 ────────────────────────────────────────────────────────
    ("DAYS_TO_PENDING_MO",  "mean_doz_pending/Metro_mean_doz_pending_uc_sfrcondo_sm_month.csv"),
    ("DAYS_TO_PENDING_WK",  "mean_doz_pending/Metro_mean_doz_pending_uc_sfrcondo_sm_week.csv"),
    ("PRICE_CUT_SHARE",     "perc_listings_price_cut/Metro_perc_listings_price_cut_uc_sfrcondo_sm_month.csv"),
    ("MARKET_HEAT_INDEX",   "market_temp_index/Metro_market_temp_index_uc_sfrcondo_month.csv"),
]

REDFIN_BASE = "https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker"
# Redfin 把数据放在 AWS S3 公开 bucket 上
# 文件格式是 .tsv000.gz（Tab 分隔，gzip 压缩）

REDFIN_DATASETS = [
    # Redfin 数据是"长表"格式，每行是一个地区×时间段的所有指标
    # 包含：median_sale_price, homes_sold, inventory, median_dom,
    #       sold_above_list, price_drops, off_market_in_two_weeks 等
    ("NATIONAL",     f"{REDFIN_BASE}/us_national_market_tracker.tsv000.gz"),
    ("METRO",        f"{REDFIN_BASE}/redfin_metro_market_tracker.tsv000.gz"),
    ("STATE",        f"{REDFIN_BASE}/state_market_tracker.tsv000.gz"),
    ("COUNTY",       f"{REDFIN_BASE}/county_market_tracker.tsv000.gz"),
    ("CITY",         f"{REDFIN_BASE}/city_market_tracker.tsv000.gz"),
    ("ZIP",          f"{REDFIN_BASE}/zip_code_market_tracker.tsv000.gz"),
    ("NEIGHBORHOOD", f"{REDFIN_BASE}/neighborhood_market_tracker.tsv000.gz"),
    # 周度数据放在不同路径（COVID 时期开始发布，沿用至今）
    ("WEEKLY",       "https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_covid19/weekly_housing_market_data_most_recent.tsv000.gz"),
]


# ── Snowflake 连接 ─────────────────────────────────────────────────────────────

def get_conn(schema):
    """
    建立 Snowflake 连接，使用 RSA Key Pair 认证。

    为什么用 Key Pair 而不是密码：
    - 密码是明文字符串，存在代码或环境变量里都有泄露风险
    - Key Pair 用数字签名证明身份，私钥本身不在网络上传输
    - 是公司生产环境的标准做法

    为什么每次传入 schema 参数：
    - Zillow 数据写入 ZILLOW_RAW，Redfin 写入 REDFIN_RAW
    - 两个 schema 权限隔离，互不影响
    """

    # 从文件读取 PEM 格式私钥
    # "rb" = read binary，因为 PEM 文件是二进制格式
    with open(PRIVATE_KEY_PATH, "rb") as f:
        private_key = serialization.load_pem_private_key(
            f.read(),
            password=None,           # 生成私钥时没有加密码保护
            backend=default_backend()
        )

    # 把私钥从 PEM 格式转成 DER 格式
    # Snowflake connector 只接受 DER 格式
    # PEM 是人类可读的文本格式（就是你看到的那个 -----BEGIN...-----）
    # DER 是机器用的二进制格式，内容相同，只是编码方式不同
    pk = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    # 建立连接，返回 connection 对象
    # 后续所有 SQL 操作都通过这个 connection 的 cursor 执行
    return snowflake.connector.connect(
        account     = SNOWFLAKE_ACCOUNT,
        user        = SNOWFLAKE_USER,
        private_key = pk,            # 用私钥代替密码
        database    = SNOWFLAKE_DATABASE,
        schema      = schema,        # ZILLOW_RAW 或 REDFIN_RAW
        warehouse   = SNOWFLAKE_WAREHOUSE,
        role        = SNOWFLAKE_ROLE,
    )


# ── 上传单个数据集 ─────────────────────────────────────────────────────────────

def upload_dataset(table, url, cur, is_tsv=False):
    """
    下载一个数据集并上传到 Snowflake，完整流程：
        1. HTTP GET 下载文件到内存
        2. 如果是 CSV（Zillow），gzip 压缩；如果已经是 .gz（Redfin），直接用
        3. PUT 上传到 Snowflake Internal Stage（临时存储区）
        4. INFER_SCHEMA 自动推断列名和类型，CREATE TABLE（如果表不存在）
        5. COPY INTO 从 Stage 把数据导入目标表
        6. PURGE=TRUE 自动删除 Stage 里的临时文件

    参数:
        table   — Snowflake 目标表名
        url     — 文件下载地址
        cur     — Snowflake cursor（执行 SQL 的工具）
        is_tsv  — True = Tab 分隔（Redfin），False = 逗号分隔（Zillow）

    返回 True（成功）或 False（失败）
    """

    # ── 第一步：下载文件 ───────────────────────────────────────────────────────
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0"},  # 模拟浏览器，避免被服务器拒绝
            timeout=60    # 60秒超时，大文件（Redfin ZIP 600MB+）需要时间
        )
        # raise_for_status() 检查 HTTP 状态码
        # 200 = 成功，继续往下走
        # 404 = 文件不存在，403 = 没权限，都会抛出异常被 except 捕获
        resp.raise_for_status()
    except Exception as e:
        print(f"  ✗ 下载失败: {e}")
        return False  # 返回 False，调用方知道这个数据集失败了，继续跑下一个

    # ── 第二步：处理压缩 ───────────────────────────────────────────────────────
    if url.endswith(".gz"):
        # Redfin 文件已经是 gzip 压缩的，直接用原始字节
        data = resp.content
    else:
        # Zillow 文件是纯 CSV，需要手动压缩
        # 为什么压缩：减少传输量，Snowflake COPY INTO 原生支持 gzip
        # io.BytesIO() 在内存里创建一个"虚拟文件"，不需要写到磁盘
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
            gz.write(resp.content)  # 把 CSV 内容写入压缩流
        data = buf.getvalue()       # 拿出压缩后的全部字节

    # ── 第三步：PUT 上传到 Stage ───────────────────────────────────────────────
    # Internal Stage 是 Snowflake 内部的临时存储区，类似一个私有 S3
    # file:///dev/stdin 是 Unix 标准输入，配合 file_stream 把内存数据当文件上传
    # AUTO_COMPRESS=FALSE — 不要 Snowflake 再压一次，我们已经压好了
    # OVERWRITE=TRUE      — 同名文件直接覆盖，重跑时不会报错
    stage_file = f"{table}.gz"
    cur.execute(
        f"PUT file:///dev/stdin @PIPELINE_STAGE/{stage_file} "
        f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE",
        file_stream=io.BytesIO(data)    # 把内存数据作为文件流传给 Snowflake
    )

    # ── 第四步：定义文件格式 ───────────────────────────────────────────────────
    # 告诉 Snowflake 怎么解析文件里的内容
    # PARSE_HEADER=TRUE   — 第一行是列名，不当数据处理
    # NULL_IF=('','NULL') — 空字符串和文本"NULL"都转成真正的 SQL NULL
    if is_tsv:
        # Redfin 用 Tab 分隔
        ff = "(TYPE=CSV FIELD_DELIMITER='\\t' FIELD_OPTIONALLY_ENCLOSED_BY='\"' PARSE_HEADER=TRUE NULL_IF=('','NULL'))"
    else:
        # Zillow 用逗号分隔
        ff = "(TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY='\"' PARSE_HEADER=TRUE NULL_IF=('','NULL'))"

    # ── 第五步：自动建表 ───────────────────────────────────────────────────────
    # INFER_SCHEMA 要求用命名的 file format，不能内联写
    # 所以先建一个临时 file format，用完删掉
    fmt_name = f"_FMT_{table}"
    if is_tsv:
        cur.execute(f"""
            CREATE OR REPLACE TEMPORARY FILE FORMAT {fmt_name}
            TYPE = CSV
            FIELD_DELIMITER = '\\t'
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            PARSE_HEADER = TRUE
            NULL_IF = ('', 'NULL')
        """)
    else:
        cur.execute(f"""
            CREATE OR REPLACE TEMPORARY FILE FORMAT {fmt_name}
            TYPE = CSV
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            PARSE_HEADER = TRUE
            NULL_IF = ('', 'NULL')
        """)

    # INFER_SCHEMA 读取文件 header，自动推断每列名字和类型
    # USING TEMPLATE 用推断结果作为建表列定义
    # IF NOT EXISTS — 表已存在就跳过，保证幂等
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table}
        USING TEMPLATE (
            SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
            FROM TABLE(INFER_SCHEMA(
                LOCATION => '@PIPELINE_STAGE/{stage_file}',
                FILE_FORMAT => '{fmt_name}'
            ))
        )
    """)

    # ── 第六步：COPY INTO ──────────────────────────────────────────────────────
    # 从 Stage 批量导入数据到目标表
    # MATCH_BY_COLUMN_NAME — 按列名匹配，文件里列的顺序不重要
    # PURGE = TRUE         — 导完自动删除 Stage 里的临时文件
    cur.execute(f"""
        COPY INTO {table}
        FROM @PIPELINE_STAGE/{stage_file}
        FILE_FORMAT = (FORMAT_NAME = '{fmt_name}')
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        PURGE = TRUE
    """)

    return True  # 所有步骤成功


# ── Zillow 主流程 ──────────────────────────────────────────────────────────────

def run_zillow():
    """
    遍历 ZILLOW_DATASETS，把每个数据集上传到 HOUSING_DB.ZILLOW_RAW。
    每个数据集上传成功后立即 commit，失败不影响已成功的。
    """
    print("=== Zillow → HOUSING_DB.ZILLOW_RAW ===")

    # 连接到 ZILLOW_RAW schema
    conn = get_conn("ZILLOW_RAW")

    # cursor 是执行 SQL 的工具，相当于数据库里的一个查询窗口
    # 一个 connection 可以开多个 cursor，这里只需要一个
    cur = conn.cursor()

    # 确保 Stage 存在，IF NOT EXISTS 保证幂等
    cur.execute("CREATE STAGE IF NOT EXISTS PIPELINE_STAGE")

    success = failed = 0

    for table, path in ZILLOW_DATASETS:
        # end=" ... " — 不换行，等结果出来后在同一行打印 ✓ 或 ✗
        # flush=True  — 立即输出，不等缓冲区，这样能实时看到进度
        print(f"  ▶ {table}", end=" ... ", flush=True)

        if upload_dataset(table, f"{ZILLOW_BASE}/{path}", cur, is_tsv=False):
            conn.commit()   # 每个数据集成功后立即提交事务
            print("✓")
            success += 1
        else:
            # 失败了记录下来，继续跑下一个，不中断整个 pipeline
            print("✗")
            failed += 1

    cur.close()     # 关闭 cursor，释放资源
    conn.close()    # 关闭连接
    print(f"  Zillow 完成  ✓{success}  ✗{failed}\n")
    return failed   # 返回失败数量，让主程序决定是否报错退出


# ── Redfin 主流程 ──────────────────────────────────────────────────────────────

def run_redfin():
    """
    遍历 REDFIN_DATASETS，把每个数据集上传到 HOUSING_DB.REDFIN_RAW。
    逻辑与 run_zillow 完全相同，is_tsv=True 因为 Redfin 是 Tab 分隔。
    """
    print("=== Redfin → HOUSING_DB.REDFIN_RAW ===")

    conn = get_conn("REDFIN_RAW")
    cur  = conn.cursor()
    cur.execute("CREATE STAGE IF NOT EXISTS PIPELINE_STAGE")

    success = failed = 0

    for table, url in REDFIN_DATASETS:
        print(f"  ▶ {table}", end=" ... ", flush=True)

        if upload_dataset(table, url, cur, is_tsv=True):  # is_tsv=True：Tab 分隔
            conn.commit()
            print("✓")
            success += 1
        else:
            print("✗")
            failed += 1

    cur.close()
    conn.close()
    print(f"  Redfin 完成  ✓{success}  ✗{failed}\n")
    return failed


# ── 入口 ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # if __name__ == "__main__" — 只有直接运行这个文件时才执行以下代码
    # 如果被其他脚本 import，这段不会运行
    # 这是 Python 的标准写法，让脚本既能直接运行，也能被其他模块引用

    # sys.argv 是命令行参数列表
    # sys.argv[0] = "upload.py"（脚本文件名本身，永远是第一个）
    # sys.argv[1] = 你传入的第一个参数，比如 "zillow" 或 "redfin"
    # len(sys.argv) > 1 检查有没有传参数，没有就默认 "all"
    source = sys.argv[1].lower() if len(sys.argv) > 1 else "all"

    print(f"连接 Snowflake ({SNOWFLAKE_ACCOUNT})...\n")

    total_failed = 0

    # 根据参数决定跑哪个
    if source in ("all", "zillow"):
        total_failed += run_zillow()
    if source in ("all", "redfin"):
        total_failed += run_redfin()

    # sys.exit() 设置程序的退出码
    # 退出码 0 = 成功，非零 = 失败
    # GitHub Actions 检测到非零退出码会把这次 run 标记为失败，发送告警
    if total_failed > 0:
        print(f"⚠️  {total_failed} 个数据集失败，请检查上方错误信息")
        sys.exit(1)
    else:
        print("✅ 全部完成")
        sys.exit(0)
