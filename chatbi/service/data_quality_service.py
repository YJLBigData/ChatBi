from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from chatbi.domain.geo_catalog import city_meta, city_names, province_meta, province_names
from chatbi.domain.product_catalog import allowed_brand_names, allowed_product_names, catalog_rows_by_sku
from chatbi.service.inventory_service import INVENTORY_STATUSES, ensure_inventory_runtime
from chatbi.repository.db import ensure_table_columns


DATA_QUALITY_RUN_DDL = """
CREATE TABLE IF NOT EXISTS `data_quality_run` (
    `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT '巡检ID',
    `run_type` VARCHAR(32) NOT NULL DEFAULT 'runtime' COMMENT '巡检类型',
    `status` VARCHAR(20) NOT NULL DEFAULT 'running' COMMENT '执行状态',
    `fixed_count` INT NOT NULL DEFAULT 0 COMMENT '修复记录数',
    `issue_count` INT NOT NULL DEFAULT 0 COMMENT '问题条数',
    `summary_json` LONGTEXT NULL COMMENT '巡检摘要',
    `started_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '开始时间',
    `finished_at` DATETIME NULL COMMENT '完成时间',
    PRIMARY KEY (`id`),
    KEY `idx_data_quality_run_started_at` (`started_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ChatBI原始数据质量巡检记录';
"""

DATA_QUALITY_ISSUE_DDL = """
CREATE TABLE IF NOT EXISTS `data_quality_issue` (
    `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT '问题ID',
    `run_id` BIGINT NOT NULL COMMENT '巡检ID',
    `issue_key` VARCHAR(80) NOT NULL COMMENT '问题编码',
    `severity` VARCHAR(20) NOT NULL COMMENT '问题级别',
    `affected_table` VARCHAR(64) NOT NULL COMMENT '影响表',
    `issue_count` INT NOT NULL DEFAULT 0 COMMENT '问题数量',
    `sample_value` VARCHAR(255) NULL COMMENT '样例值',
    `message` VARCHAR(500) NOT NULL COMMENT '问题说明',
    PRIMARY KEY (`id`),
    KEY `idx_data_quality_issue_run_id` (`run_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ChatBI原始数据质量问题明细';
"""

BUSINESS_TABLE_MIGRATIONS = {
    "user_info": {
        "province_code": "ALTER TABLE `user_info` ADD COLUMN `province_code` VARCHAR(12) NOT NULL DEFAULT '' COMMENT '常住省份编码' AFTER `province`",
        "city_code": "ALTER TABLE `user_info` ADD COLUMN `city_code` VARCHAR(12) NOT NULL DEFAULT '' COMMENT '常住城市编码' AFTER `city`",
    },
    "store_info": {
        "province_code": "ALTER TABLE `store_info` ADD COLUMN `province_code` VARCHAR(12) NOT NULL DEFAULT '' COMMENT '所在省份编码' AFTER `province`",
        "city_code": "ALTER TABLE `store_info` ADD COLUMN `city_code` VARCHAR(12) NOT NULL DEFAULT '' COMMENT '所在城市编码' AFTER `city`",
    },
    "product_info": {
        "barcode": "ALTER TABLE `product_info` ADD COLUMN `barcode` VARCHAR(32) NOT NULL DEFAULT '' COMMENT '商品条码' AFTER `sku_code`",
        "shelf_life_days": "ALTER TABLE `product_info` ADD COLUMN `shelf_life_days` INT NOT NULL DEFAULT 0 COMMENT '保质期天数' AFTER `cost_price`",
    },
    "order_master": {
        "coupon_amount": "ALTER TABLE `order_master` ADD COLUMN `coupon_amount` DECIMAL(12,2) NOT NULL DEFAULT 0.00 COMMENT '优惠券抵扣金额' AFTER `refund_amount`",
        "promotion_type": "ALTER TABLE `order_master` ADD COLUMN `promotion_type` VARCHAR(32) NOT NULL DEFAULT '' COMMENT '促销类型' AFTER `coupon_amount`",
        "receiver_province_code": "ALTER TABLE `order_master` ADD COLUMN `receiver_province_code` VARCHAR(12) NOT NULL DEFAULT '' COMMENT '收货省份编码' AFTER `receiver_province`",
        "receiver_city_code": "ALTER TABLE `order_master` ADD COLUMN `receiver_city_code` VARCHAR(12) NOT NULL DEFAULT '' COMMENT '收货城市编码' AFTER `receiver_city`",
    },
}

GENERIC_DISTRICTS = {"核心商圈", "成熟社区", "校园周边", "写字楼区", "居民城区", "", "未知区域"}
PROMOTION_TYPES = ["满减", "会员价", "直播补贴", "单品直降", "组合购"]
ALLOWED_PRODUCT_NAMES = allowed_product_names()
ALLOWED_BRAND_NAMES = allowed_brand_names()
CATALOG_BY_SKU = catalog_rows_by_sku()


def ensure_data_quality_runtime(conn) -> None:
    with conn.cursor() as cursor:
        cursor.execute(DATA_QUALITY_RUN_DDL)
        cursor.execute(DATA_QUALITY_ISSUE_DDL)
        for table_name, migrations in BUSINESS_TABLE_MIGRATIONS.items():
            ensure_table_columns(cursor, table_name, migrations)
    ensure_inventory_runtime(conn)


def _pick_district(city: str, row_id: int) -> str:
    for province in province_names():
        if city in city_names(province):
            districts = city_meta(province, city)["districts"]
            return districts[(row_id - 1) % len(districts)]
    return "城区"


def _execute_fix(cursor, sql: str, params: tuple[Any, ...] | None = None) -> int:
    cursor.execute(sql, params or ())
    return cursor.rowcount or 0


def _backfill_codes_and_dimensions(cursor) -> int:
    fixed = 0
    for province in province_names():
        p_meta = province_meta(province)
        for city in city_names(province):
            c_meta = city_meta(province, city)
            fixed += _execute_fix(
                cursor,
                """
                UPDATE `user_info`
                SET `province_code`=%s, `city_code`=%s, `city_tier`=%s
                WHERE `province`=%s AND `city`=%s AND (
                    `province_code`='' OR `city_code`='' OR `city_tier` IN ('T1','T2','T3','')
                )
                """,
                (p_meta["province_code"], c_meta["city_code"], c_meta["city_tier"], province, city),
            )
            fixed += _execute_fix(
                cursor,
                """
                UPDATE `store_info`
                SET `province_code`=%s, `city_code`=%s
                WHERE `province`=%s AND `city`=%s AND (
                    `province_code`='' OR `city_code`=''
                )
                """,
                (p_meta["province_code"], c_meta["city_code"], province, city),
            )
            fixed += _execute_fix(
                cursor,
                """
                UPDATE `order_master`
                SET `receiver_province_code`=%s, `receiver_city_code`=%s
                WHERE `receiver_province`=%s AND `receiver_city`=%s AND (
                    `receiver_province_code`='' OR `receiver_city_code`=''
                )
                """,
                (p_meta["province_code"], c_meta["city_code"], province, city),
            )
    return fixed


def _normalize_unknown_genders(cursor) -> int:
    return _execute_fix(
        cursor,
        """
        UPDATE `user_info`
        SET `gender` = CASE
            WHEN `is_mother` = 1 THEN '女'
            WHEN MOD(`user_id`, 2) = 0 THEN '女'
            ELSE '男'
        END
        WHERE `gender`='未知' OR `gender`='' OR `gender` IS NULL
        """,
    )


def _normalize_product_fields(cursor) -> int:
    fixed = 0
    fixed += _execute_fix(
        cursor,
        """
        UPDATE `product_info`
        SET `barcode` = CONCAT('69', LPAD(`product_id`, 11, '0'))
        WHERE `barcode`='' OR `barcode` IS NULL
        """,
    )
    fixed += _execute_fix(
        cursor,
        """
        UPDATE `product_info`
        SET `shelf_life_days` = CASE
            WHEN `category_l1` IN ('液态奶', '含乳饮料') THEN 180
            WHEN `category_l1` IN ('低温酸奶', '低温饮品', '鲜奶') THEN 25
            WHEN `category_l1` = '冰淇淋' THEN 365
            ELSE 180
        END
        WHERE `shelf_life_days` <= 0
        """,
    )
    return fixed


def _normalize_order_promotions(cursor) -> int:
    fixed = 0
    fixed += _execute_fix(
        cursor,
        """
        UPDATE `order_master`
        SET `coupon_amount` = ROUND(`discount_amount` * 0.65, 2)
        WHERE `coupon_amount` IS NULL OR `coupon_amount` = 0
        """,
    )
    cases = " ".join(
        f"WHEN MOD(`order_id`, {len(PROMOTION_TYPES)}) = {idx} THEN '{name}'"
        for idx, name in enumerate(PROMOTION_TYPES)
    )
    fixed += _execute_fix(
        cursor,
        f"""
        UPDATE `order_master`
        SET `promotion_type` = CASE {cases} ELSE '满减' END
        WHERE `promotion_type`='' OR `promotion_type` IS NULL
        """,
    )
    return fixed


def _normalize_mengniu_product_catalog(cursor) -> int:
    fixed = 0

    product_updates = [
        (
            item["brand"],
            item["spu"],
            item["name"],
            item["cat1"],
            item["cat2"],
            item["capacity"],
            item["package"],
            item["channel"],
            item["target"],
            sku_code,
        )
        for sku_code, item in CATALOG_BY_SKU.items()
    ]
    if product_updates:
        cursor.executemany(
            """
            UPDATE `product_info`
            SET `brand_name`=%s,
                `spu_name`=%s,
                `product_name`=%s,
                `category_l1`=%s,
                `category_l2`=%s,
                `capacity_desc`=%s,
                `package_type`=%s,
                `channel_type`=%s,
                `target_group`=%s
            WHERE `sku_code`=%s AND (
                `brand_name`<>%s OR
                `spu_name`<>%s OR
                `product_name`<>%s OR
                `category_l1`<>%s OR
                `category_l2`<>%s OR
                `capacity_desc`<>%s OR
                `package_type`<>%s OR
                `channel_type`<>%s OR
                `target_group`<>%s
            )
            """,
            [
                update + update[:-1]
                for update in product_updates
            ],
        )
        fixed += cursor.rowcount or 0

    fixed += _execute_fix(
        cursor,
        """
        UPDATE `order_detail` od
        JOIN `product_info` p ON od.`product_id` = p.`product_id`
        SET od.`product_name` = p.`product_name`,
            od.`brand_name` = p.`brand_name`,
            od.`category_l1` = p.`category_l1`,
            od.`category_l2` = p.`category_l2`
        WHERE od.`product_name` NOT IN ({product_names})
           OR od.`brand_name` NOT IN ({brand_names})
           OR od.`product_name` IS NULL
           OR od.`brand_name` IS NULL
           OR od.`product_name` = ''
           OR od.`brand_name` = ''
        """.format(
            product_names=", ".join(["%s"] * len(ALLOWED_PRODUCT_NAMES)),
            brand_names=", ".join(["%s"] * len(ALLOWED_BRAND_NAMES)),
        ),
        tuple(sorted(ALLOWED_PRODUCT_NAMES)) + tuple(sorted(ALLOWED_BRAND_NAMES)),
    )

    fixed += _execute_fix(
        cursor,
        """
        UPDATE `refund_detail` rd
        JOIN `product_info` p ON rd.`product_id` = p.`product_id`
        SET rd.`product_name` = p.`product_name`
        WHERE rd.`product_name` NOT IN ({product_names})
           OR rd.`product_name` IS NULL
           OR rd.`product_name` = ''
        """.format(product_names=", ".join(["%s"] * len(ALLOWED_PRODUCT_NAMES))),
        tuple(sorted(ALLOWED_PRODUCT_NAMES)),
    )
    return fixed


def _normalize_generic_districts(cursor) -> int:
    fixed = 0
    cursor.execute(
        """
        SELECT `store_id`, `city`
        FROM `store_info`
        WHERE `district` IN (%s, %s, %s, %s, %s, %s, %s)
        """,
        tuple(GENERIC_DISTRICTS),
    )
    store_updates = [
        (_pick_district(row["city"], int(row["store_id"])), row["store_id"])
        for row in cursor.fetchall()
    ]
    if store_updates:
        cursor.executemany(
            "UPDATE `store_info` SET `district`=%s WHERE `store_id`=%s",
            store_updates,
        )
        fixed += cursor.rowcount or 0

    cursor.execute(
        """
        SELECT `order_id`, `receiver_city`
        FROM `order_master`
        WHERE `receiver_district` IN (%s, %s, %s, %s, %s, %s, %s)
        """,
        tuple(GENERIC_DISTRICTS),
    )
    order_updates = [
        (_pick_district(row["receiver_city"], int(row["order_id"])), row["order_id"])
        for row in cursor.fetchall()
    ]
    if order_updates:
        batch_size = 5000
        for index in range(0, len(order_updates), batch_size):
            cursor.executemany(
                "UPDATE `order_master` SET `receiver_district`=%s WHERE `order_id`=%s",
                order_updates[index:index + batch_size],
            )
            fixed += cursor.rowcount or 0
    return fixed


def _collect_issue(cursor, issue_key: str, severity: str, affected_table: str, sql: str, message: str) -> dict[str, Any] | None:
    cursor.execute(sql)
    row = cursor.fetchone() or {}
    cnt = int(row.get("cnt") or 0)
    if cnt <= 0:
        return None
    return {
        "issue_key": issue_key,
        "severity": severity,
        "affected_table": affected_table,
        "issue_count": cnt,
        "sample_value": str(row.get("sample_value") or "").strip() or None,
        "message": message,
    }


def get_latest_data_quality_summary(conn) -> dict[str, Any]:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT `id`, `run_type`, `status`, `fixed_count`, `issue_count`, `summary_json`, `started_at`, `finished_at`
            FROM `data_quality_run`
            ORDER BY `id` DESC
            LIMIT 1
            """
        )
        run_row = cursor.fetchone() or {}
        if not run_row:
            return {"latest_run": {}, "issues": []}
        cursor.execute(
            """
            SELECT `issue_key`, `severity`, `affected_table`, `issue_count`, `sample_value`, `message`
            FROM `data_quality_issue`
            WHERE `run_id`=%s
            ORDER BY `issue_count` DESC, `id` DESC
            LIMIT 20
            """,
            (run_row["id"],),
        )
        issues = cursor.fetchall() or []
    latest_run = {
        "run_id": run_row.get("id"),
        "run_type": run_row.get("run_type"),
        "status": run_row.get("status"),
        "fixed_count": run_row.get("fixed_count"),
        "issue_count": run_row.get("issue_count"),
        "started_at": str(run_row.get("started_at") or ""),
        "finished_at": str(run_row.get("finished_at") or ""),
    }
    summary_json = str(run_row.get("summary_json") or "").strip()
    if summary_json:
        try:
            latest_run["summary"] = json.loads(summary_json)
        except json.JSONDecodeError:
            latest_run["summary"] = {}
    else:
        latest_run["summary"] = {}
    return {"latest_run": latest_run, "issues": issues}


def run_data_quality_audit(conn, run_type: str = "runtime", auto_fix: bool = True) -> dict[str, Any]:
    ensure_data_quality_runtime(conn)
    fixed_count = 0
    issues: list[dict[str, Any]] = []
    started_at = datetime.now()
    with conn.cursor() as cursor:
        cursor.execute(
            "INSERT INTO `data_quality_run` (`run_type`, `status`, `started_at`) VALUES (%s, 'running', %s)",
            (run_type, started_at),
        )
        run_id = cursor.lastrowid
        if auto_fix:
            fixed_count += _backfill_codes_and_dimensions(cursor)
            fixed_count += _normalize_unknown_genders(cursor)
            fixed_count += _normalize_product_fields(cursor)
            fixed_count += _normalize_mengniu_product_catalog(cursor)
            fixed_count += _normalize_order_promotions(cursor)
            fixed_count += _normalize_generic_districts(cursor)

        checks = [
            (
                "user_unknown_gender",
                "medium",
                "user_info",
                "SELECT COUNT(*) AS cnt, '未知' AS sample_value FROM `user_info` WHERE `gender`='未知' OR `gender`='' OR `gender` IS NULL",
                "用户性别仍存在未知值，影响用户画像分析稳定性。",
            ),
            (
                "user_blank_geo",
                "high",
                "user_info",
                "SELECT COUNT(*) AS cnt, MIN(CONCAT(COALESCE(`province`,''), '/', COALESCE(`city`,''))) AS sample_value FROM `user_info` WHERE `province`='' OR `city`='' OR `province` IS NULL OR `city` IS NULL OR `province_code`='' OR `city_code`=''",
                "用户常住省市或行政编码缺失。",
            ),
            (
                "store_blank_geo",
                "high",
                "store_info",
                "SELECT COUNT(*) AS cnt, MIN(CONCAT(COALESCE(`province`,''), '/', COALESCE(`city`,''))) AS sample_value FROM `store_info` WHERE `province`='' OR `city`='' OR `province` IS NULL OR `city` IS NULL OR `province_code`='' OR `city_code`=''",
                "门店省市或行政编码缺失。",
            ),
            (
                "generic_store_district",
                "medium",
                "store_info",
                "SELECT COUNT(*) AS cnt, MIN(`district`) AS sample_value FROM `store_info` WHERE `district` IN ('核心商圈','成熟社区','校园周边','写字楼区','居民城区','','未知区域')",
                "门店区县仍是通用占位值，不利于真实区域分析。",
            ),
            (
                "generic_receiver_district",
                "medium",
                "order_master",
                "SELECT COUNT(*) AS cnt, MIN(`receiver_district`) AS sample_value FROM `order_master` WHERE `receiver_district` IN ('核心商圈','成熟社区','校园周边','写字楼区','居民城区','','未知区域')",
                "订单收货区县仍是通用占位值，不利于地址分析。",
            ),
            (
                "order_blank_receiver_geo",
                "high",
                "order_master",
                "SELECT COUNT(*) AS cnt, MIN(CONCAT(COALESCE(`receiver_province`,''), '/', COALESCE(`receiver_city`,''))) AS sample_value FROM `order_master` WHERE `receiver_province`='' OR `receiver_city`='' OR `receiver_province` IS NULL OR `receiver_city` IS NULL OR `receiver_province_code`='' OR `receiver_city_code`=''",
                "订单收货地或行政编码缺失。",
            ),
            (
                "invalid_user_city_tier",
                "low",
                "user_info",
                "SELECT COUNT(*) AS cnt, MIN(`city_tier`) AS sample_value FROM `user_info` WHERE `city_tier` NOT IN ('一线','新一线','二线','三线')",
                "城市等级未标准化到常见企业口径。",
            ),
            (
                "product_barcode_missing",
                "medium",
                "product_info",
                "SELECT COUNT(*) AS cnt, MIN(`sku_code`) AS sample_value FROM `product_info` WHERE `barcode`='' OR `barcode` IS NULL OR `shelf_life_days`<=0",
                "产品条码或保质期缺失。",
            ),
            (
                "product_info_non_mengniu_name",
                "high",
                "product_info",
                "SELECT COUNT(*) AS cnt, MIN(`product_name`) AS sample_value FROM `product_info` WHERE `product_name` NOT IN ({}) OR `brand_name` NOT IN ({})".format(
                    ", ".join(["%s"] * len(ALLOWED_PRODUCT_NAMES)),
                    ", ".join(["%s"] * len(ALLOWED_BRAND_NAMES)),
                ),
                "产品主数据中存在非蒙牛白名单商品名或品牌名。",
            ),
            (
                "order_detail_non_mengniu_name",
                "high",
                "order_detail",
                "SELECT COUNT(*) AS cnt, MIN(`product_name`) AS sample_value FROM `order_detail` WHERE `product_name` NOT IN ({}) OR `brand_name` NOT IN ({})".format(
                    ", ".join(["%s"] * len(ALLOWED_PRODUCT_NAMES)),
                    ", ".join(["%s"] * len(ALLOWED_BRAND_NAMES)),
                ),
                "订单明细快照中存在非蒙牛白名单商品名或品牌名。",
            ),
            (
                "refund_detail_non_mengniu_name",
                "high",
                "refund_detail",
                "SELECT COUNT(*) AS cnt, MIN(`product_name`) AS sample_value FROM `refund_detail` WHERE `product_name` NOT IN ({})".format(
                    ", ".join(["%s"] * len(ALLOWED_PRODUCT_NAMES))
                ),
                "退款明细快照中存在非蒙牛白名单商品名。",
            ),
            (
                "orphans",
                "high",
                "fact_tables",
                """
                SELECT
                    (
                        (SELECT COUNT(*) FROM `order_master` o LEFT JOIN `store_info` s ON o.`store_id`=s.`store_id` WHERE s.`store_id` IS NULL) +
                        (SELECT COUNT(*) FROM `order_master` o LEFT JOIN `user_info` u ON o.`buyer_id`=u.`user_id` WHERE u.`user_id` IS NULL) +
                        (SELECT COUNT(*) FROM `order_detail` d LEFT JOIN `order_master` o ON d.`order_id`=o.`order_id` WHERE o.`order_id` IS NULL) +
                        (SELECT COUNT(*) FROM `refund_master` r LEFT JOIN `order_master` o ON r.`order_id`=o.`order_id` WHERE o.`order_id` IS NULL) +
                        (SELECT COUNT(*) FROM `refund_detail` d LEFT JOIN `refund_master` r ON d.`refund_id`=r.`refund_id` WHERE r.`refund_id` IS NULL) +
                        (SELECT COUNT(*) FROM `inventory_stock` i LEFT JOIN `store_info` s ON i.`store_id`=s.`store_id` WHERE s.`store_id` IS NULL) +
                        (SELECT COUNT(*) FROM `inventory_stock` i LEFT JOIN `product_info` p ON i.`product_id`=p.`product_id` WHERE p.`product_id` IS NULL)
                    ) AS cnt,
                    NULL AS sample_value
                """,
                "事实表存在孤儿记录。",
            ),
            (
                "invalid_amounts",
                "high",
                "fact_tables",
                """
                SELECT
                    (
                        (SELECT COUNT(*) FROM `order_master` WHERE `paid_amount` < 0 OR `gross_amount` < 0 OR `discount_amount` < 0) +
                        (SELECT COUNT(*) FROM `order_detail` WHERE `quantity` <= 0 OR `line_paid_amount` < 0 OR `line_gross_amount` < 0) +
                        (SELECT COUNT(*) FROM `refund_master` WHERE `refund_amount` < 0) +
                        (SELECT COUNT(*) FROM `refund_detail` WHERE `refund_quantity` <= 0 OR `refund_amount` < 0) +
                        (SELECT COUNT(*) FROM `inventory_stock` WHERE `on_hand_qty` < 0 OR `reserved_qty` < 0 OR `available_qty` < 0 OR `in_transit_qty` < 0 OR `safety_stock_qty` < 0 OR `damaged_qty` < 0 OR `inventory_amount` < 0)
                    ) AS cnt,
                    NULL AS sample_value
                """,
                "存在负数金额或非法件数。",
            ),
            (
                "inventory_qty_inconsistent",
                "high",
                "inventory_stock",
                """
                SELECT COUNT(*) AS cnt, MIN(CONCAT(`inventory_id`, ':', `available_qty`)) AS sample_value
                FROM `inventory_stock`
                WHERE `available_qty` <> GREATEST(`on_hand_qty` - `reserved_qty` - `damaged_qty`, 0)
                """,
                "库存表可售库存与在库/预占/残损的关系不一致。",
            ),
            (
                "inventory_status_invalid",
                "medium",
                "inventory_stock",
                "SELECT COUNT(*) AS cnt, MIN(`stock_status`) AS sample_value FROM `inventory_stock` WHERE `stock_status` NOT IN ({})".format(
                    ", ".join(["%s"] * len(INVENTORY_STATUSES))
                ),
                "库存状态不在企业约定枚举内。",
            ),
        ]
        issue_params_map = {
            "product_info_non_mengniu_name": tuple(sorted(ALLOWED_PRODUCT_NAMES)) + tuple(sorted(ALLOWED_BRAND_NAMES)),
            "order_detail_non_mengniu_name": tuple(sorted(ALLOWED_PRODUCT_NAMES)) + tuple(sorted(ALLOWED_BRAND_NAMES)),
            "refund_detail_non_mengniu_name": tuple(sorted(ALLOWED_PRODUCT_NAMES)),
            "inventory_status_invalid": tuple(INVENTORY_STATUSES),
        }
        for issue_key, severity, affected_table, sql, message in checks:
            params = issue_params_map.get(issue_key)
            if params:
                cursor.execute(sql, params)
                row = cursor.fetchone() or {}
                cnt = int(row.get("cnt") or 0)
                issue = None
                if cnt > 0:
                    issue = {
                        "issue_key": issue_key,
                        "severity": severity,
                        "affected_table": affected_table,
                        "issue_count": cnt,
                        "sample_value": str(row.get("sample_value") or "").strip() or None,
                        "message": message,
                    }
            else:
                issue = _collect_issue(cursor, issue_key, severity, affected_table, sql, message)
            if issue:
                issues.append(issue)

        for issue in issues:
            cursor.execute(
                """
                INSERT INTO `data_quality_issue` (`run_id`, `issue_key`, `severity`, `affected_table`, `issue_count`, `sample_value`, `message`)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    run_id,
                    issue["issue_key"],
                    issue["severity"],
                    issue["affected_table"],
                    issue["issue_count"],
                    issue["sample_value"],
                    issue["message"],
                ),
            )

        summary = {
            "checked_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "fixed_count": fixed_count,
            "issue_count": len(issues),
            "generic_district_policy": "统一按城市映射到真实区县列表，订单和门店不再保留占位区县。",
            "geo_code_policy": "用户、门店、订单收货地统一补全省市编码。",
            "product_catalog_policy": "产品主数据、订单明细快照、退款明细快照统一约束为蒙牛产品白名单，不允许出现“其他”或非蒙牛商品名。",
            "inventory_policy": "库存按门店×商品生成快照，统一维护在库、预占、可售、在途、安全库存、库存金额和库存状态口径。",
        }
        cursor.execute(
            """
            UPDATE `data_quality_run`
            SET `status`='succeeded', `fixed_count`=%s, `issue_count`=%s, `summary_json`=%s, `finished_at`=%s
            WHERE `id`=%s
            """,
            (
                fixed_count,
                len(issues),
                json.dumps(summary, ensure_ascii=False),
                datetime.now(),
                run_id,
            ),
        )
    conn.commit()
    return {
        "run_id": run_id,
        "fixed_count": fixed_count,
        "issue_count": len(issues),
        "issues": issues,
        "summary": summary,
    }
