from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal


INVENTORY_STOCK_DDL = """
CREATE TABLE IF NOT EXISTS `inventory_stock` (
    `inventory_id` BIGINT NOT NULL COMMENT '库存快照ID，主键',
    `snapshot_date` DATE NOT NULL COMMENT '库存快照日期',
    `store_id` BIGINT NOT NULL COMMENT '门店ID，关联store_info.store_id',
    `product_id` BIGINT NOT NULL COMMENT '产品ID，关联product_info.product_id',
    `sales_channel` VARCHAR(32) NOT NULL COMMENT '销售渠道快照',
    `warehouse_code` VARCHAR(32) NOT NULL COMMENT '仓库编码',
    `warehouse_name` VARCHAR(128) NOT NULL COMMENT '仓库名称',
    `warehouse_type` VARCHAR(32) NOT NULL COMMENT '仓库类型',
    `on_hand_qty` INT NOT NULL COMMENT '账面在库量',
    `reserved_qty` INT NOT NULL COMMENT '预占库存量',
    `available_qty` INT NOT NULL COMMENT '可售库存量',
    `in_transit_qty` INT NOT NULL COMMENT '在途库存量',
    `safety_stock_qty` INT NOT NULL COMMENT '安全库存量',
    `damaged_qty` INT NOT NULL COMMENT '残损库存量',
    `inventory_amount` DECIMAL(12,2) NOT NULL COMMENT '库存金额，按成本口径',
    `days_of_supply` INT NOT NULL COMMENT '预计可售天数',
    `stock_status` VARCHAR(20) NOT NULL COMMENT '库存状态',
    `last_inbound_at` DATETIME NULL COMMENT '最近入库时间',
    `last_outbound_at` DATETIME NULL COMMENT '最近出库时间',
    PRIMARY KEY (`inventory_id`),
    UNIQUE KEY `uk_inventory_snapshot_store_product` (`snapshot_date`, `store_id`, `product_id`),
    KEY `idx_inventory_snapshot` (`snapshot_date`),
    KEY `idx_inventory_channel` (`sales_channel`),
    KEY `idx_inventory_status` (`stock_status`),
    KEY `idx_inventory_store` (`store_id`),
    KEY `idx_inventory_product` (`product_id`),
    CONSTRAINT `fk_inventory_store` FOREIGN KEY (`store_id`) REFERENCES `store_info` (`store_id`),
    CONSTRAINT `fk_inventory_product` FOREIGN KEY (`product_id`) REFERENCES `product_info` (`product_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='库存快照表';
"""

INVENTORY_STOCK_MIGRATIONS: dict[str, str] = {}
INVENTORY_STATUSES = ("正常", "预警", "缺货", "滞销")


def _table_exists(cursor, table_name: str) -> bool:
    cursor.execute("SHOW TABLES LIKE %s", (table_name,))
    return bool(cursor.fetchone())


def _channel_inventory_multiplier(channel_name: str, product_channel_type: str) -> float:
    if product_channel_type == "全渠道":
        return 1.0
    if product_channel_type == "电商优先":
        return 1.35 if channel_name in ("天猫", "京东", "抖音", "小程序") else 0.58
    if product_channel_type == "冷链渠道":
        return 1.28 if channel_name in ("线下门店", "O2O到家", "社区团购") else 0.72
    if product_channel_type == "母婴渠道":
        return 1.22 if channel_name in ("线下门店", "小程序") else 0.66
    return 1.0


def _warehouse_type(channel_name: str, store_type: str, temperature_zone: str) -> str:
    if temperature_zone == "冷冻":
        return "冷冻仓"
    if "前置仓" in store_type or channel_name in ("O2O到家", "社区团购"):
        return "前置仓"
    if channel_name in ("天猫", "京东", "抖音", "小程序"):
        return "电商仓"
    return "门店仓"


def ensure_inventory_runtime(conn) -> dict[str, int]:
    with conn.cursor() as cursor:
        if not _table_exists(cursor, "store_info") or not _table_exists(cursor, "product_info"):
            return {"rows": 0, "seeded": 0}

        cursor.execute(INVENTORY_STOCK_DDL)
        cursor.execute("SELECT COUNT(*) AS cnt FROM `inventory_stock`")
        existing_count = int((cursor.fetchone() or {}).get("cnt") or 0)
        if existing_count > 0:
            return {"rows": existing_count, "seeded": 0}

        cursor.execute(
            """
            SELECT `store_id`, `store_code`, `store_name`, `store_type`, `channel_name`, `channel_type`, `city`, `sales_region`, `store_status`
            FROM `store_info`
            ORDER BY `store_id`
            """
        )
        stores = cursor.fetchall()
        cursor.execute(
            """
            SELECT `product_id`, `sku_code`, `product_name`, `brand_name`, `category_l1`, `channel_type`, `cost_price`, `temperature_zone`
            FROM `product_info`
            ORDER BY `product_id`
            """
        )
        products = cursor.fetchall()

        snapshot_date = date.today()
        now = datetime.now()
        rows: list[tuple] = []
        for store in stores:
            for product in products:
                inventory_id = int(store["store_id"]) * 1000 + int(product["product_id"])
                multiplier = _channel_inventory_multiplier(
                    str(store["channel_name"] or ""),
                    str(product["channel_type"] or "全渠道"),
                )
                base_qty = int(((int(store["store_id"]) * 37 + int(product["product_id"]) * 13) % 160) + 20)
                if str(store["store_status"]) != "营业中":
                    base_qty = max(8, base_qty // 4)
                on_hand_qty = max(6, int(base_qty * multiplier))
                reserved_qty = min(on_hand_qty // 5, (int(store["store_id"]) + int(product["product_id"])) % 18)
                damaged_qty = (int(store["store_id"]) + int(product["product_id"])) % 4
                available_qty = max(on_hand_qty - reserved_qty - damaged_qty, 0)
                in_transit_qty = (
                    (int(store["store_id"]) * int(product["product_id"])) % 28
                    if available_qty < 90
                    else (int(store["store_id"]) + int(product["product_id"])) % 10
                )
                safety_stock_qty = max(12, int(on_hand_qty * 0.18))
                demand_index = ((int(store["store_id"]) * 5 + int(product["product_id"]) * 7) % 18) + 4
                days_of_supply = max(3, min(120, available_qty // demand_index))
                if available_qty == 0:
                    stock_status = "缺货"
                elif available_qty < safety_stock_qty:
                    stock_status = "预警"
                elif days_of_supply > 75:
                    stock_status = "滞销"
                else:
                    stock_status = "正常"
                warehouse_type = _warehouse_type(
                    str(store["channel_name"] or ""),
                    str(store["store_type"] or ""),
                    str(product["temperature_zone"] or ""),
                )
                warehouse_name = f"{store['city']}{warehouse_type}"
                inventory_amount = (Decimal(str(product["cost_price"])) * Decimal(available_qty)).quantize(Decimal("0.01"))
                inbound_offset = (int(store["store_id"]) + int(product["product_id"])) % 25
                outbound_offset = (int(store["store_id"]) * 2 + int(product["product_id"])) % 10
                last_inbound_at = now - timedelta(days=inbound_offset, hours=(int(product["product_id"]) * 3) % 23)
                last_outbound_at = now - timedelta(days=outbound_offset, hours=(int(store["store_id"]) + int(product["product_id"])) % 21)
                rows.append(
                    (
                        inventory_id,
                        snapshot_date,
                        store["store_id"],
                        product["product_id"],
                        store["channel_name"],
                        f"WH{int(store['store_id']):05d}",
                        warehouse_name,
                        warehouse_type,
                        on_hand_qty,
                        reserved_qty,
                        available_qty,
                        in_transit_qty,
                        safety_stock_qty,
                        damaged_qty,
                        inventory_amount,
                        days_of_supply,
                        stock_status,
                        last_inbound_at,
                        last_outbound_at,
                    )
                )

        cursor.executemany(
            """
            INSERT INTO `inventory_stock` (
                `inventory_id`, `snapshot_date`, `store_id`, `product_id`, `sales_channel`,
                `warehouse_code`, `warehouse_name`, `warehouse_type`, `on_hand_qty`, `reserved_qty`,
                `available_qty`, `in_transit_qty`, `safety_stock_qty`, `damaged_qty`,
                `inventory_amount`, `days_of_supply`, `stock_status`, `last_inbound_at`, `last_outbound_at`
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            rows,
        )
        return {"rows": len(rows), "seeded": len(rows)}
