import argparse
import os
import random
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP

import pymysql
from dotenv import load_dotenv

from semantic_layer import ensure_semantic_runtime


load_dotenv()
random.seed(20260313)


PROVINCE_CITY_REGION = {
    "北京": {"region": "华北大区", "cities": ["北京"]},
    "上海": {"region": "华东大区", "cities": ["上海"]},
    "江苏": {"region": "华东大区", "cities": ["南京", "苏州", "无锡", "徐州"]},
    "浙江": {"region": "华东大区", "cities": ["杭州", "宁波", "温州", "金华"]},
    "广东": {"region": "华南大区", "cities": ["广州", "深圳", "佛山", "东莞"]},
    "福建": {"region": "华南大区", "cities": ["福州", "厦门", "泉州"]},
    "河南": {"region": "华中大区", "cities": ["郑州", "洛阳", "南阳", "新乡"]},
    "湖北": {"region": "华中大区", "cities": ["武汉", "襄阳", "宜昌"]},
    "山东": {"region": "华北大区", "cities": ["济南", "青岛", "烟台", "临沂"]},
    "四川": {"region": "西南大区", "cities": ["成都", "绵阳", "德阳", "南充"]},
    "重庆": {"region": "西南大区", "cities": ["重庆"]},
    "陕西": {"region": "西北大区", "cities": ["西安", "咸阳", "宝鸡"]},
}
DISTRICTS = ["核心商圈", "成熟社区", "校园周边", "写字楼区", "居民城区"]
STORE_TYPES = ["直营门店", "经销商门店", "会员仓店", "社区前置仓", "电商旗舰店"]
STORE_STATUS = ["营业中", "筹备中", "暂停营业"]
CHANNELS = ["线下门店", "天猫", "京东", "抖音", "小程序", "社区团购", "O2O到家"]
CHANNEL_TYPE_MAP = {
    "线下门店": "线下零售",
    "天猫": "传统电商",
    "京东": "传统电商",
    "抖音": "兴趣电商",
    "小程序": "私域直营",
    "社区团购": "新零售",
    "O2O到家": "即时零售",
}
PAYMENT_METHODS = ["微信支付", "支付宝", "银行卡", "云闪付", "门店收银"]
ORDER_STATUS_WEIGHTS = [
    ("待支付", 4),
    ("已支付", 10),
    ("已发货", 12),
    ("已完成", 60),
    ("部分退款", 8),
    ("已退款", 3),
    ("已取消", 3),
]
MEMBER_LEVELS = ["新客", "普通会员", "银卡会员", "金卡会员", "黑金会员"]
MEMBER_LEVEL_WEIGHTS = [18, 42, 23, 12, 5]
GENDERS = ["男", "女", "未知"]
GENDER_WEIGHTS = [45, 47, 8]
REGISTER_CHANNELS = ["门店拉新", "电商投放", "社媒种草", "直播转化", "会员转介绍"]
CUSTOMER_TAGS = ["家庭囤货", "母婴优先", "品质白领", "价格敏感", "高频复购"]
OCCUPATIONS = ["上班族", "学生", "自由职业", "企业职员", "个体经营", "家庭主理人"]
PRODUCT_CATALOG = [
    {"brand": "特仑苏", "spu": "特仑苏纯牛奶", "name": "特仑苏纯牛奶 250ml*24盒", "cat1": "液态奶", "cat2": "常温纯牛奶", "capacity": "250ml*24", "package": "箱装", "price": 79.90, "cost": 54.00, "channel": "全渠道", "target": "家庭"},
    {"brand": "特仑苏", "spu": "特仑苏有机纯牛奶", "name": "特仑苏有机纯牛奶 250ml*12盒", "cat1": "液态奶", "cat2": "常温纯牛奶", "capacity": "250ml*12", "package": "箱装", "price": 56.90, "cost": 38.50, "channel": "全渠道", "target": "家庭"},
    {"brand": "特仑苏", "spu": "特仑苏低脂牛奶", "name": "特仑苏低脂牛奶 250ml*24盒", "cat1": "液态奶", "cat2": "功能牛奶", "capacity": "250ml*24", "package": "箱装", "price": 82.90, "cost": 57.00, "channel": "电商优先", "target": "白领"},
    {"brand": "蒙牛", "spu": "蒙牛纯牛奶", "name": "蒙牛纯牛奶 250ml*24盒", "cat1": "液态奶", "cat2": "常温纯牛奶", "capacity": "250ml*24", "package": "箱装", "price": 59.90, "cost": 42.00, "channel": "全渠道", "target": "家庭"},
    {"brand": "蒙牛", "spu": "蒙牛高钙牛奶", "name": "蒙牛高钙牛奶 250ml*16盒", "cat1": "液态奶", "cat2": "功能牛奶", "capacity": "250ml*16", "package": "箱装", "price": 45.90, "cost": 31.50, "channel": "全渠道", "target": "家庭"},
    {"brand": "蒙牛", "spu": "蒙牛早餐奶", "name": "蒙牛麦香早餐奶 250ml*16盒", "cat1": "液态奶", "cat2": "调制乳", "capacity": "250ml*16", "package": "箱装", "price": 39.90, "cost": 27.80, "channel": "全渠道", "target": "大众"},
    {"brand": "纯甄", "spu": "纯甄经典风味酸牛奶", "name": "纯甄经典风味酸牛奶 200g*12盒", "cat1": "低温酸奶", "cat2": "风味酸奶", "capacity": "200g*12", "package": "提装", "price": 36.90, "cost": 24.60, "channel": "全渠道", "target": "家庭"},
    {"brand": "纯甄", "spu": "纯甄香草风味酸牛奶", "name": "纯甄香草风味酸牛奶 200g*12盒", "cat1": "低温酸奶", "cat2": "风味酸奶", "capacity": "200g*12", "package": "提装", "price": 39.90, "cost": 26.80, "channel": "电商优先", "target": "白领"},
    {"brand": "纯甄", "spu": "纯甄果粒酸奶", "name": "纯甄黄桃燕麦风味酸牛奶 200g*10盒", "cat1": "低温酸奶", "cat2": "果粒酸奶", "capacity": "200g*10", "package": "提装", "price": 34.90, "cost": 23.40, "channel": "全渠道", "target": "白领"},
    {"brand": "真果粒", "spu": "真果粒草莓果粒酸牛奶", "name": "真果粒草莓果粒酸牛奶 230g*10盒", "cat1": "低温酸奶", "cat2": "果粒酸奶", "capacity": "230g*10", "package": "提装", "price": 32.90, "cost": 22.10, "channel": "全渠道", "target": "年轻人"},
    {"brand": "真果粒", "spu": "真果粒芦荟酸牛奶", "name": "真果粒芦荟椰果酸牛奶 230g*10盒", "cat1": "低温酸奶", "cat2": "果粒酸奶", "capacity": "230g*10", "package": "提装", "price": 31.90, "cost": 21.50, "channel": "全渠道", "target": "年轻人"},
    {"brand": "未来星", "spu": "未来星儿童成长牛奶", "name": "未来星儿童成长牛奶原味 190ml*12盒", "cat1": "液态奶", "cat2": "儿童牛奶", "capacity": "190ml*12", "package": "箱装", "price": 42.90, "cost": 29.60, "channel": "全渠道", "target": "母婴"},
    {"brand": "未来星", "spu": "未来星DHA成长牛奶", "name": "未来星DHA成长牛奶 190ml*12盒", "cat1": "液态奶", "cat2": "儿童牛奶", "capacity": "190ml*12", "package": "箱装", "price": 48.90, "cost": 33.80, "channel": "母婴渠道", "target": "母婴"},
    {"brand": "冠益乳", "spu": "冠益乳益生菌发酵乳", "name": "冠益乳原味益生菌发酵乳 250g*10瓶", "cat1": "低温酸奶", "cat2": "益生菌酸奶", "capacity": "250g*10", "package": "提装", "price": 29.90, "cost": 19.90, "channel": "全渠道", "target": "家庭"},
    {"brand": "冠益乳", "spu": "冠益乳轻食酸奶", "name": "冠益乳轻食高蛋白酸奶 200g*8盒", "cat1": "低温酸奶", "cat2": "高蛋白酸奶", "capacity": "200g*8", "package": "提装", "price": 27.90, "cost": 18.30, "channel": "电商优先", "target": "白领"},
    {"brand": "每日鲜语", "spu": "每日鲜语鲜牛奶", "name": "每日鲜语鲜牛奶 250ml*12瓶", "cat1": "鲜奶", "cat2": "巴氏鲜奶", "capacity": "250ml*12", "package": "冷链箱装", "price": 58.90, "cost": 39.60, "channel": "冷链渠道", "target": "家庭"},
    {"brand": "每日鲜语", "spu": "每日鲜语高蛋白牛奶", "name": "每日鲜语高蛋白牛奶 250ml*10瓶", "cat1": "鲜奶", "cat2": "功能鲜奶", "capacity": "250ml*10", "package": "冷链箱装", "price": 55.90, "cost": 37.20, "channel": "冷链渠道", "target": "白领"},
    {"brand": "酸酸乳", "spu": "酸酸乳原味", "name": "蒙牛酸酸乳原味 250ml*16盒", "cat1": "含乳饮料", "cat2": "乳酸菌饮品", "capacity": "250ml*16", "package": "箱装", "price": 29.90, "cost": 20.50, "channel": "全渠道", "target": "年轻人"},
    {"brand": "酸酸乳", "spu": "酸酸乳草莓味", "name": "蒙牛酸酸乳草莓味 250ml*16盒", "cat1": "含乳饮料", "cat2": "乳酸菌饮品", "capacity": "250ml*16", "package": "箱装", "price": 29.90, "cost": 20.50, "channel": "全渠道", "target": "年轻人"},
    {"brand": "蒂兰圣雪", "spu": "蒂兰圣雪经典香草冰淇淋", "name": "蒂兰圣雪经典香草冰淇淋 90g*6支", "cat1": "冰淇淋", "cat2": "家庭装冰淇淋", "capacity": "90g*6", "package": "冷链盒装", "price": 39.90, "cost": 26.40, "channel": "冷链渠道", "target": "家庭"},
    {"brand": "蒂兰圣雪", "spu": "蒂兰圣雪巧克力冰淇淋", "name": "蒂兰圣雪巧克力冰淇淋 90g*6支", "cat1": "冰淇淋", "cat2": "家庭装冰淇淋", "capacity": "90g*6", "package": "冷链盒装", "price": 41.90, "cost": 27.80, "channel": "冷链渠道", "target": "家庭"},
    {"brand": "特仑苏", "spu": "特仑苏沙漠有机纯牛奶", "name": "特仑苏沙漠有机纯牛奶 250ml*10盒", "cat1": "液态奶", "cat2": "高端纯牛奶", "capacity": "250ml*10", "package": "箱装", "price": 62.90, "cost": 42.50, "channel": "电商优先", "target": "品质家庭"},
    {"brand": "蒙牛", "spu": "蒙牛优益C乳酸菌", "name": "蒙牛优益C原味乳酸菌饮品 100ml*20瓶", "cat1": "低温饮品", "cat2": "乳酸菌饮品", "capacity": "100ml*20", "package": "提装", "price": 24.90, "cost": 16.80, "channel": "全渠道", "target": "家庭"},
    {"brand": "真果粒", "spu": "真果粒桃果粒酸牛奶", "name": "真果粒蜜桃果粒酸牛奶 230g*10盒", "cat1": "低温酸奶", "cat2": "果粒酸奶", "capacity": "230g*10", "package": "提装", "price": 32.90, "cost": 22.10, "channel": "全渠道", "target": "年轻人"},
]


def db_config(include_database: bool = True) -> dict:
    config = {
        "host": os.getenv("MYSQL_HOST", "127.0.0.1"),
        "port": int(os.getenv("MYSQL_PORT", "3306")),
        "user": os.getenv("MYSQL_USER", "root"),
        "password": os.getenv("MYSQL_PASSWORD", "toor"),
        "charset": "utf8mb4",
        "autocommit": False,
    }
    if include_database:
        config["database"] = os.getenv("MYSQL_DATABASE", "test")
    return config


def quantize(amount: float | Decimal) -> Decimal:
    return Decimal(str(amount)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def random_datetime(start: datetime, end: datetime) -> datetime:
    seconds = max(int((end - start).total_seconds()), 1)
    return start + timedelta(seconds=random.randint(0, seconds))


def random_name() -> str:
    last_names = ["王", "李", "张", "刘", "陈", "杨", "黄", "赵", "周", "吴"]
    first_names = ["嘉宁", "思远", "雨桐", "浩然", "子涵", "梓萱", "宇轩", "若彤", "佳琪", "晨曦"]
    return random.choice(last_names) + random.choice(first_names)


def create_database() -> None:
    database = os.getenv("MYSQL_DATABASE", "test")
    with pymysql.connect(**db_config(include_database=False)) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"CREATE DATABASE IF NOT EXISTS `{database}` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
            )
        conn.commit()


def recreate_tables() -> None:
    ddl_statements = [
        """
        CREATE TABLE `user_info` (
            `user_id` BIGINT NOT NULL COMMENT '用户ID，主键',
            `user_code` VARCHAR(32) NOT NULL COMMENT '用户编码',
            `user_name` VARCHAR(64) NOT NULL COMMENT '用户名称',
            `gender` VARCHAR(10) NOT NULL COMMENT '性别',
            `age` TINYINT NOT NULL COMMENT '年龄',
            `birthday` DATE NOT NULL COMMENT '生日',
            `mobile_city` VARCHAR(32) NOT NULL COMMENT '手机号归属城市',
            `province` VARCHAR(32) NOT NULL COMMENT '常住省份',
            `city` VARCHAR(32) NOT NULL COMMENT '常住城市',
            `city_tier` VARCHAR(10) NOT NULL COMMENT '城市等级',
            `register_channel` VARCHAR(32) NOT NULL COMMENT '注册渠道',
            `register_source` VARCHAR(32) NOT NULL COMMENT '注册来源',
            `register_at` DATETIME NOT NULL COMMENT '注册时间',
            `member_level` VARCHAR(20) NOT NULL COMMENT '会员等级',
            `loyalty_points` INT NOT NULL COMMENT '会员积分',
            `preferred_channel` VARCHAR(20) NOT NULL COMMENT '偏好购买渠道',
            `device_type` VARCHAR(20) NOT NULL COMMENT '常用设备类型',
            `occupation` VARCHAR(32) NOT NULL COMMENT '职业标签',
            `customer_tag` VARCHAR(32) NOT NULL COMMENT '用户标签',
            `is_mother` TINYINT(1) NOT NULL COMMENT '是否母婴人群',
            PRIMARY KEY (`user_id`),
            UNIQUE KEY `uk_user_code` (`user_code`),
            KEY `idx_user_province` (`province`),
            KEY `idx_user_city` (`city`),
            KEY `idx_user_register_at` (`register_at`),
            KEY `idx_user_member_level` (`member_level`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户信息维度表';
        """,
        """
        CREATE TABLE `store_info` (
            `store_id` BIGINT NOT NULL COMMENT '门店ID，主键',
            `store_code` VARCHAR(32) NOT NULL COMMENT '门店编码',
            `store_name` VARCHAR(128) NOT NULL COMMENT '门店名称',
            `store_type` VARCHAR(32) NOT NULL COMMENT '门店类型',
            `channel_name` VARCHAR(32) NOT NULL COMMENT '渠道名称',
            `channel_type` VARCHAR(32) NOT NULL COMMENT '渠道类型',
            `country` VARCHAR(32) NOT NULL COMMENT '所在国家',
            `province` VARCHAR(32) NOT NULL COMMENT '所在省份',
            `city` VARCHAR(32) NOT NULL COMMENT '所在城市',
            `district` VARCHAR(32) NOT NULL COMMENT '所在区域',
            `sales_region` VARCHAR(32) NOT NULL COMMENT '销售大区',
            `org_level_1` VARCHAR(32) NOT NULL COMMENT '一级组织',
            `org_level_2` VARCHAR(32) NOT NULL COMMENT '二级组织',
            `org_level_3` VARCHAR(32) NOT NULL COMMENT '三级组织',
            `manager_name` VARCHAR(32) NOT NULL COMMENT '门店负责人',
            `open_date` DATE NOT NULL COMMENT '开店日期',
            `store_status` VARCHAR(20) NOT NULL COMMENT '门店状态',
            PRIMARY KEY (`store_id`),
            UNIQUE KEY `uk_store_code` (`store_code`),
            KEY `idx_store_region` (`sales_region`),
            KEY `idx_store_province` (`province`),
            KEY `idx_store_channel` (`channel_name`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='门店信息维度表';
        """,
        """
        CREATE TABLE `product_info` (
            `product_id` BIGINT NOT NULL COMMENT '产品ID，主键',
            `sku_code` VARCHAR(32) NOT NULL COMMENT '产品SKU编码',
            `brand_name` VARCHAR(32) NOT NULL COMMENT '品牌名称',
            `spu_name` VARCHAR(128) NOT NULL COMMENT '产品SPU名称',
            `product_name` VARCHAR(128) NOT NULL COMMENT '产品名称',
            `category_l1` VARCHAR(32) NOT NULL COMMENT '一级品类',
            `category_l2` VARCHAR(32) NOT NULL COMMENT '二级品类',
            `capacity_desc` VARCHAR(32) NOT NULL COMMENT '规格描述',
            `package_type` VARCHAR(32) NOT NULL COMMENT '包装类型',
            `channel_type` VARCHAR(32) NOT NULL COMMENT '适销渠道类型',
            `target_group` VARCHAR(32) NOT NULL COMMENT '目标人群',
            `list_price` DECIMAL(12,2) NOT NULL COMMENT '建议零售价',
            `cost_price` DECIMAL(12,2) NOT NULL COMMENT '成本单价',
            `launch_date` DATE NOT NULL COMMENT '上市日期',
            `temperature_zone` VARCHAR(20) NOT NULL COMMENT '温层类型',
            `product_status` VARCHAR(20) NOT NULL COMMENT '产品状态',
            PRIMARY KEY (`product_id`),
            UNIQUE KEY `uk_sku_code` (`sku_code`),
            KEY `idx_product_brand` (`brand_name`),
            KEY `idx_product_category` (`category_l1`, `category_l2`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='产品信息维度表';
        """,
        """
        CREATE TABLE `order_master` (
            `order_id` BIGINT NOT NULL COMMENT '订单ID，主键',
            `order_no` VARCHAR(40) NOT NULL COMMENT '订单编号',
            `buyer_id` BIGINT NOT NULL COMMENT '用户ID，关联user_info.user_id',
            `store_id` BIGINT NOT NULL COMMENT '门店ID，关联store_info.store_id',
            `platform` VARCHAR(32) NOT NULL COMMENT '订单来源平台',
            `sales_channel` VARCHAR(32) NOT NULL COMMENT '销售渠道',
            `channel_type` VARCHAR(32) NOT NULL COMMENT '渠道类型',
            `order_source` VARCHAR(32) NOT NULL COMMENT '订单来源',
            `order_status` VARCHAR(20) NOT NULL COMMENT '订单状态',
            `payment_status` VARCHAR(20) NOT NULL COMMENT '支付状态',
            `fulfillment_status` VARCHAR(20) NOT NULL COMMENT '履约状态',
            `payment_method` VARCHAR(20) NOT NULL COMMENT '支付方式',
            `currency_code` VARCHAR(10) NOT NULL COMMENT '币种编码',
            `item_count` INT NOT NULL COMMENT '商品件数',
            `gross_amount` DECIMAL(12,2) NOT NULL COMMENT '商品原价金额',
            `discount_amount` DECIMAL(12,2) NOT NULL COMMENT '优惠金额',
            `freight_amount` DECIMAL(12,2) NOT NULL COMMENT '运费金额',
            `paid_amount` DECIMAL(12,2) NOT NULL COMMENT '订单实付金额',
            `refund_amount` DECIMAL(12,2) NOT NULL COMMENT '订单累计退款金额',
            `receiver_name` VARCHAR(32) NOT NULL COMMENT '收货人姓名',
            `receiver_province` VARCHAR(32) NOT NULL COMMENT '收货省份',
            `receiver_city` VARCHAR(32) NOT NULL COMMENT '收货城市',
            `receiver_district` VARCHAR(32) NOT NULL COMMENT '收货区域',
            `created_at` DATETIME NOT NULL COMMENT '下单时间',
            `paid_at` DATETIME NULL COMMENT '支付时间',
            `shipped_at` DATETIME NULL COMMENT '发货时间',
            `delivered_at` DATETIME NULL COMMENT '签收时间',
            `completed_at` DATETIME NULL COMMENT '完成时间',
            `updated_at` DATETIME NOT NULL COMMENT '更新时间',
            PRIMARY KEY (`order_id`),
            UNIQUE KEY `uk_order_no` (`order_no`),
            KEY `idx_order_created_at` (`created_at`),
            KEY `idx_order_status` (`order_status`),
            KEY `idx_order_buyer` (`buyer_id`),
            KEY `idx_order_store` (`store_id`),
            KEY `idx_order_channel` (`sales_channel`),
            KEY `idx_order_receiver_province` (`receiver_province`),
            CONSTRAINT `fk_order_master_user` FOREIGN KEY (`buyer_id`) REFERENCES `user_info` (`user_id`),
            CONSTRAINT `fk_order_master_store` FOREIGN KEY (`store_id`) REFERENCES `store_info` (`store_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='订单主表';
        """,
        """
        CREATE TABLE `order_detail` (
            `order_detail_id` BIGINT NOT NULL COMMENT '订单明细ID，主键',
            `order_id` BIGINT NOT NULL COMMENT '订单ID，关联order_master.order_id',
            `line_no` INT NOT NULL COMMENT '订单行号',
            `product_id` BIGINT NOT NULL COMMENT '产品ID，关联product_info.product_id',
            `product_name` VARCHAR(128) NOT NULL COMMENT '下单时产品名称快照',
            `brand_name` VARCHAR(32) NOT NULL COMMENT '下单时品牌快照',
            `category_l1` VARCHAR(32) NOT NULL COMMENT '一级品类快照',
            `category_l2` VARCHAR(32) NOT NULL COMMENT '二级品类快照',
            `sales_channel` VARCHAR(32) NOT NULL COMMENT '下单渠道快照',
            `list_price` DECIMAL(12,2) NOT NULL COMMENT '建议零售价快照',
            `sale_unit_price` DECIMAL(12,2) NOT NULL COMMENT '实际成交单价',
            `quantity` INT NOT NULL COMMENT '购买数量',
            `line_gross_amount` DECIMAL(12,2) NOT NULL COMMENT '行原价金额',
            `line_discount_amount` DECIMAL(12,2) NOT NULL COMMENT '行优惠金额',
            `line_paid_amount` DECIMAL(12,2) NOT NULL COMMENT '行实付金额',
            PRIMARY KEY (`order_detail_id`),
            KEY `idx_order_detail_order` (`order_id`),
            KEY `idx_order_detail_product` (`product_id`),
            KEY `idx_order_detail_brand` (`brand_name`),
            CONSTRAINT `fk_order_detail_order` FOREIGN KEY (`order_id`) REFERENCES `order_master` (`order_id`),
            CONSTRAINT `fk_order_detail_product` FOREIGN KEY (`product_id`) REFERENCES `product_info` (`product_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='订单明细子表';
        """,
        """
        CREATE TABLE `refund_master` (
            `refund_id` BIGINT NOT NULL COMMENT '退款单ID，主键',
            `refund_no` VARCHAR(40) NOT NULL COMMENT '退款单编号',
            `order_id` BIGINT NOT NULL COMMENT '订单ID，关联order_master.order_id',
            `buyer_id` BIGINT NOT NULL COMMENT '用户ID，关联user_info.user_id',
            `store_id` BIGINT NOT NULL COMMENT '门店ID，关联store_info.store_id',
            `refund_status` VARCHAR(20) NOT NULL COMMENT '退款状态',
            `refund_type` VARCHAR(20) NOT NULL COMMENT '退款类型',
            `refund_reason` VARCHAR(50) NOT NULL COMMENT '退款原因',
            `refund_item_count` INT NOT NULL COMMENT '退款件数',
            `refund_amount` DECIMAL(12,2) NOT NULL COMMENT '退款金额',
            `applied_at` DATETIME NOT NULL COMMENT '申请时间',
            `approved_at` DATETIME NULL COMMENT '审核时间',
            `completed_at` DATETIME NULL COMMENT '完成时间',
            PRIMARY KEY (`refund_id`),
            UNIQUE KEY `uk_refund_no` (`refund_no`),
            KEY `idx_refund_order` (`order_id`),
            KEY `idx_refund_applied` (`applied_at`),
            KEY `idx_refund_status` (`refund_status`),
            CONSTRAINT `fk_refund_master_order` FOREIGN KEY (`order_id`) REFERENCES `order_master` (`order_id`),
            CONSTRAINT `fk_refund_master_user` FOREIGN KEY (`buyer_id`) REFERENCES `user_info` (`user_id`),
            CONSTRAINT `fk_refund_master_store` FOREIGN KEY (`store_id`) REFERENCES `store_info` (`store_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='退款主表';
        """,
        """
        CREATE TABLE `refund_detail` (
            `refund_detail_id` BIGINT NOT NULL COMMENT '退款明细ID，主键',
            `refund_id` BIGINT NOT NULL COMMENT '退款单ID，关联refund_master.refund_id',
            `order_detail_id` BIGINT NOT NULL COMMENT '订单明细ID，关联order_detail.order_detail_id',
            `product_id` BIGINT NOT NULL COMMENT '产品ID，关联product_info.product_id',
            `product_name` VARCHAR(128) NOT NULL COMMENT '退款产品名称快照',
            `refund_quantity` INT NOT NULL COMMENT '退款数量',
            `refund_unit_amount` DECIMAL(12,2) NOT NULL COMMENT '退款单价',
            `refund_amount` DECIMAL(12,2) NOT NULL COMMENT '退款金额',
            `refund_reason` VARCHAR(50) NOT NULL COMMENT '退款原因',
            PRIMARY KEY (`refund_detail_id`),
            KEY `idx_refund_detail_refund` (`refund_id`),
            KEY `idx_refund_detail_order_detail` (`order_detail_id`),
            KEY `idx_refund_detail_product` (`product_id`),
            CONSTRAINT `fk_refund_detail_refund` FOREIGN KEY (`refund_id`) REFERENCES `refund_master` (`refund_id`),
            CONSTRAINT `fk_refund_detail_order_detail` FOREIGN KEY (`order_detail_id`) REFERENCES `order_detail` (`order_detail_id`),
            CONSTRAINT `fk_refund_detail_product` FOREIGN KEY (`product_id`) REFERENCES `product_info` (`product_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='退款明细子表';
        """,
    ]

    drop_order = [
        "SET FOREIGN_KEY_CHECKS = 0",
        "DROP TABLE IF EXISTS `refund_detail`",
        "DROP TABLE IF EXISTS `refund_master`",
        "DROP TABLE IF EXISTS `order_detail`",
        "DROP TABLE IF EXISTS `order_master`",
        "DROP TABLE IF EXISTS `product_info`",
        "DROP TABLE IF EXISTS `store_info`",
        "DROP TABLE IF EXISTS `user_info`",
        "DROP TABLE IF EXISTS `order`",
        "SET FOREIGN_KEY_CHECKS = 1",
    ]

    with pymysql.connect(**db_config()) as conn:
        with conn.cursor() as cursor:
            for statement in drop_order:
                cursor.execute(statement)
            for statement in ddl_statements:
                cursor.execute(statement)
        conn.commit()


def build_stores() -> list[dict]:
    stores: list[dict] = []
    store_id = 1
    for province, meta in PROVINCE_CITY_REGION.items():
        for city in meta["cities"]:
            for store_type in STORE_TYPES:
                channel_name = "线下门店" if store_type != "电商旗舰店" else random.choice(["天猫", "京东", "抖音", "小程序"])
                if store_type == "社区前置仓":
                    channel_name = random.choice(["社区团购", "O2O到家"])
                channel_type = CHANNEL_TYPE_MAP[channel_name]
                store_name = f"蒙牛{city}{store_type}{store_id:03d}店"
                stores.append(
                    {
                        "store_id": store_id,
                        "store_code": f"ST{store_id:05d}",
                        "store_name": store_name,
                        "store_type": store_type,
                        "channel_name": channel_name,
                        "channel_type": channel_type,
                        "country": "中国",
                        "province": province,
                        "city": city,
                        "district": random.choice(DISTRICTS),
                        "sales_region": meta["region"],
                        "org_level_1": "蒙牛中国销售中心",
                        "org_level_2": meta["region"],
                        "org_level_3": f"{province}省区",
                        "manager_name": random_name(),
                        "open_date": date(2020, 1, 1) + timedelta(days=random.randint(0, 1800)),
                        "store_status": random.choices(STORE_STATUS, weights=[88, 4, 8], k=1)[0],
                    }
                )
                store_id += 1
    return stores


def build_products() -> list[dict]:
    products: list[dict] = []
    for index, item in enumerate(PRODUCT_CATALOG, start=1):
        list_price = quantize(item["price"])
        cost_price = quantize(item["cost"])
        products.append(
            {
                "product_id": index,
                "sku_code": f"SKU{index:05d}",
                "brand_name": item["brand"],
                "spu_name": item["spu"],
                "product_name": item["name"],
                "category_l1": item["cat1"],
                "category_l2": item["cat2"],
                "capacity_desc": item["capacity"],
                "package_type": item["package"],
                "channel_type": item["channel"],
                "target_group": item["target"],
                "list_price": list_price,
                "cost_price": cost_price,
                "launch_date": date(2021, 1, 1) + timedelta(days=random.randint(0, 1400)),
                "temperature_zone": "冷链" if "冷" in item["package"] or item["cat1"] in {"鲜奶", "冰淇淋"} else "常温",
                "product_status": "在售",
            }
        )
    return products


def build_users(user_count: int) -> list[dict]:
    users: list[dict] = []
    register_start = datetime.now() - timedelta(days=900)
    register_end = datetime.now() - timedelta(days=3)
    provinces = list(PROVINCE_CITY_REGION.keys())
    city_tiers = ["T1", "T2", "T3"]
    sources = ["公众号", "直播间", "电商广告", "门店活动", "社群裂变"]
    for user_id in range(1, user_count + 1):
        province = random.choice(provinces)
        city = random.choice(PROVINCE_CITY_REGION[province]["cities"])
        age = random.randint(18, 58)
        birthday = date.today() - timedelta(days=age * 365 + random.randint(0, 364))
        register_at = random_datetime(register_start, register_end)
        preferred_channel = random.choice(CHANNELS)
        users.append(
            {
                "user_id": user_id,
                "user_code": f"U{user_id:07d}",
                "user_name": random_name(),
                "gender": random.choices(GENDERS, weights=GENDER_WEIGHTS, k=1)[0],
                "age": age,
                "birthday": birthday,
                "mobile_city": city,
                "province": province,
                "city": city,
                "city_tier": random.choices(city_tiers, weights=[30, 45, 25], k=1)[0],
                "register_channel": random.choice(REGISTER_CHANNELS),
                "register_source": random.choice(sources),
                "register_at": register_at,
                "member_level": random.choices(MEMBER_LEVELS, weights=MEMBER_LEVEL_WEIGHTS, k=1)[0],
                "loyalty_points": random.randint(0, 12000),
                "preferred_channel": preferred_channel,
                "device_type": random.choice(["iOS", "Android", "Web", "MiniProgram"]),
                "occupation": random.choice(OCCUPATIONS),
                "customer_tag": random.choice(CUSTOMER_TAGS),
                "is_mother": 1 if random.random() < 0.22 else 0,
            }
        )
    return users


def choose_status() -> str:
    population = [item[0] for item in ORDER_STATUS_WEIGHTS]
    weights = [item[1] for item in ORDER_STATUS_WEIGHTS]
    return random.choices(population, weights=weights, k=1)[0]


def build_fact_batches(order_count: int, batch_size: int, users: list[dict], stores: list[dict], products: list[dict]) -> dict[str, int]:
    user_by_id = {user["user_id"]: user for user in users}
    stores_by_channel: dict[str, list[dict]] = {}
    for store in stores:
        stores_by_channel.setdefault(store["channel_name"], []).append(store)

    product_pool = products
    now = datetime.now()
    order_start = now - timedelta(days=365)
    order_end = now
    refund_reasons = ["临期担忧", "包装破损", "口味不符", "配送超时", "重复下单", "活动价差"]

    order_master_sql = """
    INSERT INTO `order_master` (
        `order_id`, `order_no`, `buyer_id`, `store_id`, `platform`, `sales_channel`, `channel_type`, `order_source`,
        `order_status`, `payment_status`, `fulfillment_status`, `payment_method`, `currency_code`, `item_count`,
        `gross_amount`, `discount_amount`, `freight_amount`, `paid_amount`, `refund_amount`, `receiver_name`,
        `receiver_province`, `receiver_city`, `receiver_district`, `created_at`, `paid_at`, `shipped_at`, `delivered_at`, `completed_at`, `updated_at`
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    """
    order_detail_sql = """
    INSERT INTO `order_detail` (
        `order_detail_id`, `order_id`, `line_no`, `product_id`, `product_name`, `brand_name`, `category_l1`, `category_l2`,
        `sales_channel`, `list_price`, `sale_unit_price`, `quantity`, `line_gross_amount`, `line_discount_amount`, `line_paid_amount`
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s
    )
    """
    refund_master_sql = """
    INSERT INTO `refund_master` (
        `refund_id`, `refund_no`, `order_id`, `buyer_id`, `store_id`, `refund_status`, `refund_type`, `refund_reason`,
        `refund_item_count`, `refund_amount`, `applied_at`, `approved_at`, `completed_at`
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    refund_detail_sql = """
    INSERT INTO `refund_detail` (
        `refund_detail_id`, `refund_id`, `order_detail_id`, `product_id`, `product_name`, `refund_quantity`, `refund_unit_amount`, `refund_amount`, `refund_reason`
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    order_master_rows: list[tuple] = []
    order_detail_rows: list[tuple] = []
    refund_master_rows: list[tuple] = []
    refund_detail_rows: list[tuple] = []
    detail_id = 1
    refund_id = 1
    refund_detail_id = 1

    totals = {"orders": 0, "details": 0, "refunds": 0, "refund_details": 0}

    with pymysql.connect(**db_config()) as conn:
        with conn.cursor() as cursor:
            for order_id in range(1, order_count + 1):
                user = user_by_id[random.randint(1, len(users))]
                sales_channel = random.choices(CHANNELS, weights=[24, 18, 16, 15, 10, 8, 9], k=1)[0]
                candidate_stores = stores_by_channel.get(sales_channel) or stores
                store = random.choice(candidate_stores)
                created_at_start = max(order_start, user["register_at"] + timedelta(hours=1))
                created_at = random_datetime(created_at_start, order_end)
                status = choose_status()
                payment_status = "未支付"
                fulfillment_status = "待履约"
                paid_at = None
                shipped_at = None
                delivered_at = None
                completed_at = None
                updated_at = created_at

                if status != "待支付" and status != "已取消":
                    payment_status = "已支付"
                    paid_at = created_at + timedelta(minutes=random.randint(1, 120))
                    updated_at = paid_at
                if status in {"已发货", "已完成", "部分退款", "已退款"} and paid_at:
                    fulfillment_status = "已发货"
                    shipped_at = paid_at + timedelta(hours=random.randint(4, 72))
                    updated_at = shipped_at
                if status in {"已完成", "部分退款", "已退款"} and shipped_at:
                    fulfillment_status = "已签收"
                    delivered_at = shipped_at + timedelta(days=random.randint(1, 6))
                    completed_at = delivered_at + timedelta(days=random.randint(0, 5))
                    updated_at = completed_at
                if status == "已取消":
                    fulfillment_status = "已关闭"
                if status in {"部分退款", "已退款"}:
                    fulfillment_status = "售后中"

                line_count = random.choices([1, 2, 3, 4, 5], weights=[36, 30, 18, 10, 6], k=1)[0]
                selected_products = random.sample(product_pool, k=line_count)
                item_count = 0
                gross_amount = Decimal("0.00")
                discount_amount = Decimal("0.00")
                detail_rows_for_order: list[dict] = []

                for line_no, product in enumerate(selected_products, start=1):
                    quantity = random.choices([1, 2, 3, 4], weights=[58, 26, 11, 5], k=1)[0]
                    discount_ratio = Decimal(str(random.uniform(0.82, 0.98))).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                    sale_unit_price = quantize(product["list_price"] * discount_ratio)
                    line_gross_amount = quantize(product["list_price"] * quantity)
                    line_paid_amount = quantize(sale_unit_price * quantity)
                    line_discount_amount = quantize(line_gross_amount - line_paid_amount)
                    item_count += quantity
                    gross_amount += line_gross_amount
                    discount_amount += line_discount_amount

                    detail_rows_for_order.append(
                        {
                            "order_detail_id": detail_id,
                            "product_id": product["product_id"],
                            "product_name": product["product_name"],
                            "brand_name": product["brand_name"],
                            "category_l1": product["category_l1"],
                            "category_l2": product["category_l2"],
                            "sales_channel": sales_channel,
                            "list_price": product["list_price"],
                            "sale_unit_price": sale_unit_price,
                            "quantity": quantity,
                            "line_gross_amount": line_gross_amount,
                            "line_discount_amount": line_discount_amount,
                            "line_paid_amount": line_paid_amount,
                        }
                    )
                    detail_id += 1

                freight_amount = quantize(0 if sales_channel == "线下门店" else random.choice([0, 4, 6, 8, 10]))
                paid_amount = quantize(gross_amount - discount_amount + freight_amount)
                refund_amount = Decimal("0.00")
                receiver_name = user["user_name"]
                receiver_province = user["province"]
                receiver_city = user["city"]
                receiver_district = random.choice(DISTRICTS)

                if status in {"待支付", "已取消"}:
                    paid_amount = Decimal("0.00") if status == "待支付" else paid_amount
                    if status == "已取消":
                        payment_status = "已关闭" if not paid_at else "已退款"
                        if paid_at:
                            refund_amount = paid_amount
                order_no = f"OM{created_at:%Y%m%d}{order_id:07d}"
                order_source = random.choice(["会员商城", "门店POS", "直播间", "搜索推荐", "活动会场"])
                currency_code = "CNY"
                platform = sales_channel

                if status in {"部分退款", "已退款"}:
                    refundable_lines = detail_rows_for_order[:]
                    random.shuffle(refundable_lines)
                    take_count = 1 if status == "已退款" and len(refundable_lines) == 1 else random.randint(1, max(1, len(refundable_lines) - (0 if status == "已退款" else 1)))
                    chosen_lines = refundable_lines[:take_count]
                    refund_reason = random.choice(refund_reasons)
                    refund_rows_count = 0
                    refund_amount_total = Decimal("0.00")
                    for line in chosen_lines:
                        max_qty = line["quantity"]
                        refund_qty = max_qty if status == "已退款" else random.randint(1, max_qty)
                        refund_unit_amount = line["sale_unit_price"]
                        line_refund_amount = quantize(refund_unit_amount * refund_qty)
                        refund_amount_total += line_refund_amount
                        refund_rows_count += refund_qty
                        refund_detail_rows.append(
                            (
                                refund_detail_id,
                                refund_id,
                                line["order_detail_id"],
                                line["product_id"],
                                line["product_name"],
                                refund_qty,
                                refund_unit_amount,
                                line_refund_amount,
                                refund_reason,
                            )
                        )
                        refund_detail_id += 1
                    refund_amount = refund_amount_total
                    if status == "已退款":
                        refund_amount = min(refund_amount_total, paid_amount)
                    applied_at = (completed_at or shipped_at or paid_at or created_at) + timedelta(days=random.randint(1, 10))
                    approved_at = applied_at + timedelta(hours=random.randint(2, 36))
                    refund_completed_at = approved_at + timedelta(days=random.randint(1, 7))
                    updated_at = max(updated_at, refund_completed_at)
                    refund_master_rows.append(
                        (
                            refund_id,
                            f"RF{applied_at:%Y%m%d}{refund_id:07d}",
                            order_id,
                            user["user_id"],
                            store["store_id"],
                            "已完成",
                            "整单退款" if status == "已退款" else "部分退款",
                            refund_reason,
                            refund_rows_count,
                            refund_amount,
                            applied_at,
                            approved_at,
                            refund_completed_at,
                        )
                    )
                    refund_id += 1

                order_master_rows.append(
                    (
                        order_id,
                        order_no,
                        user["user_id"],
                        store["store_id"],
                        platform,
                        sales_channel,
                        store["channel_type"],
                        order_source,
                        status,
                        payment_status,
                        fulfillment_status,
                        random.choice(PAYMENT_METHODS),
                        currency_code,
                        item_count,
                        gross_amount,
                        discount_amount,
                        freight_amount,
                        paid_amount,
                        refund_amount,
                        receiver_name,
                        receiver_province,
                        receiver_city,
                        receiver_district,
                        created_at,
                        paid_at,
                        shipped_at,
                        delivered_at,
                        completed_at,
                        updated_at,
                    )
                )
                for line_no, detail in enumerate(detail_rows_for_order, start=1):
                    order_detail_rows.append(
                        (
                            detail["order_detail_id"],
                            order_id,
                            line_no,
                            detail["product_id"],
                            detail["product_name"],
                            detail["brand_name"],
                            detail["category_l1"],
                            detail["category_l2"],
                            detail["sales_channel"],
                            detail["list_price"],
                            detail["sale_unit_price"],
                            detail["quantity"],
                            detail["line_gross_amount"],
                            detail["line_discount_amount"],
                            detail["line_paid_amount"],
                        )
                    )

                should_flush_orders = len(order_master_rows) >= batch_size or len(order_detail_rows) >= batch_size * 3
                should_flush_refunds = len(refund_master_rows) >= batch_size or len(refund_detail_rows) >= batch_size * 2

                if (should_flush_orders or should_flush_refunds) and order_master_rows:
                    cursor.executemany(order_master_sql, order_master_rows)
                    totals["orders"] += len(order_master_rows)
                    order_master_rows.clear()
                if (len(order_detail_rows) >= batch_size * 3 or should_flush_refunds) and order_detail_rows:
                    cursor.executemany(order_detail_sql, order_detail_rows)
                    totals["details"] += len(order_detail_rows)
                    order_detail_rows.clear()
                if should_flush_refunds and refund_master_rows:
                    cursor.executemany(refund_master_sql, refund_master_rows)
                    totals["refunds"] += len(refund_master_rows)
                    refund_master_rows.clear()
                if should_flush_refunds and refund_detail_rows:
                    cursor.executemany(refund_detail_sql, refund_detail_rows)
                    totals["refund_details"] += len(refund_detail_rows)
                    refund_detail_rows.clear()

            if order_master_rows:
                cursor.executemany(order_master_sql, order_master_rows)
                totals["orders"] += len(order_master_rows)
            if order_detail_rows:
                cursor.executemany(order_detail_sql, order_detail_rows)
                totals["details"] += len(order_detail_rows)
            if refund_master_rows:
                cursor.executemany(refund_master_sql, refund_master_rows)
                totals["refunds"] += len(refund_master_rows)
            if refund_detail_rows:
                cursor.executemany(refund_detail_sql, refund_detail_rows)
                totals["refund_details"] += len(refund_detail_rows)
        conn.commit()

    return totals


def seed_dimensions(user_count: int) -> dict[str, int]:
    users = build_users(user_count)
    stores = build_stores()
    products = build_products()

    user_rows = [
        (
            user["user_id"], user["user_code"], user["user_name"], user["gender"], user["age"], user["birthday"],
            user["mobile_city"], user["province"], user["city"], user["city_tier"], user["register_channel"],
            user["register_source"], user["register_at"], user["member_level"], user["loyalty_points"], user["preferred_channel"],
            user["device_type"], user["occupation"], user["customer_tag"], user["is_mother"]
        )
        for user in users
    ]
    store_rows = [
        (
            store["store_id"], store["store_code"], store["store_name"], store["store_type"], store["channel_name"],
            store["channel_type"], store["country"], store["province"], store["city"], store["district"], store["sales_region"],
            store["org_level_1"], store["org_level_2"], store["org_level_3"], store["manager_name"], store["open_date"], store["store_status"]
        )
        for store in stores
    ]
    product_rows = [
        (
            product["product_id"], product["sku_code"], product["brand_name"], product["spu_name"], product["product_name"],
            product["category_l1"], product["category_l2"], product["capacity_desc"], product["package_type"], product["channel_type"],
            product["target_group"], product["list_price"], product["cost_price"], product["launch_date"], product["temperature_zone"], product["product_status"]
        )
        for product in products
    ]

    with pymysql.connect(**db_config()) as conn:
        with conn.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO `user_info` (
                    `user_id`, `user_code`, `user_name`, `gender`, `age`, `birthday`, `mobile_city`, `province`, `city`,
                    `city_tier`, `register_channel`, `register_source`, `register_at`, `member_level`, `loyalty_points`,
                    `preferred_channel`, `device_type`, `occupation`, `customer_tag`, `is_mother`
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                user_rows,
            )
            cursor.executemany(
                """
                INSERT INTO `store_info` (
                    `store_id`, `store_code`, `store_name`, `store_type`, `channel_name`, `channel_type`, `country`, `province`, `city`,
                    `district`, `sales_region`, `org_level_1`, `org_level_2`, `org_level_3`, `manager_name`, `open_date`, `store_status`
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                store_rows,
            )
            cursor.executemany(
                """
                INSERT INTO `product_info` (
                    `product_id`, `sku_code`, `brand_name`, `spu_name`, `product_name`, `category_l1`, `category_l2`, `capacity_desc`,
                    `package_type`, `channel_type`, `target_group`, `list_price`, `cost_price`, `launch_date`, `temperature_zone`, `product_status`
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                product_rows,
            )
        conn.commit()

    return {
        "users": len(user_rows),
        "stores": len(store_rows),
        "products": len(product_rows),
        "user_objects": users,
        "store_objects": stores,
        "product_objects": products,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Initialize enterprise ChatBI demo schema and seed data")
    parser.add_argument("--rows", type=int, default=120000, help="Number of order_master rows to generate")
    parser.add_argument("--user-rows", type=int, default=40000, help="Number of user_info rows to generate")
    parser.add_argument("--batch-size", type=int, default=2000, help="Insert batch size")
    args = parser.parse_args()

    create_database()
    recreate_tables()
    dimension_result = seed_dimensions(args.user_rows)
    fact_result = build_fact_batches(
        order_count=args.rows,
        batch_size=args.batch_size,
        users=dimension_result.pop("user_objects"),
        stores=dimension_result.pop("store_objects"),
        products=dimension_result.pop("product_objects"),
    )
    ensure_semantic_runtime(refresh_embeddings=False)
    database = os.getenv("MYSQL_DATABASE", "test")
    print(
        "Done. Seeded "
        f"{database}.user_info={dimension_result['users']}, "
        f"{database}.store_info={dimension_result['stores']}, "
        f"{database}.product_info={dimension_result['products']}, "
        f"{database}.order_master={fact_result['orders']}, "
        f"{database}.order_detail={fact_result['details']}, "
        f"{database}.refund_master={fact_result['refunds']}, "
        f"{database}.refund_detail={fact_result['refund_details']}"
    )


if __name__ == "__main__":
    main()
