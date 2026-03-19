from __future__ import annotations

from typing import Any


MENGNIU_PRODUCT_CATALOG: list[dict[str, Any]] = [
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


def allowed_product_names() -> set[str]:
    return {item["name"] for item in MENGNIU_PRODUCT_CATALOG}


def allowed_brand_names() -> set[str]:
    return {item["brand"] for item in MENGNIU_PRODUCT_CATALOG}


def catalog_rows_by_sku() -> dict[str, dict[str, Any]]:
    return {
        f"SKU{index:05d}": item
        for index, item in enumerate(MENGNIU_PRODUCT_CATALOG, start=1)
    }
