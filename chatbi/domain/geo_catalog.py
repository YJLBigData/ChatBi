from __future__ import annotations

from typing import Any


GEO_CATALOG: dict[str, dict[str, Any]] = {
    "北京": {
        "province_code": "110000",
        "region": "华北大区",
        "cities": {
            "北京": {
                "city_code": "110100",
                "city_tier": "一线",
                "districts": ["朝阳区", "海淀区", "丰台区", "通州区"],
            }
        },
    },
    "上海": {
        "province_code": "310000",
        "region": "华东大区",
        "cities": {
            "上海": {
                "city_code": "310100",
                "city_tier": "一线",
                "districts": ["浦东新区", "闵行区", "徐汇区", "宝山区"],
            }
        },
    },
    "江苏": {
        "province_code": "320000",
        "region": "华东大区",
        "cities": {
            "南京": {
                "city_code": "320100",
                "city_tier": "新一线",
                "districts": ["江宁区", "鼓楼区", "建邺区", "栖霞区"],
            },
            "苏州": {
                "city_code": "320500",
                "city_tier": "新一线",
                "districts": ["工业园区", "姑苏区", "吴中区", "相城区"],
            },
            "无锡": {
                "city_code": "320200",
                "city_tier": "二线",
                "districts": ["滨湖区", "梁溪区", "新吴区", "锡山区"],
            },
            "徐州": {
                "city_code": "320300",
                "city_tier": "二线",
                "districts": ["云龙区", "鼓楼区", "泉山区", "铜山区"],
            },
        },
    },
    "浙江": {
        "province_code": "330000",
        "region": "华东大区",
        "cities": {
            "杭州": {
                "city_code": "330100",
                "city_tier": "新一线",
                "districts": ["西湖区", "滨江区", "余杭区", "萧山区"],
            },
            "宁波": {
                "city_code": "330200",
                "city_tier": "二线",
                "districts": ["鄞州区", "海曙区", "江北区", "镇海区"],
            },
            "温州": {
                "city_code": "330300",
                "city_tier": "二线",
                "districts": ["鹿城区", "龙湾区", "瓯海区", "洞头区"],
            },
            "金华": {
                "city_code": "330700",
                "city_tier": "三线",
                "districts": ["婺城区", "金东区", "义乌市", "东阳市"],
            },
        },
    },
    "广东": {
        "province_code": "440000",
        "region": "华南大区",
        "cities": {
            "广州": {
                "city_code": "440100",
                "city_tier": "一线",
                "districts": ["天河区", "海珠区", "白云区", "番禺区"],
            },
            "深圳": {
                "city_code": "440300",
                "city_tier": "一线",
                "districts": ["南山区", "福田区", "宝安区", "龙岗区"],
            },
            "佛山": {
                "city_code": "440600",
                "city_tier": "二线",
                "districts": ["禅城区", "南海区", "顺德区", "三水区"],
            },
            "东莞": {
                "city_code": "441900",
                "city_tier": "二线",
                "districts": ["南城街道", "东城街道", "长安镇", "虎门镇"],
            },
        },
    },
    "福建": {
        "province_code": "350000",
        "region": "华南大区",
        "cities": {
            "福州": {
                "city_code": "350100",
                "city_tier": "二线",
                "districts": ["鼓楼区", "台江区", "仓山区", "晋安区"],
            },
            "厦门": {
                "city_code": "350200",
                "city_tier": "二线",
                "districts": ["思明区", "湖里区", "集美区", "海沧区"],
            },
            "泉州": {
                "city_code": "350500",
                "city_tier": "二线",
                "districts": ["丰泽区", "鲤城区", "晋江市", "南安市"],
            },
        },
    },
    "河南": {
        "province_code": "410000",
        "region": "华中大区",
        "cities": {
            "郑州": {
                "city_code": "410100",
                "city_tier": "新一线",
                "districts": ["金水区", "中原区", "管城回族区", "郑东新区"],
            },
            "洛阳": {
                "city_code": "410300",
                "city_tier": "三线",
                "districts": ["洛龙区", "西工区", "涧西区", "老城区"],
            },
            "南阳": {
                "city_code": "411300",
                "city_tier": "三线",
                "districts": ["宛城区", "卧龙区", "邓州市", "唐河县"],
            },
            "新乡": {
                "city_code": "410700",
                "city_tier": "三线",
                "districts": ["红旗区", "卫滨区", "牧野区", "新乡县"],
            },
        },
    },
    "湖北": {
        "province_code": "420000",
        "region": "华中大区",
        "cities": {
            "武汉": {
                "city_code": "420100",
                "city_tier": "新一线",
                "districts": ["洪山区", "江汉区", "武昌区", "东湖高新区"],
            },
            "襄阳": {
                "city_code": "420600",
                "city_tier": "三线",
                "districts": ["樊城区", "襄城区", "襄州区", "枣阳市"],
            },
            "宜昌": {
                "city_code": "420500",
                "city_tier": "三线",
                "districts": ["西陵区", "伍家岗区", "点军区", "夷陵区"],
            },
        },
    },
    "山东": {
        "province_code": "370000",
        "region": "华北大区",
        "cities": {
            "济南": {
                "city_code": "370100",
                "city_tier": "二线",
                "districts": ["历下区", "市中区", "槐荫区", "历城区"],
            },
            "青岛": {
                "city_code": "370200",
                "city_tier": "新一线",
                "districts": ["市南区", "崂山区", "黄岛区", "城阳区"],
            },
            "烟台": {
                "city_code": "370600",
                "city_tier": "二线",
                "districts": ["芝罘区", "莱山区", "福山区", "开发区"],
            },
            "临沂": {
                "city_code": "371300",
                "city_tier": "三线",
                "districts": ["兰山区", "罗庄区", "河东区", "沂南县"],
            },
        },
    },
    "四川": {
        "province_code": "510000",
        "region": "西南大区",
        "cities": {
            "成都": {
                "city_code": "510100",
                "city_tier": "新一线",
                "districts": ["高新区", "锦江区", "武侯区", "双流区"],
            },
            "绵阳": {
                "city_code": "510700",
                "city_tier": "三线",
                "districts": ["涪城区", "游仙区", "安州区", "江油市"],
            },
            "德阳": {
                "city_code": "510600",
                "city_tier": "三线",
                "districts": ["旌阳区", "罗江区", "广汉市", "什邡市"],
            },
            "南充": {
                "city_code": "511300",
                "city_tier": "三线",
                "districts": ["顺庆区", "高坪区", "嘉陵区", "阆中市"],
            },
        },
    },
    "重庆": {
        "province_code": "500000",
        "region": "西南大区",
        "cities": {
            "重庆": {
                "city_code": "500100",
                "city_tier": "新一线",
                "districts": ["渝北区", "南岸区", "九龙坡区", "沙坪坝区"],
            }
        },
    },
    "陕西": {
        "province_code": "610000",
        "region": "西北大区",
        "cities": {
            "西安": {
                "city_code": "610100",
                "city_tier": "新一线",
                "districts": ["雁塔区", "未央区", "高新区", "长安区"],
            },
            "咸阳": {
                "city_code": "610400",
                "city_tier": "三线",
                "districts": ["秦都区", "渭城区", "兴平市", "泾阳县"],
            },
            "宝鸡": {
                "city_code": "610300",
                "city_tier": "三线",
                "districts": ["金台区", "渭滨区", "陈仓区", "岐山县"],
            },
        },
    },
}


def province_names() -> list[str]:
    return list(GEO_CATALOG.keys())


def city_names(province: str) -> list[str]:
    province_meta = GEO_CATALOG.get(province, {})
    return list((province_meta.get("cities") or {}).keys())


def province_meta(province: str) -> dict[str, Any]:
    return GEO_CATALOG[province]


def city_meta(province: str, city: str) -> dict[str, Any]:
    return GEO_CATALOG[province]["cities"][city]

