# 行访问策略起点

Python 3.11，仅标准库。从本目录运行 `python3 demo.py`。

`policy.py` 提供独立策略客户可调用的目录、计划和表达式求值；`row_gateway.py` 在实际 SQLite 行上应用当前计划。示例为合成数据。起点只实现首条匹配规则和普通 SELECT/UPDATE，不是完整策略或数据库安全实现。`seed` 是受信初始化入口，不属于受策略约束的业务写入。
