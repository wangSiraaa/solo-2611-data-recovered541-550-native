# 分桶物理行 + 事务写号目录（materials/550）

两个相互独立但配套的真实生产起点：

1. **`txn_directory.py` — `Directory`**：SQLite 持久化的全局事务号 / 每表写号目录。
2. **`bucket_reader.py` — `Reader`**：从可查看的 JSONL 物理行，按合法快照读取真实当前行。

不兼容完整 Hive/ORC 或任何商业数据湖；只实现题面的有限合同。仅用 Python 3.11 标准库（含 `sqlite3`、`json`），无第三方依赖。

## 修复的两个根本缺陷

| 旧行为 | 后果 | 现在的行为 |
| --- | --- | --- |
| 快照例外清单放的是全局 `txn_id` | 多表交错后全局事务号 ≠ 表写号，例外清单指错写号 | 例外清单只含**表 `write_id`**；`txn_id` 仅用于目录解释分配映射，绝不进入写号清单 |
| 行身份按 `(bucket_id, row_id)` 覆盖 | 同 `row_id` 不同物理行被误并；删除按 row 号误删另一行 | 行身份严格为完整 RID `(original_write_id, bucket_id, row_id)`；删除只撤目标复合 RID |

## 编号模型（两套互不替代的编号空间）

- `begin()` 分配从 1 递增、不复用的全局 `txn_id`。
- `allocate(txn_id, table)` 仅接受 **open** 事务，为该表分配从 1 递增的 `write_id`；
  同事务同表重试返回原号；不同表分别计数；一个事务可写多表也可空写。
- `finish(txn_id, 'committed'|'aborted')`：首次终结后不可反向；相同终态重试幂等；
  未知事务/非法输入原子拒绝（`DirectoryError`）。布尔值不被接受为整数编号，`table` 必须非空字符串。
- 全部状态落真实 SQLite 文件，关闭重开保持；只要求单进程顺序调用。

## 快照语义

`snapshot(table)` 返回固定切面（JSON 可序列化、可由外部目录生成、可落盘重读）：

- `txn_high_watermark`：调用时全局事务高水位；
- `high_write_id`：该表写号高水位（空表为 0）；
- `open_write_ids` / `aborted_write_ids` / `invalid_write_ids`（两者并集），全部是**表写号**；
- 仅当写在该切面已 committed 且 `write_id <= high_write_id` 才可见；
- 快照导出后，原 open 事务再提交/中止不改变已有快照的有效集合；
- 非法快照（表身份不符、负编号、越界/重复例外、open 与 aborted 交叠、并集不符等）由读取器**拒绝**，不静默修正。

## 物理行与读取规则

- 清单声明 `table`、`bases`、`deltas`；delta 项给 `path/kind/statement_id/min_write_id/max_write_id`。
- insert 行含 `current_write_id, original_write_id(==current), bucket_id(0..15), row_id>=0, operation_no>=0, 整数 value`；
  delete 行无 `value`，前三字段指向被删旧行。
- 先用当前快照过滤每个操作的 `current_write_id`，再按 `(current_write_id, statement_id, operation_no)` 解释；
  **不按文件枚举顺序决定结果**，也不用删除目标的 `original_write_id` 判断删除是否提交。
- 重复操作身份、同一可见 insert RID 重复、非法字段、删除不可见 RID 均使本次读取 `failed`；失败绝不产出 complete 数据集。
- 输出按完整 RID 排序，每行保留 `value`、原物理 `source_file/source_line`、`provenance` 及本次快照信息。

### base 资格

一份 base 可用，当且仅当 `base_write_id <= 快照表高水位` 且快照中不存在 `<= base_write_id` 的 open 写号。
旧快照中的 open 后来提交，也**不能**使用包含它的新 base（材料中 B2 即此反例）。
选 base 时取合资格者中边界最高者；无合资格 base 则始终可从起始空表完整重放全部 delta。

### 完整重放 vs 合资格 base + 尾部（取舍）

机器可读小例：`artifacts/expected/replay_vs_base_example.json`（measurements，结果集逐 RID/value 相同）。

- **读取量**：本实现为做全量校验，两种模式都会打开清单中的全部 delta 文件并逐行做快照过滤；
  差别在**应用的操作数**——完整重放应用全部可见操作（例中 253 个），base+尾部只应用边界之后的操作（例中 0 个），
  但要额外读一次 base 文件（例中 253 行）。在增量写入远大于 base 的真实场景，base+尾部的 CPU/状态操作量显著更低。
- **校验**：完整重放天然校验整条 delta 链（重复身份、非法字段、删除落空都会暴露）；
  base 是受信任的物化结果，只校验 base 自身与尾部，base 之前的历史不再被复核。
- **存储**：保留 base 增加一份与当前行集同量级的冗余拷贝；不选 base 则零冗余但每次读取都重放全历史。
- 题面不要求最优 base 选择；本实现选最高合资格边界，并始终保证两种模式结果一致（有核验）。

## 入口

- `Directory.begin/allocate/finish/snapshot` 用途保留；新增 `export_snapshot/tables/status/close`。
- `Reader.scan/scan_snapshot` 用途保留；新增 `export_scan`（JSONL + complete/failed 状态 sidecar）、
  `explain_rid`（按完整 RID 解释）、`load_snapshot`（接受外部合法目录生成的快照 JSON）。
- `tool.py` 提供 CLI：`status / snapshot / scan / explain`（见文件头示例）。

## 复跑

```bash
cd materials/550
python3 demo.py                 # 原始最小演示（保留可运行）
python3 generate_materials.py   # 确定性生成 artifacts/（4 表/100 事务/16 桶/≤100 文件/每表≤1000 操作）
python3 verify.py               # 114 项独立核验，报告写入 artifacts/results/verification_report.txt
```

## 产物（新进程可直接打开）

```
artifacts/
  txn_directory.sqlite          # 持久目录文件（分配、终态、身份）
  snapshots/*.json              # 各表 final 快照 + measurements 的 S1 旧切面快照
  data/                         # 输入只读物理族：JSONL、bases、各清单（含 replay/s1/b2only 变体）
  expected/*_operations.jsonl   # 逐操作小列表（独立预期的输入，非 Reader 导出）
  expected/s1_meta.json, replay_vs_base_example.json
  results/*_rows.jsonl          # 真实行数据集（complete）
  results/*_status.json         # complete/failed 状态 sidecar
  results/verification_report.txt
```

## 已验证行为（独立预期，不用 Reader 自身导出当答案）

预期有效写号来自对 SQLite `txns/writes` 原始表的独立读取与分配/终态重放；
预期复合 RID 来自 `expected/*_operations.jsonl` 的手写顺序/可见性重放。核验覆盖：

- 多表交错后 `txn_id` 与表 `write_id` 不同；同事务多表分配与重试幂等；
- 同 `row_id`、不同 `original_write_id` 的两行同时存在；
- committed 删除只撤目标复合 RID；aborted 删除不生效（`(1,7,1)` 存活）；
- 旧快照 S1 在 open 事务终结后不漂移；S1 拒绝使用后来包含原 open 写号的 B2，回退完整重放；
- 不同 statement / 反转文件顺序结果一致；完整重放与合资格 base+尾部结果一致；
- 目录状态机、非法输入（布尔编号、空表名、终态反转、未知事务等）与非法快照/清单/物理行全部被拒绝；
- 读取失败只写 `failed` 状态，不产出 complete 数据；SQLite 关闭重开保持编号与终态。

## 支持范围与边界

- 规模：4 张表、100 个事务、每表 ≤1000 物理操作、16 个桶、≤100 个 JSONL 文件（生成结果为 20 个文件）。
- 可全量读入内存；无吞吐、最少扫描次数或固定文件布局要求。
- 路径仅限输入根内普通相对路径（拒绝绝对路径与 `..` 越界）。
- 不要求压缩、损坏文件修复、并发 compaction、GC、分布式提交或活动写入崩溃恢复。
