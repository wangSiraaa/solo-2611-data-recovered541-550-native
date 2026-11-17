# 排序规则换代下的持久文字索引与旧读语义

Python 3.11 实现。三个交付件：

| 文件 | 作用 |
|------|------|
| `collations.py` | 公共比较目录：不可变版本、`Handle`/`Registry`，定义校验与持久化 |
| `store.py` | 持久文字索引：版本化物理结构、读句柄、迁移/发布边界 |
| `demo.py` | 起点样例的正常路径与新结果（等价类、旧 Handle、旧读句柄、索引证据） |
| `verify.py` | 覆盖全部合同的自动核对（158 项） |
| `build_artifacts.py` | 生成可核对的真实持久文件与 `report.json` |

```bash
cd materials/547
python3 demo.py            # 查看等价类、旧 Handle/旧读句柄与索引查询计划
python3 verify.py          # 全量核对（含 20000 行、10 次登记、2 个名字、故障点）
python3 build_artifacts.py # 写出 artifacts/{catalogue,index}.sqlite 与 report.json
```

## 1. 比较规则（目录公共承诺）

固定、与区域无关，`key_for(text, definition)`：

```
NFD → (case_insensitive 时完整 casefold) → (accent_sensitive 为假时删除 category==Mn)
    → NFC
```

等值 = 该键字符串相等；顺序 = 键字符串的 Unicode 码点字典序（不做自然语言排序）。
`collations.py` 与 `store.py` 使用同一实现，独立导入工具直接用 `Handle.key/compare`。

`e/E/é` 两类：
* `case_insensitive=True, accent_sensitive=True` : `e == E`，`e != é`
* `case_insensitive=True, accent_sensitive=False`: `e == E == é == É`

`ß/ẞ/SS` 在 casefold 下等价（`straẞe`、`STRASSE` 同类），演示见 `demo.py`。

## 2. 目录（`Registry`）

* `define(name, *, case_insensitive, accent_sensitive)` **总是新增**一个不可变版本，
  返回 `Handle`。两个选项必须是真正的 `bool`（`1`/`"true"` 均拒绝）；未知选项、
  未知名字、未知 revision 明确抛异常，且**拒绝不改变任何既有版本**（校验先于写入）。
* `snapshot(name, revision=None)` 取当前或一个明确旧版本；`Handle` 不可变，
  所以独立导入程序在后续 `define` 之后持有的旧 `Handle` 比较结果不变。
* 每个版本的身份是 `(name, revision)` **加上定义内容哈希** `definition_id`
  （`sha256` 规范化 JSON）。`resolve`/`restore` 按 `(name, revision)` 取回时核对哈希：
  重建 `Registry` 后计数从 1 开始、却拿另一份定义来解释旧数据，会被明确拒绝。
* 可选 `Registry(store_path)` 把每个版本及当前 revision 持久化到可重读的
  SQLite（`definitions`、`current_versions`）。

## 3. 持久索引布局（`store.py`）

**选择：版本化并存结构 + 单事务全量重建 + 原子发布，而不是同一 collation 名换回调。**

* `docs(id, word)`：原文与独立行身份（id）。两份文字相等、或不同原文比较键相同，
  都保留为不同记录。
* `doc_keys(version_key, id, from_seq, to_seq, ckey, word)`：每个
  `(name, revision, definition_id)` 一份**物理独立**的 B-tree。
  * `ckey` 是用 `handle.key()` 算好的比较键，**不需要任何自定义 collation 回调**；
    索引使用 SQLite `BINARY` 排序，其 UTF-8 字节序恰等于 Unicode 码点序，
    因此“换规则”必须换结构，旧结构不会被悄悄重新解释。
  * 行以半开写序号区间 `[from_seq, to_seq)` 记录可见期：更新/删除只关闭旧区间、
    插入新区间，所以固定快照的旧读句柄看到的是当时版本、当时行集合，不漏不重。
  * `doc_keys_order(version_key, ckey, id, from_seq)` 支撑等值与范围；等值/范围都
    是索引前缀 `(version_key, ckey…)`，查询计划见 `demo.py` / `Explain*`。
* `doc_versions`：每个版本的 `definition_json`、`name/revision/definition_id`、
  `catalog_state`（unpublished/published）、`state`（building/complete/reclaimed）、
  `built_seq/struct_seq/published_seq`。**持久规则定义带同身份哈希**，重开时可核对。
* `outbox(seq, op, id, word)`：构建快照之后的写入历史，用于发布前追赶。
* `migrations`：迁移审计日志（started/built/published/aborted/failed）。

### 写入与旧读

`add/update/delete` 在一个写事务里同时更新 `docs`、当前版本的 `doc_keys`、`outbox`。
所有版本共享同一 `write_seq` 序号。读句柄 `Store.open_reader()` 绑定**当时的
`version_key` 与 `write_seq`**，直到 `close()` 都按它们解释：
旧版本区间在更新/删除时只被“关闭”而非删除，因此旧句柄不会漏行、重复或被重解释。
返回的 `ReadRow` 只含原始 `id`、原始 `word` 和使用版本，**不把归一化文字冒充原文**。
不同 `Store` 实例的读句柄也计入进程级活动计数，避免互相回收。

### 迁移与发布边界

`Store.migrate(name=None, revision=None, *, crash_after_build=False,
crash_after_publish=False)` 选择一个**已登记明确版本**（`revision=None` 取当前，
显式 revision 取旧版本），然后：

1. **构建**：在单个写事务内为全表算键写入新结构，事务内 `_verify_built_structure`
   逐行核对 `docs` 完全覆盖且键正确；提交时结构为 `complete` 但仍 `unpublished`。
   失败则回滚并清理，旧完整版本保持可读。
2. **故障点 A（`crash_after_build=True`）**：结构已建完、尚未发布时抛出；
   重开 Store 后 `status()/migrations` 可判定“已建未发布”，`migrate` 重试直接复用
   完整结构并发布，无需重建。
3. **发布**：另一个写事务里从 `outbox` 重放构建快照之后的写入（故迁移快照后成功的
   新增不会被覆盖），再核对发布快照，最后把 `catalog_state` 置 `published`、
   记录 `published_seq`——**原子**完成。
4. **故障点 B（`crash_after_publish=True`）**：发布已提交后抛出；重开可见
   `published`，`migrate` 幂等（`already_published=True`），不会重复。

无读句柄保护/重建需要时（历史已被裁剪），`_rebuild_catching_up` 在同一事务内
替换结构并自校验，且拒绝在有活动读句柄时执行。写事务之间的交错按提交次序确定；
迁移只短暂串行化写事务。

### 空间与回收

新的当前版本发布后，旧结构仍保留以服务已有旧读句柄；当某旧版本的**最后一个**
读句柄 `close()` 时，自动删除其 `doc_keys` 并把该版本标记 `reclaimed`（`status`
可见行数归零）。`Store.reclaim_unused_structures()` 可显式触发。
`outbox` 只保留尚未发布/仍需追赶/仍有活动读句柄的结构可能需要的写入前缀，
超过后裁剪并记录 `outbox_floor`，避免无限增长；需要时按 floor 触发重建而非错误重放。

## 4. 复现与证据

* `python3 demo.py`：打印 `e/E/é` 两类等价、旧 `Handle` 在 `define` 后仍保持旧比较、
  旧读句柄在迁移后仍见旧版本、新读句柄见新版本；对当前版本执行一次等值与一次窄范围
  请求，输出 SQLite `EXPLAIN QUERY PLAN`（`SEARCH doc_keys USING INDEX
  doc_keys_order (version_key=? AND ckey=?)` 等）及实际检查记录数。
* `python3 verify.py`：158 项断言，覆盖短数组逐行独立计算、等值/范围/重复键/多次切换、
  固定读句柄交错增删改与迁移、两处故障点与重开判定、同版本号不同定义拒绝、
  20000 行 × 10 次登记 × 2 个名字 × 四种选项组合、随机差分（零误差）、回收等。
* `python3 build_artifacts.py`：写出真实文件
  `artifacts/catalogue.sqlite`、`artifacts/index.sqlite`、`artifacts/report.json`；
  两个版本的结构都保留在库中，可直接用 `sqlite3` 核对 `doc_versions`、`doc_keys`、
  `migrations` 与 `definitions`。

关于材料中的 `REINDEX`：它只对“同一个 collation 名换了回调”的孤立索引有效，
无法履行公共目录、固定读句柄和发布边界这些合同；本交付因此采用版本化物理结构，
换规则时建立并核对新结构后再原子发布，旧结构继续服务旧句柄。
