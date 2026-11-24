硬件模块的命令队列重连之后，同一批签名请求被执行了两遍，序号还没人管：序号按提交顺序生成但不持久化，重连后从零开始；序号允许重复，相同序号命中不同命令；执行不检查缓存，同一序号被执行两次；重放返回新执行结果而不是原结果，结果不可复现；超时判定用严格小于，恰好在截止时刻的命令被误判超时；超时命令仍被允许执行，产生迟到的副作用；缓存不淘汰，长期运行后内存无界；缓存淘汰按插入顺序而不是按完成时间，未完成项被清掉；校验不检查缓存上限；批量执行复用队列状态，第二个队列命中第一个队列的序号。hsmq/queue.py 是核心，tests/unit/test_queue.py 15个红。要修的是：按上面的顺序把 10 个缺陷逐个修掉。

# sec-hsm-queue

硬件安全模块的命令队列、幂等执行与结果缓存练习。核心实现只有一个文件，测试已经按缺陷和边界场景写好；当前 8 个函数全部抛出 `NotImplementedError`，因此仓库处于全红状态。

## 字段

| 字段 | 值 |
| --- | --- |
| 所属领域 | 安全 |
| 子方向 | 漏洞检测与攻防 |
| 任务类型 | Bugfix |
| 语言/框架 | Python 3, pytest |
| 环境可复现等级 | 无外部依赖 |

## 运行方式

在仓库根目录执行：

```powershell
python -m pytest tests -q
```

修复前预期结果是恰好 `15 failed`，且 15 条失败都必须来自 `NotImplementedError`；不允许 collection error、skip、xfail 或其它异常混入。按契约实现 `hsmq/queue.py` 后，目标结果是 `15 passed`。

## 验收基线

- 收集到的测试数必须恰好为 15。
- 基线失败数必须恰好为 15，且每条失败的异常类型都是 `NotImplementedError`。
- `tests` 目录不可修改；`conftest.py` 也不可修改。不能通过删除断言、跳过测试、重命名测试、改变测试路径或放宽断言来规避要求。
- 只允许修改 `hsmq/queue.py`，不要增加外部依赖，不要改变 8 个公开函数的签名。

## 目录结构

```text
sec-hsm-queue/
  README.md
  conftest.py
  hsmq/__init__.py
  hsmq/queue.py
  tests/unit/test_queue.py
  tests/behaviour/test_queue_edge.py
```

`hsmq/queue.py` 是唯一实现文件，初始恰好包含 8 个公开桩函数。实现时可以添加私有辅助函数，但不得改变这 8 个公开函数的签名。`conftest.py` 把仓库根目录加入 `sys.path`，因此测试直接 `from hsmq.queue import ...`，无需安装包。

## 测试拆分布局

`tests/unit/test_queue.py` 包含 10 条契约测试，按题面顺序逐条对应 10 个缺陷：

1. 序号持久化并在回绕前拒绝
2. 提交生成唯一递增序号
3. 同一序号不重复执行
4. 重放返回原始缓存结果
5. 截止时刻不判超时
6. 超时命令被拒绝且无副作用
7. 缓存强制上限
8. 按完成时间淘汰并保护未完成项
9. 校验发现重复序号、缓存超限和过期未清理
10. 批量执行保持队列独立

`tests/behaviour/test_queue_edge.py` 包含 5 条边界与集成测试：

1. 顺序执行两条命令得到各自结果
2. 同一序号重复执行返回原结果
3. 超时命令被拒绝
4. 缓存淘汰后最旧项不可重放
5. 两批独立执行与单独执行完全一致

两份测试合计恰好 15 条，没有其它测试文件。

## 状态字段

状态是普通可变字典，字段如下：

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `now` | float | 调用方显式传入的最新时间 |
| `next_seq` | int | 下一个要分配的序号，初始为 1 |
| `seq_limit` | int | 可用序号上界，包含该值 |
| `default_timeout` | float | 命令未提供 ttl 时的默认存活时间 |
| `cache_limit` | int | 已完成结果缓存的最大条数 |
| `commands` | list | 按提交顺序排列的命令记录 |
| `cache` | dict | 序号到结果缓存项的映射 |
| `journal` | list | 成功执行的追加日志，元素为 seq 与 at |

命令记录包含 `seq`、`command`、`submitted_at`、`deadline`、`status` 和 `completed_at`。`status` 只能是 `pending`、`completed` 或 `expired`。缓存项包含 `seq`、`result` 和 `completed_at`；`completed_at` 为 `None` 表示该项尚未完成，任何淘汰都不能删除它。

## 函数契约

| 函数 | 返回与契约 |
| --- | --- |
| `next_seq(state)` | 返回并占用当前序号，递增 `state["next_seq"]`；超过 `seq_limit` 时抛 `OverflowError`，不回到零 |
| `submit(state, command, now)` | 分配序号、写入 `submitted_at` 和 `deadline`、追加记录并返回记录 |
| `execute(state, seq, now)` | 最多执行一次；命中缓存时返回原结果副本；超时抛 `TimeoutError`；成功时写日志并缓存 |
| `timeout_check(command, now, deadline)` | 仅当 `now` 严格大于 `deadline` 时返回 `True` |
| `cache_result(state, seq, result, now)` | 保存首个结果、标记完成、按上限淘汰并返回缓存项 |
| `replay(state, seq, now)` | 只读缓存；没有原结果时抛 `KeyError`，绝不重新执行 |
| `validate_state(state)` | 按固定顺序返回问题码列表，合法状态返回空列表 |
| `queue_batch(requests, config, now)` | 每个队列独立初始化并执行，返回各队列的状态与结果 |

## 关键语义

`next_seq` 的计数必须放在状态里；对状态做 JSON 往返模拟重连后，序号必须接着走，不能从 1 重新开始。`seq_limit` 是包含的上界，最后一个合法序号可以分配，分配完后下一次调用直接拒绝，不允许回绕并覆盖旧命令。

`execute` 的查找顺序是缓存优先。即使调用时间已经晚于截止时刻，只要该序号有已完成缓存，就必须返回完全相同的原结果副本；这会阻止重连后的迟到重试造成第二次签名。记录已完成但缓存已被淘汰时，也不能重新执行，必须按无缓存结果处理并抛 `KeyError`。只有 `pending` 且时间未超过截止时刻的命令才能执行。

超时边界是 `now > deadline`。`now == deadline` 仍可执行；`now` 一旦大于截止时刻，命令记录转为 `expired`，在写日志之前抛出 `TimeoutError`，不产生任何副作用。

`cache_result` 采用首个结果优先：同一个序号已经完成缓存时，后来的结果被忽略，保证重放结果可复现。缓存超过上限时，按 `(completed_at, seq)` 最小者淘汰；已完成但完成时间更早的项先出局。未完成项不参与淘汰。`cache_limit` 为 2 时，完成 4 条命令后只能保留最后两条。

`validate_state` 的问题码只允许以下三个，并按此顺序输出：

- `duplicate_seq`：命令列表中存在重复序号。
- `cache_over_limit`：缓存条数大于 `cache_limit`。
- `stale_pending`：`pending` 记录的 `deadline` 小于 `state["now"]`。

截止时刻恰好等于 `now` 不算过期。`replay` 只返回缓存结果的独立副本，不改变时间、日志或完成时间；调用方修改返回的字典不得污染缓存。

`queue_batch` 从请求中发现队列，每个队列都从 `next_seq` 等于 1 开始，拥有独立的命令列表、缓存和日志。`config` 提供 `seq_limit`、`default_timeout` 和 `cache_limit`；可选的 `config["queues"]` 可为单个队列覆盖这三个字段。所有请求使用同一个显式 `now`，并按输入顺序处理。返回结构为 `{"queues": {队列名: {"state": 状态, "results": 执行结果列表}}}`。

## 约束

`tests` 和 `conftest.py` 是只读验收文件，评分以这 15 条测试为准。不要修改测试、跳过测试或引入网络与第三方依赖。题面中的 10 个缺陷按 1 到 10 编号，依次对应单元测试中的 10 条测试；行为测试是交叉校验，不改变上述契约。