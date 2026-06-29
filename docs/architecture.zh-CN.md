# SwarmSolve — 详细架构（简体中文）

本文是 SwarmSolve 的**代码级**走读。项目概览、快速开始与分工见根目录
[`README.md`](../README.md)。

---

## 1. 分层架构

```
┌──────────────────────────────────────────────────────────────┐
│ Peer（一个操作系统进程 / 一台机器）                            │
│                                                                │
│  求解层 Solver     约束传播 + DFS              solver/         │
│  任务层 Task       切分 / 去重 / 租约 / 再均衡  tasks/         │
│  传播层 Gossip     流行病式扩散 + 去重          gossip/        │
│  发现层 Discovery  Kademlia DHT（XOR, k桶）     discovery/     │
│  传输层 Transport  TCP（任务）+ UDP（发现）     transport/     │
└──────────────────────────────────────────────────────────────┘
        ▲                                                  ▲
        └──────────── 本机/局域网上的 TCP / UDP ───────────┘
```

| 层 | 文件 | 课程知识点 |
|----|------|-----------|
| 传输 | [`transport/messages.py`](../src/swarmsolve/transport/messages.py)、[`transport.py`](../src/swarmsolve/transport/transport.py) | 第2章 — TCP、消息 |
| 发现 | [`discovery/node_id.py`](../src/swarmsolve/discovery/node_id.py)、[`routing.py`](../src/swarmsolve/discovery/routing.py)、[`kademlia.py`](../src/swarmsolve/discovery/kademlia.py) | **第6章 — Kademlia** |
| 传播 | [`gossip/gossip.py`](../src/swarmsolve/gossip/gossip.py) | 第2章 Gossip + 第7章 BubbleStorm |
| 任务 | [`tasks/task.py`](../src/swarmsolve/tasks/task.py)、[`scheduler.py`](../src/swarmsolve/tasks/scheduler.py) | 第5章 负载均衡 + 容错 |
| 求解 | [`solver/board.py`](../src/swarmsolve/solver/board.py)、[`search.py`](../src/swarmsolve/solver/search.py) | 应用核心 |
| 编排 | [`peer.py`](../src/swarmsolve/peer.py)、[`cli.py`](../src/swarmsolve/cli.py) | 粘合 + 演示 |

---

## 2. 仓库结构

```
src/swarmsolve/
├── transport/
│   ├── messages.py   # Message 数据类 + MessageType 枚举 + encode/decode
│   └── transport.py  # asyncio TCP 服务器 + UDP 端点
├── discovery/
│   ├── node_id.py    # 160 位 NodeID、XOR 距离、task_key()
│   ├── routing.py    # k桶 RoutingTable、Contact
│   └── kademlia.py   # PING/PONG/FIND_NODE、迭代查找、bootstrap
├── gossip/
│   └── gossip.py     # 推式 gossip：seen 去重 + TTL 扇出
├── tasks/
│   ├── task.py       # Task（一棵子树）+ TaskStatus + path_repr/task_key
│   └── scheduler.py  # open/claimed/dead/done 集合、租约、按 XOR 选任务
├── solver/
│   ├── board.py      # 位掩码棋盘 + 约束传播
│   └── search.py     # DFS、子树切分、enumerate、node_delay
├── puzzles.py        # 解析 / 生成题目（瞬时构造完整解再挖空）
├── peer.py           # Peer：串起所有层 + 工作循环
└── cli.py            # gen / solve / demo / benchmark / dashboard / fault / peer
```

---

## 3. 逐模块走读

### 3.1 传输层 — `transport/`

**`messages.py`** 定义唯一的线上类型 [`Message`](../src/swarmsolve/transport/messages.py)
（`type, sender, payload, msg_id, ttl, ts`）和 [`MessageType`](../src/swarmsolve/transport/messages.py)
枚举。作业要求的三类*应用*消息是 `OPEN_TASK`、`DEAD_END`、`SOLUTION`；其余是协调
（`TASK_CLAIM`、`TASK_DONE`）与发现（`PING/PONG/FIND_NODE/FIND_NODE_REPLY`）。序列化用
换行分隔的 JSON（`encode`/`decode`），便于演示调试，后续可换 msgpack。

**`transport.py`** — [`Transport`](../src/swarmsolve/transport/transport.py) 持有一个
`asyncio` **TCP 服务器**（每连接一请求，用于任务/解载荷）和一个 **UDP 端点**（数据报，
用于 Kademlia）。统一回调 `handler(msg, addr, kind)` 接收所有消息；`send_tcp` 在对端
掉线时返回 `False`（用于探测故障），`send_udp` 尽力而为。

### 3.2 发现层（Kademlia） — `discovery/`

**`node_id.py`** — [`NodeID`](../src/swarmsolve/discovery/node_id.py) 是 160 位 ID。
关键函数：`xor_distance`、`shared_prefix_len`（桶下标），以及最关键的
[`task_key`](../src/swarmsolve/discovery/node_id.py)：把搜索树路径哈希进与节点 ID **同一个**
XOR 空间——这是 Solver 与 DHT 之间的桥梁。

**`routing.py`** — [`RoutingTable`](../src/swarmsolve/discovery/routing.py) 持有
`ID_BITS` 个 k桶（`K=8`）。`add` 在桶内按 LRU（偏好长寿节点 → 抗日食攻击）；
`closest(target, n)` 返回 XOR 距离最近的 n 个联系人——既用于路由也用于任务归属。

**`kademlia.py`** — [`KademliaNode`](../src/swarmsolve/discovery/kademlia.py) 实现
`PING/PONG`、`FIND_NODE`、迭代 `lookup`（O(log n) 轮）、`bootstrap`，以及
`is_responsible_for(key, replicas)`（我是否在离该 key 最近的若干 peer 之内？）。刻意
省略 STORE/FIND_VALUE——我们只用键空间把*任务路由到最近 peer*，不做值存储。

### 3.3 传播层 — `gossip/`

[`Gossip`](../src/swarmsolve/gossip/gossip.py) 是推式：收到消息时 (1) 用有界
`seen` `OrderedDict` 丢弃重复，(2) 投递给本地 `deliver` 回调，(3) 若 `ttl > 0` 则递减
并转发给随机 `fanout`（=3）个邻居。既限制流量，又能高概率覆盖全网（第7章 BubbleStorm
思想）。

### 3.4 任务层 — `tasks/`

**`task.py`** — [`Task`](../src/swarmsolve/tasks/task.py) 是搜索空间的一棵子树，由其
赋值 `path` 标识。`path_repr` 是规范化（与顺序无关）字符串；`Task.key` 即
`task_key(path_repr)` → 它在 XOR 空间中的位置。`lease_active()` 判断认领是否仍有效。

**`scheduler.py`** — [`Scheduler`](../src/swarmsolve/tasks/scheduler.py) 是每个 peer
的大脑。状态：`open`、`claimed`、`dead_ends`、`done`。亮点：

* `add_open` 忽略已 dead/done/被有效认领的任务（去重）。
* `next_task` 选取**与本机 ID XOR 距离最小**的开放任务——这就是结构化、低冲突的放置
  （第5/6章）。
* `reclaim_expired` 把过期租约的任务移回 `open` → peer 崩溃时自动重分配（容错）。

### 3.5 求解层 — `solver/`

**`board.py`** — [`Board`](../src/swarmsolve/solver/board.py) 每个格子存一个**候选位
掩码**。`assign` 做消元 + 唯一候选（naked singles）传播（AC-3 风格），冲突时抛
`Contradiction`。`most_constrained_cell` 实现 MRV 启发式。支持任意 N=k²（9/16/25）。

**`search.py`** — 三个原语：
* [`expand_subtasks`](../src/swarmsolve/solver/search.py) — 对 MRV 格子的每个候选生成
  一个子路径（即时矛盾的直接剪掉）。
* [`solve_subtree`](../src/swarmsolve/solver/search.py) — DFS 一棵子树，带钩子
  `is_dead_end` / `record_dead_end` / `should_stop`，外加 `node_delay`（演示成本旋钮）
  与 `enumerate_all`（遍历整棵树 / 统计解数）。
* [`solve_local`](../src/swarmsolve/solver/search.py) — 单机基线。

### 3.6 编排 — `peer.py`

[`Peer`](../src/swarmsolve/peer.py) 串起所有层并运行工作循环。关键方法：
`start`/`bootstrap`、`_dispatch`（区分发现 vs gossip）、`_on_gossip`（应用
OPEN_TASK/DEAD_END/TASK_DONE/TASK_CLAIM/SOLUTION）、`seed_frontier`+`submit`（生产者）、
`run`+`_work_on`（消费者）以及剪枝钩子。重要开关：`split_depth`（工作窃取）、
`enumerate_mode`、`lease_seconds`、`idle_limit`、`node_delay`、`dead_end_share_depth`、
`on_tick`（仪表盘）。

### 3.7 命令行 — `cli.py`

[`cli.py`](../src/swarmsolve/cli.py) 暴露所有命令与共享的多进程机制（`_peer_worker`、
`_spawn`、`_collect`）。`_collect` 对被 kill 的 peer 鲁棒（轮询存活而非死等 N 个结果）。

---

## 4. 消息协议

| 类型 | 传输 | 载荷 | 用途 |
|------|------|------|------|
| `PING`/`PONG` | UDP | host, port | 存活探测 / 桶刷新 |
| `FIND_NODE` | UDP | target | 迭代查找 |
| `FIND_NODE_REPLY` | UDP | target, nodes[], reply_to | 查找应答 |
| `OPEN_TASK` | TCP/gossip | task | 公布一棵未探索子树 |
| `TASK_CLAIM` | TCP/gossip | task（owner, lease） | “我来做这个” |
| `DEAD_END` | TCP/gossip | path | 全网裁剪该子树 |
| `TASK_DONE` | TCP/gossip | path | 子树已探索完 |
| `SOLUTION` | TCP/gossip | board（扁平） | 最终答案 → 全员停止 |

---

## 5. 端到端流程

```mermaid
sequenceDiagram
    participant S as 提交者
    participant A as 节点 A
    participant B as 节点 B
    S->>S: seed_frontier() 把根切成子任务
    S-->>A: OPEN_TASK*
    S-->>B: OPEN_TASK*
    A->>A: next_task() = 离我最近，认领（租约）
    B->>B: next_task()（另一个任务），认领（租约）
    A->>A: DFS；浅层矛盾
    A-->>B: DEAD_END(path)
    B->>B: 裁剪该子树
    B->>B: DFS → SOLUTION
    B-->>S: SOLUTION
    B-->>A: SOLUTION
    Note over S,A,B: should_stop() 在各处触发
```

---

## 6. 关键机制（深入）

* **XOR 任务放置。** `task_key(path)` 位于节点 ID 空间，因此 `next_task` 偏好离本机最近
  的任务，使工作确定性分布且冲突少——Kademlia（第6章）兼作负载均衡器（第5章）。
* **工作窃取（`split_depth`）。** 当任务深度小于 `split_depth` 时，`_work_on` 把它再切成
  更细的 OPEN_TASK 并 gossip 出去，而不是自己求解。粒度自适应集群规模，让空闲 peer 有活干。
* **租约与重分配。** `claim_local` 设 `lease_expires = now + lease`；`reclaim_expired`
  （在 `next_task` 内调用）把过期任务移回 `open`，于是崩溃 peer 的工作被重做。`idle_limit`
  让 peer 存活足够久以等待租约过期。
* **去重。** `add_open` 加上 `_work_on` 里最后一刻的检查，跳过已 done/dead/被有效认领的
  任务，削减由 gossip 延迟造成的大部分重复（即作业的“避免重复工作”挑战）。
* **死路深度上限（`dead_end_share_depth`）。** 只 gossip *浅层*死路；深层叶子死路太多太
  具体。没有这个限制，难题会用上万条消息把网络打爆。
* **`node_delay`。** 仅用于演示的人为每节点成本。真实数独节点太廉价，无法暴露网络效应，
  故用它代理“昂贵”计算（25×25 / 拼图），用于测量加速、恢复与仪表盘。

---

## 7. 三个演示

### A）容错 — `swarmsolve fault`
以**穷举模式** + 较大 `idle_limit` 运行，因此每个任务*必须*完成。它在求解中途 kill 一个
peer（`--kill-peer`、`--kill-after`）；该 peer 的租约（`--lease`）过期后其任务被存活者
接管。只有重分配成功，整个运行才会结束。
```bash
uv run swarmsolve fault --file examples/puzzles/hard_9x9.txt \
    --peers 4 --kill-peer 2 --kill-after 1.5 --lease 1.5 --node-delay 0.0008
```
关注：*“killed peer #2 returned a result: no”* 与 *“swarm STILL solved …”*。

### C）实时仪表盘 — `swarmsolve dashboard`
每个 peer 通过 `on_tick` 上报快照；父进程用 `rich.Live` 渲染表格（每个 peer 的
邻居 / open / claimed / dead / done / nodes / found）。
```bash
uv run swarmsolve dashboard --file examples/puzzles/hard_9x9.txt --peers 4 --node-delay 0.003
```

### B）真实加速 — `swarmsolve benchmark`
诚实的加速叙事。**首解**搜索（`demo`）把答案放在一条无法并行的深 DFS 路径上；
**穷举**搜索（`benchmark`——统计所有解 / 验证唯一性）天然可并行，呈现近线性加速。
```bash
uv run swarmsolve benchmark --file examples/puzzles/hard_9x9.txt \
    --peers 4 --node-delay 0.0012 --split-depth 4
# 基线 ~14.5s ；集群 ~8.7s ；加速 ~1.67x ；解数一致
```

---

## 8. 性能 — 诚实讨论

* 对**首解**数独，墙钟加速有限：解位于一条深路径，沿之的 DFS 本质串行。协调开销甚至会
  让极小的 9×9 比单机更*慢*。
* 对**穷举**型负载，加速是真实的（我们 4 peer 实测约 1.3–1.7×）。低于理想的 4× 源于
  (a) 异步 gossip 造成的重复探索，(b) 当某子树远大于其他时的负载不均。更深的 `split_depth`
  改善均衡但增加重复——这是分布式搜索的经典权衡，也是报告的极佳讨论点。
* 进一步提升加速的方向：基于 XOR key 的确定性单一负责人执行（消除重复）、感知抖动的归属、
  以及更细的按需自适应切分（按需工作窃取）。

---

## 9. 扩展：拼图

框架与具体谜题无关：凡是能表达为“搜索树切分子任务 + 死路剪枝 + 首解/全解”的问题都适配。
对拼图，每次**拼块放置**是一个分支，非法的局部拼装就是死路。只需替换 `solver/` 包，
传输/发现/传播/任务层完全复用。

---

## 10. 课程知识点映射

| 知识点 | 代码位置 |
|--------|----------|
| Gnutella 式消息、TTL 泛洪（第2章） | `transport/`、`gossip/` |
| Gossip / 流行病式扩散（第2章） | `gossip/gossip.py` |
| Kademlia：XOR 度量、k桶、FIND_NODE（第6章） | `discovery/` |
| 结构化放置 / 负载均衡（第5章） | `task_key` + `scheduler.next_task` |
| 概率覆盖（第7章 BubbleStorm） | gossip 扇出 + seen 集合 |
| 容错 / 抖动 | 租约 + `reclaim_expired` + `is_responsible_for` |

---

## 11. 课程知识点详解

每个课程章节在代码中的体现。

* **第1章 — P2P 基础。** SwarmSolve 是*纯* P2P 系统：每个节点既是客户端又是服务器，
  **没有中央索引**（不同于 Napster），节点自组织，系统**自扩展**（节点越多搜索吞吐越高）
  且**有韧性**（容忍崩溃）。见 [`peer.py`](../src/swarmsolve/peer.py)。
* **第2章 — 非结构化覆盖网与 gossip。** Gnutella 用 TTL 泛洪查询。我们保留其优点
  （gossip + TTL），并用 **seen 集合**去重修正冗余。三类应用消息都搭载在此 gossip 上。
  见 [`gossip/gossip.py`](../src/swarmsolve/gossip/gossip.py)。
* **第3章 — 随机图模型。** Kademlia 构建的覆盖网是低直径图（O(log n) 跳）；每个节点保存
  O(k·log n) 状态——这是度与直径的经典权衡（小世界 / 无标度）。
* **第4章 — DHT（CAN / Chord）。** 结构化覆盖网用定向 O(log n) 路由与 put/get 键接口
  取代泛洪。我们采用这种*结构化*哲学，并选 Kademlia（第6章）作为具体 DHT。
* **第5章 — 负载均衡（Distance-Halving 思想）。** 目标是均匀分摊负载。我们把任务路径
  均匀哈希进 XOR 键空间，*免费*得到均匀的任务分布；离 key 最近的节点拥有该任务。见
  [`task_key`](../src/swarmsolve/discovery/node_id.py) + [`Scheduler.next_task`](../src/swarmsolve/tasks/scheduler.py)。
* **第6章 — Kademlia（我们的发现层）。** XOR 距离、偏好长寿节点的 k桶（抗日食）、基于
  UDP 的迭代 FIND_NODE——既是发现也是任务放置的骨架。见 [`discovery/`](../src/swarmsolve/discovery)。
* **第7章 — BubbleStorm（概率覆盖）。** 随机副本使查询以高概率遇到数据。我们的 gossip
  扇出 + TTL 实现同样思想：消息以高概率覆盖全网，同时流量受控。

---

## 12. 消息链路详解（数据流走读）

每一步都标注了运行的函数，便于端到端追踪链路。

### 12.1 节点加入（bootstrap）
```
Peer.start(boot)
  → KademliaNode.bootstrap([boot])      # discovery/kademlia.py
      → PING boot（UDP）
      → lookup(self)：多轮 FIND_NODE      # 迭代，O(log n)
      → RoutingTable.add(contacts)       # k桶逐渐填满
```
结果：加入者认识足够多的邻居以进行 gossip。

### 12.2 OPEN_TASK（生产 → 消费）
```
Peer.submit(target)                       # 仅提交者
  → seed_frontier()：expand_subtasks(root)
  → gossip.broadcast(OPEN_TASK)           # 去重(seen) → 转发给 fanout，ttl--
远端 Peer._on_gossip(OPEN_TASK)
  → Scheduler.add_open(task)              # 对 done/dead/claimed 去重
Peer.run() → Scheduler.next_task()        # 选离我最近(XOR)的任务
```

### 12.3 TASK_CLAIM（分布式租约）
```
Peer._work_on(task)
  → Scheduler.claim_local(task)           # lease_expires = now + lease
  → gossip.broadcast(TASK_CLAIM)
远端 Peer._on_gossip(TASK_CLAIM)
  → Scheduler.note_claim(task)            # 从 open 移除 → 去重
```

### 12.4 DEAD_END（共享剪枝）
```
solve_subtree(record_dead_end=_publish_dead_end)
  → 在浅层(≤ dead_end_share_depth)发生矛盾
  → _publish_dead_end：mark_dead(path) + gossip.broadcast(DEAD_END)
远端 Peer._on_gossip(DEAD_END) → Scheduler.mark_dead(path)
之后 DFS → _is_dead_end(path)==True → 跳过该子树
```

### 12.5 SOLUTION（全局停止）
```
solve_subtree → 完整棋盘
  → self.solution = board；gossip.broadcast(SOLUTION)；_stop.set()
远端 Peer._on_gossip(SOLUTION) → 重建棋盘；_stop.set()
  → 每个运行中的 DFS 内 should_stop() 触发
```

### 12.6 故障恢复（租约回收）
```
节点 C 持有任务 T（在每个节点上状态为 CLAIMED）时崩溃
  → C 的 lease_expires 过期
  → 任一 Peer.run() → next_task() → reclaim_expired()：T → OPEN
  → 某个存活者认领并重做 T                 # idle_limit 让节点存活足够久
```

---

## 13. 演示流程与预期输出

### A）`swarmsolve fault` — 容错
1. 以**穷举**模式启动 N 个进程，并设较大 `idle_limit`。
2. 在 `--kill-after` 后，父进程 `terminate()` 掉 `--kill-peer` 指定的节点。
3. 该节点租约过期；存活者接管其任务；所有任务完成。

预期（4 节点，杀 #2）：
```
>>> killed peer #2 (PID …)
Result
   killed peer #2 returned a result: no (as expected)
   surviving peers that finished: [0, 1, 3]
   swarm STILL solved the puzzle in ~13s despite the failure
```

### C）`swarmsolve dashboard` — 实时可视化
`rich.Live` 表格通过 `on_tick` 钩子刷新各节点计数（邻居 / open / claimed / dead /
done / nodes / found），随后打印最终的每节点报告与解出的棋盘。

### B）`swarmsolve benchmark` — 诚实加速
穷举搜索（统计所有解 / 验证唯一性）天然可并行。预期（难 9×9，4 节点，
`--node-delay 0.0012 --split-depth 4`）：
```
baseline : ~14.5s, 9309 nodes, 1 solutions
swarm    : ~8.7s wall, ~13k nodes across 4 peers
correctness OK: all 1 solution(s) covered exactly once
speedup  : ~1.67x (wall clock)
```
为何达不到理想的 4×：重复探索（异步 gossip）+ 负载不均——即第 8 节讨论的权衡。
**精确模式（第 14 节）同时消除了这两者。**

---

## 14. 精确模式：确定性单一负责人 + 虚拟节点（M5）

首解搜索几乎无法并行；真正的收益在**穷举**搜索（统计所有解 / 验证唯一性）。朴素的
工作窃取在异步 gossip 下会重复工作。*精确模式*让集群**确定性、零重复、计数精确**——
用 `--exclusive` 选择（`benchmark` 默认开启）。

### 工作原理
* **每个任务唯一负责人。** 任务 key 按 XOR 距离映射到唯一负责人；某节点仅当自己是
  负责人时才执行该任务（[`_owns`/`_owner_of`](../src/swarmsolve/peer.py)）。
* **可靠路由（DHT `put`）。** OPEN_TASK 不再 gossip，而是**经 TCP 直接投递给负责人**
  （ttl=0 不再转发），因此不会丢失——[`_route_open_task`](../src/swarmsolve/peer.py)。
* **静态分发。** 所有任务在开始时一次性播种（更细的前沿），关闭运行时工作窃取——这消除
  了"任务晚到 / 终止判定"问题（修复前实测 peers=2 仅得 29,982/45,475 个解，因某节点在
  任务到达前就空闲退出）。
* **虚拟节点（一致性哈希）。** 仅少数节点时，少量 ID 把 160 位空间分割得极不均（某节点
  拿到约 47% 的工作）。于是每个节点注册 `--vnodes` 个虚拟 ID；任务映射到最近的虚拟节点
  → 负载均衡。这正是第 5 章经典的负载均衡技术。

### 精确 vs 工作窃取

| 模式 | 重复工作 | 解计数 | 擅长 | 鲁棒性 |
|------|----------|--------|------|--------|
| 工作窃取（`--no-exclusive`） | 有（节点级） | 精确（dedup） | 不平衡树 | 容忍丢失/抖动 |
| 精确（`--exclusive`，默认） | 无 | 精确 | 平衡树 | 需要可靠投递 |

这是一个干净的**一致性 vs 可用性**讨论点：精确模式偏向精确/零重复；工作窃取偏向可用性/吞吐。

### 实测结果（用于报告）
在一个较宽的 9×9 上穷举统计所有解（97,605 节点，45,475 个解），`--node-delay 0.0002`，
精确模式 + 16 虚拟节点，单机基线 ≈ 28–30 秒：

| 节点数 | 墙钟时间 | vs 基线 | vs 单节点 | 各节点节点数分布 |
|--------|----------|---------|-----------|------------------|
| 1 | 34.8 s | 0.80× | 1.0× | — |
| 2 | 20.3 s | 1.50× | 1.71× | 均衡 |
| 4 | 11.6 s | 2.55× | 3.0× | 24k / 24k / 25k / 23k |

```mermaid
xychart-beta
    title "Exact-mode speedup vs peers (wide 9x9, exhaustive)"
    x-axis "peers" [1, 2, 4]
    y-axis "speedup (x)" 0 --> 4
    line [1, 2, 4]
    line [1.0, 1.71, 3.0]
    line [0.8, 1.5, 2.55]
```

_三条曲线（按声明顺序）：**理想线性**（1/2/4×）·**实测 vs 单节点**（1.0/1.71/3.0×）·
**实测 vs 单机基线**（0.8/1.5/2.55×）。vs-单节点曲线紧贴理想（4 节点 75% 效率）；
vs-基线曲线偏低仅因每进程的框架固定开销。_

每次运行都报告 **correctness OK：全部 45,475 个解恰好覆盖一次**（节点总数 97,260–97,512
≈ 基线 97,605 → 约 0% 重复）。与理想 4× 的差距来自框架固定开销（进程启动 + settle +
空闲尾巴），表现为单节点的 0.80×；*相对单节点*，4 节点 scaling 约 3.0×（75% 效率）。复现：
```bash
uv run swarmsolve gen --size 9 --clue-ratio 0.28 --seed 7 --out wide.txt
for p in 1 2 4; do uv run swarmsolve benchmark --file wide.txt --peers $p --node-delay 0.0002; done
```

### 25×25
求解器与尺寸无关（N = k²）。`swarmsolve gen --size 25` 瞬时构造合法 25×25；高线索实例靠
约束传播立即解出（`solve` → 1 节点，0.00 秒）。稀疏线索的 25×25 首解搜索是 NP 难（树爆炸），
因此我们用上面受控的*穷举*基准来量化并行加速，而非单次 25×25 首解运行。

---

## 15. 工作窃取：基于工作捐赠的动态负载均衡（M5+）

之前集群只有*被动*切分（`split_depth` 广播子任务）和*静态*归属（exclusive 模式），
缺少**真正的工作窃取**：空闲节点主动向忙节点索要工作。现已实现。

### 捐赠协议（点对点，TCP）
```
空闲节点                            忙节点（持有在跑任务）
   |                                      |
   |-- WORK_REQUEST (TCP, ttl=0) ------->|
   |                                      | _try_donate():
   |                                      |   把在跑任务切成子任务
   |                                      |   退役父任务（mark_done）
   |                                      |   本地保留 0 号子任务
   |<---- WORK_DONATE (子任务) -----------|   返回 N 号子任务
   |                                      |
纳入 open 池                              在 0 号子任务上继续
```
新增两个消息类型 [`WORK_REQUEST`](../src/swarmsolve/transport/messages.py) /
[`WORK_DONATE`](../src/swarmsolve/transport/messages.py)，**经 TCP 直接传输**
（不走 gossip）——它们是点对点协调。见
[`_on_work_steal`](../src/swarmsolve/peer.py) / [`_try_donate`](../src/swarmsolve/peer.py)
/ [`_maybe_steal`](../src/swarmsolve/peer.py)。

### 为何要退役父任务？
朴素捐赠（交出一个子任务但继续探索父任务）会导致父与子**重叠** → 重复工作。
`_try_donate` 改为退役父任务（`mark_done`）并只保留一个子任务，使父+子任务
无重叠地划分整棵子树。

### 租约续约（防止长任务被重复）
长 DFS 可能超过租约时长而被其他节点回收重做。现在
[`_tick_and_should_stop`](../src/swarmsolve/peer.py)（每个搜索节点调用一次）
会通过 [`Scheduler.renew`](../src/swarmsolve/tasks/scheduler.py) 续约所有在跑任务
的租约——忙节点不会被误判为死节点。

### 两种负载均衡策略及适用场景
| 策略 | 标志 | 重复工作 | 计数 | 适用 |
|------|------|----------|------|------|
| 精确（静态） | `--exclusive`（默认） | 无 | 精确 | 平衡树 |
| 工作窃取（动态） | `--work-stealing` | 有（重叠） | 精确（dedup） | 不平衡树、抖动 |

当搜索树**严重倾斜**（某分支远大于其他）、节点否则会空闲时，工作窃取是正解：
忙节点在运行时把工作让给空闲节点。树均匀且要零重复时，精确模式更好。复现：
```bash
uv run swarmsolve benchmark --file wide.txt --peers 4 --work-stealing
```
