# SwarmSolve 优化文档 — `feature/work-stealing-load-balance` 分支

> 本分支在 master 基础上实施了三大优化，共 3 个 commit，修改 5 个文件（+326 / -46 行）。
> 所有优化均通过现有测试，且 demo / benchmark / fault 演示正确性已验证。

---

## 优化总览

| # | 优化 | Commit | 核心思想 |
|---|------|--------|---------|
| 1 | **Work Stealing 负载均衡** | `ee16901` | deque + 双端 pop：自己从尾部取（LIFO），别人从头部偷（FIFO） |
| 2 | **混合死路回报** | `0bc6d93` | 死路直接 TCP 回报给 parent，不走 gossip 全网广播 |
| 3 | **动态节点加入/退出** | `e128192` | 周期性 bucket 刷新 + 优雅退出 + 租约续期 |

---

## 优化 1：Work Stealing 负载均衡

### 问题

master 分支的任务调度器用 `dict` 存储开放任务，`_pick_task()` 每次 O(N) 遍历找 XOR 距离最近的：

```python
# master — O(N) 选择
return min(self.open.values(), key=lambda t: xor_distance(self.id, t.key))
```

**问题**：
- O(N) 复杂度，任务多时变慢
- 只挑"最近的"，忽略其他任务
- **没有负载均衡**：空闲 peer 干等 gossip，不会主动从忙碌 peer 偷任务
- 不均衡搜索树下，一个 peer 累死（5000+ 节点），其他闲着（700 节点）

### 解决方案：Cilk/ForkJoin 经典 Work Stealing

#### 数据结构改造

```python
# scheduler.py — 之前
self.open: dict[str, Task] = {}

# 现在
self.task_deque: deque[Task] = deque()   # 双端队列，O(1) pop
self.task_map: dict[str, Task] = {}       # 侧表，仅用于去重/查找
```

#### 双端操作规则

```
deque: [老任务(大), ..., 新任务(小)]
         ↑                    ↑
      steal()             pop_own()
      (FIFO)              (LIFO)
```

| 操作 | 方向 | 语义 |
|------|------|------|
| `pop_own()` | 尾部 pop (LIFO) | 自己取任务：先做刚切分的小任务（缓存友好） |
| `steal()` | 头部 pop (FIFO) | 别人偷任务：偷走最老最大的任务（均衡负载） |

这是 **Cilk、Java ForkJoinPool、Go scheduler** 使用的经典规则：
- **Work-first 原则**：先做自己产生的任务（尾部），让别人偷走你还没碰的大任务（头部）
- **O(1) 复杂度**：不需要遍历，直接 pop

#### 新增消息类型

```python
# messages.py
STEAL_REQUEST = "STEAL_REQUEST"  # "给我一个任务"
STEAL_REPLY   = "STEAL_REPLY"    # "给你" (或空 = 没有)
```

#### Peer 空闲时主动偷取

```python
# peer.py — run() 循环中
if idle_rounds % 15 == 0 and self.dht.table.size() > 0:
    await self._try_steal()  # 每 ~0.5s 向随机邻居偷一个任务
```

```python
# peer.py — _try_steal()
async def _try_steal(self):
    victim = random.choice(self.dht.table.all_contacts())
    # 发 STEAL_REQUEST
    await self.transport.send_tcp(victim.host, victim.port, msg)
    # 等待 STEAL_REPLY
    stolen = await asyncio.wait_for(fut, timeout=1.0)
    if stolen:
        self.scheduler.add_open(Task.from_dict(stolen))
```

#### 收到 STEAL_REQUEST 时

```python
# peer.py — _on_steal_msg()
async def _on_steal_msg(self, msg, addr):
    if msg.type == MessageType.STEAL_REQUEST:
        stolen = self.scheduler.steal()  # 从头部 pop (FIFO)
        # 回复给请求者
```

### 效果

| 指标 | master | 优化后 |
|------|--------|--------|
| 任务选择复杂度 | O(N) | **O(1)** |
| 负载均衡机制 | 无 | **Work Stealing** |
| 空闲 peer 行为 | 干等 gossip | **主动偷任务** |

---

## 优化 2：混合死路回报

### 问题

master 分支发现死路后，**全网 gossip 广播**：

```python
# master — 全网广播死路
asyncio.create_task(
    self.gossip.broadcast(
        Message(MessageType.DEAD_END, self.id.hex(), {"path": path})
    )
)
```

**问题**：
- **流量浪费**：大量深层死路被广播给所有 peer，但绝大多数 peer 用不到
- **去重缓存膨胀**：每个 peer 的 `seen` 集合被死路消息填满
- **已有缓解但不够**：`dead_end_share_depth=3` 只限制深度，不限制范围

### 关键洞察

死路对其他 peer **没有参考价值**，因为：

1. **任务不重复**：Parent 在分发任务时已进行切分，同一路径不会同时派给两个人
2. **深层已截断**：既然该路径已确定是死路，Parent 就不会再往下衍生子任务
3. **不同值不影响**：其他 peer 在解同层不同值的任务（如 `0,2=3` vs `0,2=4`），该死路对其没有任何参考价值

### 解决方案：Child 直接 TCP 回报给 Parent

#### Task 新增 parent 字段

```python
# task.py
@dataclass
class Task:
    path: Path
    ...
    parent_host: str | None = None   # 新增：分发此任务的 parent 地址
    parent_port: int | None = None   # 新增
```

#### 切分任务时盖 parent 戳

```python
# peer.py — _route_open_task()
async def _route_open_task(self, task: Task) -> None:
    task.parent_host = self.host      # 我是 parent
    task.parent_port = self.port
    ...
```

#### 发现死路时直接发给 parent

```python
# peer.py — _publish_dead_end()
def _publish_dead_end(self, path: Path) -> None:
    self.scheduler.mark_dead(path)  # 本地标记

    # 1. 优先：直接 TCP 发给 parent（点对点）
    task = self.scheduler.claimed.get(tid)
    if task.parent_host:
        asyncio.create_task(
            self._report_dead_end_to_parent(path, task.parent_host, task.parent_port)
        )
        return

    # 2. Fallback：无 parent 的浅层死路才 gossip
    if len(path) <= self.dead_end_share_depth:
        asyncio.create_task(self.gossip.broadcast(...))
```

#### 新增消息类型

```python
# messages.py
DEAD_END_REPORT = "DEAD_END_REPORT"  # child → parent 点对点死路回报
```

#### Parent 收到回报后

```python
# peer.py — _on_dead_end_report()
async def _on_dead_end_report(self, msg, addr):
    path = [tuple(p) for p in msg.payload["path"]]
    self.scheduler.mark_dead(path)  # 标记死路，不再衍生
    # 不再 re-broadcast（parent 是唯一消费者）
```

### 效果

| 指标 | master | 优化后 |
|------|--------|--------|
| 死路传播范围 | 全网 gossip | **仅 parent** |
| 死路流量 | O(N × peers) | **O(1)** |
| 深层死路 | 有限分享 | **不传播**（只回报 parent） |

---

## 优化 3：动态节点加入/退出

### 问题

master 分支的节点动态性有三个问题：

| 问题 | 原因 |
|------|------|
| **新节点发现慢** | bootstrap 只在启动时执行一次，老节点不会主动发现新节点 |
| **退出有 10s 空窗** | Ctrl+C 直接杀进程，任务要等租约过期（10s）才被回收 |
| **长任务被误回收** | 租约 10s，但 DFS 可能跑超过 10s，中途被别人抢走 |

### 解决方案

#### 3a. 周期性 Bucket 刷新（加入感知）

```python
# peer.py — run() 循环中，每 ~5s 刷新一次
self._refresh_round += 1
if self._refresh_round % 167 == 0 and self.dht.table.size() > 0:
    asyncio.create_task(self._refresh_routing())

async def _refresh_routing(self):
    """重新 FIND_NODE(self)，发现新加入的 peer"""
    await self.dht.lookup(self.id)
```

**效果**：新节点加入后，老节点最多 5s 就能发现它，work stealing 能找到新节点。

#### 3b. 优雅退出（退出感知）

```python
# peer.py — graceful_leave()
async def graceful_leave(self):
    """退出前：归还任务 + 通知邻居 + 关闭"""
    # 1. 归还 open 任务（gossip 广播）
    for task in list(self.scheduler.task_deque):
        await self.gossip.broadcast(Message(OPEN_TASK, ...))

    # 2. 通知所有邻居移除自己（点对点 TCP）
    leave_msg = Message(LEAVE_ANNOUNCE, self.id.hex(), ttl=0)
    for c in self.dht.table.all_contacts():
        await self.transport.send_tcp(c.host, c.port, leave_msg)

    # 3. 关闭
    await self.transport.stop()
```

```python
# messages.py
LEAVE_ANNOUNCE = "LEAVE_ANNOUNCE"  # "我走了，把我从路由表删掉"
```

```python
# peer.py — 收到 LEAVE_ANNOUNCE
elif msg.type == MessageType.LEAVE_ANNOUNCE:
    leaver_id = NodeID.from_hex(msg.sender)
    self.dht.table.remove(leaver_id)  # 立即移除
```

#### 3c. SIGINT 捕获 → 优雅退出

```python
# cli.py — peer 命令
def _on_sigint():
    console.print("Received Ctrl+C, graceful leave...")
    leaving.set()
    p._stop.set()

loop.add_signal_handler(signal.SIGINT, _on_sigint)

# 如果是信号触发 → graceful_leave()
# 如果是正常结束 → stop()
if leaving.is_set():
    await p.graceful_leave()
else:
    await p.stop()
```

#### 3d. 租约缩短 + 心跳续租

```python
# scheduler.py — 租约从 10s 缩短到 5s
DEFAULT_LEASE_SECONDS = 5.0  # 快速检测崩溃

# peer.py — DFS 期间自动续租
def _tick_and_should_stop(self):
    for task in self.scheduler.claimed.values():
        # 剩余时间 < 一半时续租
        if task.lease_expires - time.time() < self.lease_seconds * 0.5:
            self.scheduler.renew(task)
    return self._stop.is_set()
```

**效果**：
- 崩溃检测：5s（之前 10s）
- 长任务不会被误回收（DFS 期间自动续租）

### 三个机制如何协作

```
节点加入：
  新节点 bootstrap → PING 老节点 → 老节点路由表加入新节点
  老节点每 5s FIND_NODE(self) → 发现新节点 → work stealing 能找到它

节点正常退出（Ctrl+C）：
  graceful_leave() → 归还 open 任务 (gossip) → LEAVE_ANNOUNCE 给所有邻居
  邻居收到 → 路由表移除 → 不再向它 steal/send

节点崩溃：
  租约 5s 过期 → reclaim_expired() → 任务回到 open 池 → 其他节点接手
  下次 bucket 刷新时 FIND_NODE 超时 → 路由表自然清理
```

### 效果

| 指标 | master | 优化后 |
|------|--------|--------|
| 新节点被发现 | 不可预测 | **~5s 内** |
| 正常退出空窗 | 10s | **0s**（立即归还+通知） |
| 崩溃检测 | 10s | **5s** |
| 长任务被误回收 | 可能 | **不会**（自动续租） |

---

## 文件改动明细

| 文件 | 改动 | 行数 |
|------|------|------|
| [`scheduler.py`](src/swarmsolve/tasks/scheduler.py) | dict → deque+map；新增 `pop_own()` / `steal()`；租约 10s→5s | +97 / -23 |
| [`peer.py`](src/swarmsolve/peer.py) | work stealing 逻辑；死路回报 parent；bucket 刷新；graceful_leave；租约续期 | +225 / -19 |
| [`task.py`](src/swarmsolve/tasks/task.py) | 新增 `parent_host` / `parent_port` 字段 + 序列化 | +9 |
| [`messages.py`](src/swarmsolve/transport/messages.py) | 新增 `STEAL_REQUEST` / `STEAL_REPLY` / `DEAD_END_REPORT` / `LEAVE_ANNOUNCE` | +7 |
| [`cli.py`](src/swarmsolve/cli.py) | peer 命令捕获 SIGINT → graceful_leave | +34 / -4 |

---

## 验证结果

| 测试 | 结果 |
|------|------|
| `pytest` 8 个单元测试 | ✅ 全通过 |
| `benchmark` 正确性 | ✅ `correctness OK: all 1 solution(s) covered exactly once` |
| `demo` 首解搜索 | ✅ 正常求解 |
| `fault` 容错演示 | ✅ 杀掉 peer 后仍求解 |

---

## 架构对比

```
master 分支:
  Submitter → gossip 广播任务 → peer 从 dict 选最近的 (O(N))
  死路 → gossip 全网广播
  节点退出 → 硬杀，等 10s 租约过期

feature/work-stealing-load-balance 分支:
  Submitter → gossip 广播任务 → peer 从 deque 尾部 pop (O(1))
  空闲 peer → 向随机邻居 STEAL_REQUEST → 从头部偷 (O(1))
  死路 → 直接 TCP 回报 parent (O(1))
  新节点 → 5s 内被 FIND_NODE 发现
  节点退出 → graceful_leave: 归还任务 + 通知邻居
  崩溃 → 5s 租约过期 → 自动回收
  长任务 → DFS 期间自动续租
```
