# SwarmSolve 优化文档 — `feature/work-stealing-load-balance` 分支

> 本分支在 master 基础上实施了三大优化，共 4 个 commit，修改 5 个文件（+326 / -46 行）。
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

### 1.1 问题分析

master 分支的任务调度器用 `dict` 存储开放任务，`_pick_task()` 每次 O(N) 遍历找 XOR 距离最近的：

```python
# master — scheduler.py
self.open: dict[str, Task] = {}

# master — peer.py _pick_task()
def _pick_task(self):
    pool = list(self.scheduler.open.values())   # O(N) 拷贝
    return min(pool, key=lambda t: xor_distance(self.id, t.key))  # O(N) 遍历
```

**三个问题**：

| 问题 | 影响 |
|------|------|
| **O(N) 复杂度** | 任务池大时（100+ 任务），每次选任务都要遍历全表 |
| **只挑最近的** | 忽略其他任务，导致某些任务永远没人做 |
| **无负载均衡** | 空闲 peer 干等 gossip 广播，不会主动从忙碌 peer 偷任务 |

实际演示中观察到的失衡现象：
```
Peer 9000:  777 nodes, 10 tasks    ← 轻松
Peer 9004: 5084 nodes, 10 tasks   ← 5 倍工作量！
```

### 1.2 解决方案：Cilk/ForkJoin 经典 Work Stealing

#### 1.2.1 数据结构改造

**核心思想**：用双端队列（deque）替代 dict，实现 O(1) 的任务获取和偷取。

```python
# scheduler.py — 改造后
class Scheduler:
    def __init__(self, self_id, lease_seconds=DEFAULT_LEASE_SECONDS):
        # deque 用于 O(1) pop（自己从尾部）和 steal（别人从头部）
        self.task_deque: deque[Task] = deque()
        # 侧表用于 O(1) 去重/状态查找（按 task id）
        self.task_map: dict[str, Task] = {}
        self.dead_ends: set[str] = set()
        self.claimed: dict[str, Task] = {}
        self.done: set[str] = set()
```

**为什么需要 `task_map` 侧表？**
- `deque` 不支持 O(1) 按键查找
- 去重检查（`add_open` 时判断是否已存在）需要 O(1)
- `mark_dead` / `mark_done` 时需要快速判断任务状态

#### 1.2.2 双端操作规则

```
deque: [老任务(大), T2, T3, T4, 新任务(小)]
         ↑                          ↑
      steal()                   pop_own()
      (头部 popleft)             (尾部 pop)
      (FIFO)                    (LIFO)
```

| 操作 | 方向 | 复杂度 | 语义 |
|------|------|--------|------|
| `pop_own()` | 尾部 pop (LIFO) | O(1) | 自己取任务：先做刚切分的小任务（缓存友好） |
| `steal()` | 头部 popleft (FIFO) | O(1) | 别人偷任务：偷走最老最大的任务（均衡负载） |
| `add_open()` | 尾部 append | O(1) | 新任务入队 |

**为什么是 LIFO + FIFO？** 这是 Cilk、Java ForkJoinPool、Go scheduler 的经典规则：

- **Work-first 原则**：peer 切分任务后，新子任务在尾部，自己先做（LIFO）→ 刚切分的数据还在 CPU 缓存里，速度快
- **Steal-old 原则**：偷取者从头部偷（FIFO）→ 偷走的是最老、最大的任务 → 一次偷走最大的块，减少偷取次数

#### 1.2.3 实现细节

**`pop_own()` — 自己取任务（从尾部）**：

```python
def pop_own(self) -> Task | None:
    """从尾部 pop（LIFO）。跳过已死/已完成/被认领的任务。"""
    while self.task_deque:
        task = self.task_deque.pop()        # O(1) 尾部弹出
        tid = task.id
        if not self._is_pickable(task):      # 检查是否已死/已完成/被认领
            self.task_map.pop(tid, None)     # 惰性清理
            continue
        self.task_map.pop(tid, None)
        return task
    return None
```

**`steal()` — 别人偷任务（从头部）**：

```python
def steal(self) -> Task | None:
    """从头部 popleft（FIFO）。偷取者拿走最老最大的任务。"""
    while self.task_deque:
        task = self.task_deque.popleft()    # O(1) 头部弹出
        tid = task.id
        if not self._is_pickable(task):
            self.task_map.pop(tid, None)
            continue
        self.task_map.pop(tid, None)
        return task
    return None
```

**`_is_pickable()` — 惰性清理**：

```python
def _is_pickable(self, task: Task) -> bool:
    """任务可选的条件：未死、未完成、未被活跃认领"""
    tid = task.id
    if tid in self.dead_ends or tid in self.done:
        return False
    existing = self.claimed.get(tid)
    if existing and existing.owner is not None and existing.lease_active():
        return False
    return True
```

> **惰性清理设计**：`mark_dead` / `mark_done` 时不遍历 deque 删除（O(N)），而是只更新 set/dict。等 `pop_own` / `steal` 时遇到无效任务才跳过。这是典型的"标记-跳过"策略。

#### 1.2.4 新增消息类型

```python
# messages.py
STEAL_REQUEST = "STEAL_REQUEST"  # "给我一个任务"
STEAL_REPLY   = "STEAL_REPLY"    # "给你" (payload 空 = 没有)
```

**为什么 STEAL 消息不走 gossip？**
- Gossip 有去重（`seen` 集合），但 steal 是点对点请求/响应，每次都是新的
- Gossip 有 TTL 转发，但 steal 不需要转发（直接请求/响应）
- 所以在 `_dispatch()` 中单独路由：

```python
# peer.py — _dispatch()
elif msg.type in (MessageType.STEAL_REQUEST, MessageType.STEAL_REPLY):
    await self._on_steal_msg(msg, addr)  # 点对点处理，不走 gossip
```

#### 1.2.5 Peer 空闲时主动偷取

**触发时机**：`run()` 循环中，空闲时每 ~0.5s 发起一次偷取：

```python
# peer.py — run() 循环
if task is None:  # 空闲
    idle_rounds += 1
    # Work stealing: 每 ~0.5s（15 × 0.03s）向随机邻居偷一个任务
    if idle_rounds % 15 == 0 and self.dht.table.size() > 0:
        steal_rounds += 1
        await self._try_steal()
```

**`_try_steal()` — 偷取流程**：

```python
async def _try_steal(self):
    """空闲 peer：向随机邻居请求一个任务"""
    import random as _random
    peers = self.dht.table.all_contacts()
    if not peers:
        return
    victim = _random.choice(peers)          # 1. 随机选一个邻居
    self.steals_attempted += 1

    # 2. 构造 STEAL_REQUEST，带上自己的回信地址
    msg = Message(
        MessageType.STEAL_REQUEST, self.id.hex(),
        {"host": self.host, "port": self.port}, ttl=0,
    )
    # 3. 创建 Future 等待回复
    fut = asyncio.get_running_loop().create_future()
    self._pending_steals[msg.msg_id] = fut

    # 4. 发送请求
    await self.transport.send_tcp(victim.host, victim.port, msg)

    # 5. 等待回复（1s 超时）
    try:
        stolen = await asyncio.wait_for(fut, timeout=1.0)
    except asyncio.TimeoutError:
        self._pending_steals.pop(msg.msg_id, None)
        return

    # 6. 收到任务 → 加入本地 deque
    if stolen is not None:
        self.steals_succeeded += 1
        task = Task.from_dict(stolen)
        self.scheduler.add_open(task)
        self.log(f"[{self.id.short()}] stole task (depth={task.depth}) "
                 f"from [{victim.node_id.short()}]")
```

#### 1.2.6 收到 STEAL_REQUEST 时

**`_on_steal_msg()` — 被偷者的处理**：

```python
async def _on_steal_msg(self, msg, addr):
    if msg.type == MessageType.STEAL_REQUEST:
        # 从自己 deque 的头部 pop 一个任务（FIFO，最老最大的）
        stolen = self.scheduler.steal()
        # 构造回复
        reply_payload = {"task": stolen.to_dict()} if stolen else {}
        host = msg.payload.get("host", addr[0])
        port = msg.payload.get("port", addr[1])
        # 直接 TCP 回复给请求者
        await self.transport.send_tcp(
            host, port,
            Message(MessageType.STEAL_REPLY, self.id.hex(),
                    reply_payload, ttl=0),
        )
    elif msg.type == MessageType.STEAL_REPLY:
        # 收到回复 → 解析 Future
        fut = self._pending_steals.pop(msg.msg_id, None)
        if fut and not fut.done():
            task_dict = msg.payload.get("task")
            fut.set_result(task_dict)  # None 表示对方没有任务
```

#### 1.2.7 完整的偷取时序

```
Peer A (空闲)                    Peer B (忙碌)
    │                                │
    │  1. STEAL_REQUEST              │
    │  (带 A 的回信地址)             │
    │ ─────────────────────────────> │
    │                                │  2. scheduler.steal()
    │                                │     从 deque 头部 popleft
    │                                │
    │  3. STEAL_REPLY                │
    │  (带偷来的 task 或空)          │
    │ <───────────────────────────── │
    │                                │
    │  4. add_open(task)             │
    │     加入本地 deque 尾部        │
    │  5. 下次 pop_own() 取出执行    │
```

### 1.3 效果

| 指标 | master | 优化后 |
|------|--------|--------|
| 任务选择复杂度 | O(N) | **O(1)** |
| 负载均衡机制 | 无（只挑最近的） | **Work Stealing**（空闲主动偷） |
| 空闲 peer 行为 | 干等 gossip | **主动偷任务** |
| 不均衡树处理 | 差（一个 peer 累死） | **好**（空闲 peer 从忙碌 peer 偷） |

---

## 优化 2：混合死路回报

### 2.1 问题分析

master 分支发现死路后，**全网 gossip 广播**：

```python
# master — peer.py _publish_dead_end()
def _publish_dead_end(self, path: Path) -> None:
    if len(path) > self.dead_end_share_depth:  # 只分享浅层
        return
    self.scheduler.mark_dead(path)
    asyncio.create_task(
        self.gossip.broadcast(                    # 全网广播！
            Message(MessageType.DEAD_END, self.id.hex(), {"path": path})
        )
    )
```

**三个问题**：

| 问题 | 影响 |
|------|------|
| **流量浪费** | 大量死路被广播给所有 peer，但绝大多数 peer 用不到 |
| **去重缓存膨胀** | 每个 peer 的 gossip `seen` 集合被死路消息填满（容量 4096） |
| **深层死路被丢弃** | `dead_end_share_depth=3` 限制深度，但不限制范围 |

### 2.2 关键洞察

死路对其他 peer **没有参考价值**，因为：

1. **任务不重复**：Parent 在分发任务时已进行切分，同一路径不会同时派给两个人
2. **深层已截断**：既然该路径已确定是死路，Parent 就不会再往下衍生子任务
3. **不同值不影响**：其他 peer 在解同层不同值的任务（如 `0,2=3` vs `0,2=4`），该死路对其没有任何参考价值

**结论**：死路只需告诉 Parent（切分此任务的节点），不需要全网广播。

### 2.3 解决方案：Child 直接 TCP 回报给 Parent

#### 2.3.1 Task 新增 parent 字段

```python
# task.py
@dataclass
class Task:
    path: Path
    status: TaskStatus = TaskStatus.OPEN
    owner: str | None = None
    lease_expires: float = 0.0
    # 新增：Parent peer 的地址（切分此任务的节点）
    parent_host: str | None = None
    parent_port: int | None = None
    depth: int = field(init=False, default=0)
```

**序列化也要更新**，确保 parent 地址随 Task 一起传输：

```python
def to_dict(self) -> dict:
    return {
        "path": self.path,
        "status": self.status.value,
        "owner": self.owner,
        "lease_expires": self.lease_expires,
        "parent_host": self.parent_host,   # 新增
        "parent_port": self.parent_port,   # 新增
    }

@classmethod
def from_dict(cls, d: dict) -> "Task":
    return cls(
        path=[tuple(p) for p in d["path"]],
        status=TaskStatus(d.get("status", "OPEN")),
        owner=d.get("owner"),
        lease_expires=d.get("lease_expires", 0.0),
        parent_host=d.get("parent_host"),   # 新增
        parent_port=d.get("parent_port"),   # 新增
    )
```

#### 2.3.2 切分任务时盖 parent 戳

**关键改动**：`_route_open_task()` 在发布任务时，把自己的地址写入 Task：

```python
# peer.py — _route_open_task()
async def _route_open_task(self, task: Task) -> None:
    # 盖 parent 戳：我是切分此任务的节点
    task.parent_host = self.host
    task.parent_port = self.port
    # 然后正常发布（gossip 广播或 TCP 直送）
    payload = {"task": task.to_dict()}
    if self.exclusive and self.owner_roster:
        owner = self._owner_of(task)
        if owner.node_id == self.id:
            self.scheduler.add_open(task)
        else:
            await self.transport.send_tcp(
                owner.host, owner.port,
                Message(MessageType.OPEN_TASK, self.id.hex(), payload, ttl=0),
            )
    else:
        self.scheduler.add_open(task)
        await self.gossip.broadcast(
            Message(MessageType.OPEN_TASK, self.id.hex(), payload)
        )
```

**效果**：每个被分发的 Task 都带着 parent 地址在网络中流动。认领者做完任务后，可以通过 parent 地址直接回报。

#### 2.3.3 发现死路时的混合策略

**`_publish_dead_end()` — 改造后**：

```python
def _publish_dead_end(self, path: Path) -> None:
    """混合死路回报策略：
    1. 如果死路对应的 Task 有 parent → 直接 TCP 回报给 parent（点对点）
    2. 否则（共享池任务的浅层死路）→ fallback gossip 广播
    """
    tid = path_repr(path)
    # 1. 本地标记
    self.scheduler.mark_dead(path)

    # 2. 查找此 path 对应的 Task，看是否有 parent
    claimed = self.scheduler.claimed.get(tid)
    parent_host = getattr(claimed, "parent_host", None) if claimed else None
    parent_port = getattr(claimed, "parent_port", None) if claimed else None

    # 3. 有 parent → 直接 TCP 回报（O(1) 流量）
    if parent_host and parent_port:
        asyncio.create_task(
            self._report_dead_end_to_parent(path, parent_host, parent_port)
        )
        return

    # 4. 无 parent → 浅层死路才 gossip（fallback）
    if len(path) > self.dead_end_share_depth:
        return
    asyncio.create_task(
        self.gossip.broadcast(
            Message(MessageType.DEAD_END, self.id.hex(), {"path": path})
        )
    )
```

**`_report_dead_end_to_parent()` — 点对点 TCP 回报**：

```python
async def _report_dead_end_to_parent(self, path: Path, host: str, port: int) -> None:
    """直接 TCP 发给 parent，不走 gossip"""
    await self.transport.send_tcp(
        host, port,
        Message(
            MessageType.DEAD_END_REPORT, self.id.hex(),
            {"path": path, "host": self.host, "port": self.port},
            ttl=0,  # 不转发
        ),
    )
```

#### 2.3.4 新增消息类型

```python
# messages.py
DEAD_END_REPORT = "DEAD_END_REPORT"  # child → parent 点对点死路回报
```

**在 `_dispatch()` 中单独路由**（不走 gossip 去重）：

```python
# peer.py — _dispatch()
elif msg.type == MessageType.DEAD_END_REPORT:
    # 点对点死路回报，绕过 gossip
    await self._on_dead_end_report(msg, addr)
```

#### 2.3.5 Parent 收到回报后

**`_on_dead_end_report()` — Parent 的处理**：

```python
async def _on_dead_end_report(self, msg: Message, addr) -> None:
    """Parent 收到 child 的死路回报。
    标记本地死路，不再 re-broadcast（parent 是唯一消费者）。
    """
    path = [tuple(p) for p in msg.payload["path"]]
    self.scheduler.mark_dead(path)  # 本地标记
    child_id = msg.sender[:8]
    self.log(f"[{self.id.short()}] dead-end report from [{child_id}] "
             f"path={path_repr(path)[:40]}")
    # 不再 re-broadcast！parent 是唯一需要知道的节点
```

#### 2.3.6 完整的回报时序

```
Parent (切分者)                  Child (认领者)
    │                                │
    │  1. OPEN_TASK (带 parent 戳)   │
    │ ─────────────────────────────> │
    │                                │  2. DFS 探索子树
    │                                │     所有候选都矛盾
    │                                │
    │  3. DEAD_END_REPORT (TCP)      │
    │  (path = 死路前缀)             │
    │ <───────────────────────────── │
    │                                │
    │  4. mark_dead(path)            │
    │     不再衍生此路径的子任务      │
    │     不再 re-broadcast          │
```

**对比 master 的 gossip 广播**：

```
master:
  Child → gossip DEAD_END → 所有 peer 收到（但没人需要）

优化后:
  Child → TCP DEAD_END_REPORT → 仅 Parent 收到
  （其他 peer 不受打扰，gossip seen 缓存不被污染）
```

### 2.4 效果

| 指标 | master | 优化后 |
|------|--------|--------|
| 死路传播范围 | 全网 gossip | **仅 parent** |
| 死路流量 | O(N × peers) | **O(1)** |
| 深层死路 | 有限分享（depth ≤ 3） | **不传播**（只回报 parent） |
| gossip seen 缓存 | 被死路填满 | **干净**（只用于真正需要广播的消息） |

---

## 优化 3：动态节点加入/退出

### 3.1 问题分析

master 分支的节点动态性有三个问题：

| 问题 | 原因 | 影响 |
|------|------|------|
| **新节点发现慢** | bootstrap 只在启动时执行一次，老节点不会主动发现新节点 | 新节点加入后无法参与 work stealing |
| **退出有 10s 空窗** | Ctrl+C 直接杀进程，任务要等租约过期（10s）才被回收 | 退出者的任务卡住 10s |
| **长任务被误回收** | 租约 10s，但 DFS 可能跑超过 10s | 正在做的任务被别人抢走 |

### 3.2 解决方案

#### 3.2.1 周期性 Bucket 刷新（加入感知）

**问题**：Kademlia 的 `bootstrap()` 只在启动时执行一次。新节点 PING 老节点后，老节点的路由表里有了新节点，但老节点不会主动去"发现"新节点。如果新节点不主动联系，老节点永远不知道它存在。

**解决**：在 `run()` 循环中周期性执行 `FIND_NODE(self)`：

```python
# peer.py — __init__() 中新增计数器
self._refresh_round = 0

# peer.py — run() 循环中
while not self._stop.is_set():
    self._refresh_round += 1
    # 每 ~5s（167 × 0.03s ≈ 5s）刷新一次路由表
    if self._refresh_round % 167 == 0 and self.dht.table.size() > 0:
        asyncio.create_task(self._refresh_routing())
    ...

async def _refresh_routing(self) -> None:
    """重新 FIND_NODE(self)，发现新加入的 peer，清理失效 peer。"""
    try:
        await self.dht.lookup(self.id)
    except Exception:
        pass
```

**`lookup()` 的作用**：

```python
# kademlia.py — lookup()
async def lookup(self, target: NodeID, *, alpha: int = 3) -> list[Contact]:
    """迭代 FIND_NODE：每轮并行查 α=3 个最近节点，6 轮收敛。"""
    queried = set()
    shortlist = self.table.closest(target)
    for _ in range(6):
        batch = [c for c in shortlist if c.node_id not in queried][:alpha]
        if not batch:
            break
        # 并行发 FIND_NODE
        futures = []
        for c in batch:
            msg = self._send(c.host, c.port, FIND_NODE, {"target": target.hex()})
            fut = asyncio.get_running_loop().create_future()
            self._pending[msg.msg_id] = fut
            futures.append(fut)
        await asyncio.wait_for(asyncio.gather(*futures), timeout=1.0)
        shortlist = self.table.closest(target)
    return shortlist
```

**效果**：
- 新节点加入后，老节点最多 5s 就能通过 `FIND_NODE` 发现它
- 失效节点在 `FIND_NODE` 超时后自然从候选列表中消失

#### 3.2.2 优雅退出（退出感知）

**问题**：master 的 `stop()` 直接关闭 transport，不通知任何人。正在做的任务要等租约过期（5s）才被回收。

**解决**：新增 `graceful_leave()`，退出前归还任务 + 通知邻居：

```python
# peer.py — graceful_leave()
async def graceful_leave(self) -> None:
    """优雅退出：归还任务 + 通知邻居 + 关闭。"""
    self._stop.set()
    self.log(f"[{self.id.short()}] graceful leave: handing off "
             f"{len(self.scheduler.task_deque)} open tasks")

    # 1. 归还 open 任务：gossip 广播，让其他 peer 认领
    for task in list(self.scheduler.task_deque):
        payload = {"task": task.to_dict()}
        try:
            await self.gossip.broadcast(
                Message(MessageType.OPEN_TASK, self.id.hex(), payload)
            )
        except Exception:
            pass

    # 2. 通知所有邻居移除自己（点对点 TCP）
    leave_msg = Message(MessageType.LEAVE_ANNOUNCE, self.id.hex(), ttl=0)
    for c in self.dht.table.all_contacts():
        try:
            await self.transport.send_tcp(c.host, c.port, leave_msg)
        except Exception:
            pass

    # 3. 关闭 transport
    await self.transport.stop()
```

**新增消息类型**：

```python
# messages.py
LEAVE_ANNOUNCE = "LEAVE_ANNOUNCE"  # "我走了，把我从路由表删掉"
```

**收到 LEAVE_ANNOUNCE 的处理**：

```python
# peer.py — _dispatch()
elif msg.type == MessageType.LEAVE_ANNOUNCE:
    leaver_id = NodeID.from_hex(msg.sender)
    self.dht.table.remove(leaver_id)  # 立即从路由表移除
    self.log(f"[{self.id.short()}] peer [{leaver_id.short()}] left; "
             f"peers={self.dht.table.size()}")
```

#### 3.2.3 SIGINT 捕获 → 优雅退出

**问题**：用户按 Ctrl+C 时，Python 默认抛 `KeyboardInterrupt`，`asyncio.run()` 直接终止，`graceful_leave()` 没机会执行。

**解决**：在 CLI `peer` 命令中注册信号处理器：

```python
# cli.py — peer 命令的 main()
async def main():
    p = Peer(...)
    await p.start(boot)
    ...

    # 注册 SIGINT/SIGTERM 处理器
    loop = asyncio.get_running_loop()
    leaving = asyncio.Event()

    def _on_sigint():
        console.print("[yellow]Received Ctrl+C, graceful leave...[/]")
        leaving.set()
        p._stop.set()  # 同时打断工作循环

    import signal
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _on_sigint)
        except NotImplementedError:
            pass  # Windows 不支持

    # 并行等待：工作完成 OR 信号触发
    sol_task = asyncio.create_task(p.run())
    await asyncio.wait(
        {sol_task, asyncio.create_task(leaving.wait())},
        return_when=asyncio.FIRST_COMPLETED,
    )

    # 根据退出原因选择退出方式
    if leaving.is_set():
        await p.graceful_leave()  # 优雅退出
    else:
        sol = sol_task.result()
        if sol:
            console.print("[green]Solution:[/]")
            console.print(str(sol))
        await p.stop()  # 正常停止
```

#### 3.2.4 租约缩短 + 心跳续租

**问题**：租约 10s 太长，崩溃后要等 10s 才回收。但如果缩短到 5s，长 DFS 可能中途过期被抢走。

**解决**：缩短租约 + DFS 期间自动续租。

**缩短租约**：

```python
# scheduler.py
DEFAULT_LEASE_SECONDS = 5.0  # 之前是 10.0，缩短一半
```

**DFS 期间自动续租**：

利用 `should_stop()` 钩子——DFS 每个节点都会调用它，是免费的续租检查点：

```python
# peer.py — _tick_and_should_stop()
def _tick_and_should_stop(self) -> bool:
    """每个 DFS 节点调用一次：刷新 dashboard + 续租。"""
    self._maybe_tick()
    # 续租：如果当前任务的租约剩余时间 < 一半，续租
    for task in self.scheduler.claimed.values():
        if task.owner == self.id.hex() and \
           task.lease_expires - time.time() < self.lease_seconds * 0.5:
            self.scheduler.renew(task)  # 续租
            break
    return self._stop.is_set()
```

**`renew()` 的实现**：

```python
# scheduler.py
def renew(self, task: Task, now=None):
    now = now or time.time()
    task.lease_expires = now + self.lease_seconds  # 重置过期时间
```

**续租逻辑图**：

```
租约 5s，DFS 期间每 ~0.15s 检查一次：

t=0.0s  claim_local()  → lease_expires = 5.0s
t=0.5s  should_stop()  → 剩余 4.5s > 2.5s → 不续租
t=1.0s  should_stop()  → 剩余 4.0s > 2.5s → 不续租
...
t=2.6s  should_stop()  → 剩余 2.4s < 2.5s → 续租！lease_expires = 7.6s
t=3.0s  should_stop()  → 剩余 4.6s > 2.5s → 不续租
...
t=5.2s  should_stop()  → 剩余 2.4s < 2.5s → 续租！lease_expires = 10.2s
```

**效果**：
- peer 正常工作时，租约永远不过期（自动续租）
- peer 崩溃后，最多 5s 租约过期 → `reclaim_expired()` 回收任务

### 3.3 三个机制如何协作

```
═══ 节点加入 ═══

新节点启动
    │
    ├─ bootstrap(contacts) → PING 老节点
    │       │
    │       └─ 老节点 handle(PING) → table.add(新节点)  ← 老节点学到新节点
    │
    └─ lookup(self) → FIND_NODE → 填充自己的 buckets

老节点（每 5s）:
    └─ _refresh_routing() → lookup(self)
            │
            └─ FIND_NODE 发给已知 peer
                    │
                    └─ 如果新节点在别人的路由表里 → 返回给老节点
                            │
                            └─ 老节点 table.add(新节点)

结果: 新节点 5s 内被全网发现，work stealing 能找到它

═══ 节点正常退出（Ctrl+C）═══

用户按 Ctrl+C
    │
    └─ _on_sigint() → leaving.set() + p._stop.set()
            │
            └─ run() 循环退出
                    │
                    └─ graceful_leave()
                            │
                            ├─ 1. 归还 open 任务 → gossip OPEN_TASK
                            │       └─ 其他 peer 收到 → add_open → 可认领
                            │
                            ├─ 2. LEAVE_ANNOUNCE → TCP 发给所有邻居
                            │       └─ 邻居 table.remove(退出者)
                            │
                            └─ 3. transport.stop()

结果: 任务立即归还，邻居立即知晓，0s 空窗

═══ 节点崩溃（kill -9 / 断电）═══

节点崩溃
    │
    └─ 无机会执行 graceful_leave

其他节点（每 0.03s 检查一次）:
    └─ reclaim_expired()
            │
            └─ 发现崩溃者的租约过期（5s 后）
                    │
                    └─ 任务回到 open 池 → 其他 peer 可认领

其他节点（每 5s 刷新一次）:
    └─ _refresh_routing() → FIND_NODE 发给崩溃者
            │
            └─ 超时 1s → 崩溃者不回复
                    │
                    └─ 路由表自然清理（不被添加到 shortlist）

结果: 5s 内任务被回收，路由表自然清理
```

### 3.4 效果

| 指标 | master | 优化后 |
|------|--------|--------|
| 新节点被发现 | 不可预测（靠偶然 FIND_NODE） | **~5s 内**（周期性刷新） |
| 正常退出空窗 | 10s（租约过期） | **0s**（立即归还+通知） |
| 崩溃检测 | 10s | **5s** |
| 长任务被误回收 | 可能（租约过期） | **不会**（DFS 期间自动续租） |

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
═══ master 分支 ═══

任务分发:
  Submitter → gossip 广播任务 → peer 从 dict 选最近的 (O(N))
  空闲 peer → 干等 gossip，不主动获取任务

死路传播:
  发现死路 → gossip 全网广播 → 所有 peer 收到（但多数不需要）

节点动态性:
  新节点 → bootstrap 一次，之后被动等待
  节点退出 → 硬杀，等 10s 租约过期
  长任务 → 可能被租约过期误回收

═══ feature/work-stealing-load-balance 分支 ═══

任务分发:
  Submitter → gossip 广播任务 → peer 从 deque 尾部 pop (O(1))
  空闲 peer → 向随机邻居 STEAL_REQUEST → 从头部偷 (O(1))

死路传播:
  发现死路 → 直接 TCP 回报 parent (O(1))
  无 parent 的浅层死路 → fallback gossip

节点动态性:
  新节点 → 5s 内被 FIND_NODE 发现（周期性刷新）
  节点退出 → graceful_leave: 归还任务 + 通知邻居（0s 空窗）
  崩溃 → 5s 租约过期 → 自动回收
  长任务 → DFS 期间自动续租（不会误回收）
```
