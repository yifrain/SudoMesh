# SwarmSolve 优化实现文档

本文档记录在 `feature/work-stealing-load-balance` 分支上实现的三个核心优化。

## 优化一：真正的工作窃取（Work-Stealing）负载均衡

### 问题背景

主分支的任务分发是"推"模型：submitter 一次性把任务切分好推给各个 peer。如果某个 peer 的子树特别难（分支多、
回溯深），它会成为瓶颈，而其他 peer 早早空闲。缺乏运行时的动态负载均衡。

### 解决方案

采用 Chase-Lev 风格的 **work-stealing deque**（双端队列）：

- 每个 peer 维护一个本地 deque 存放待探索的子任务
- 本地操作：从**队尾**（LIFO）push/pop，保持 DFS 的深度优先特性和缓存局部性
- 窃取操作：空闲 peer 从**队首**（FIFO）偷任务，偷到的是最"老"、最上层的任务（子树最大，价值最高）
- 通过 `STEAL_REQUEST`/`STEAL_REPLY` 消息在 peer 间传递任务

### 关键实现

- `Scheduler` 类（`scheduler.py`）：基于 `collections.deque` 的任务池，`pop_own()`（本地队尾）/ `steal()`（远程队首）均为 O(1)
- `Peer._try_steal()`：空闲时随机选一个 peer 发起窃取
- `Peer._on_steal_msg()`：收到 `STEAL_REQUEST` 时从 deque 队首取任务返回 `STEAL_REPLY`
- Peer 主循环：本地 deque 为空且开启 `--work-stealing` 时触发窃取，连续多次窃取失败则进入空闲等待

### 与现有机制的关系

这是对现有"静态推分发"的补充。submitter 仍然做初始切分，但之后各 peer 通过 work-stealing 动态平衡负载。

这是 Chase-Lev deque 的工作窃取实现。

---

## 优化二：混合死路上报（Hybrid Dead-End Reporting）

### 问题背景

死路（dead-end）信息如果全网 gossip，会产生大量冗余流量（很多 peer 根本不探索那条路径）。但如果完全不共享，
又会导致重复计算。需要在"共享"和"流量"之间找到平衡。

### 解决方案

采用混合策略：

- **点对点上报**：如果死路发生在某个"被分发的子任务"内，直接通过 TCP 向该任务的父节点（分发者）上报
- **浅层 gossip**：如果是本地探索到的浅层死路（深度 ≤ 阈值），才 gossip 给全网
- 深层死路直接丢弃（不值得占用带宽）

### 关键实现

- `Task.parent_host` / `Task.parent_port`：记录任务分发者的地址
- `Peer._publish_dead_end()`：根据死路类型选择上报方式（点对点 / 浅层 gossip / 丢弃）
- `Peer._on_dead_end_report()`：父节点接收子节点的点对点死路上报
- `MessageType.DEAD_END_REPORT`：点对点死路上报消息

### 与现有机制的关系

这是对现有 gossip 死路传播的补充。gossip 仍然用于浅层死路的全网共享，点对点用于精确的父子上报。

---

## 优化三：动态节点加入/离开（Dynamic Membership）

### 问题背景

节点可能随时加入或离开（崩溃、主动退出）。需要优雅处理这些情况，避免任务丢失或长时间卡顿。

### 解决方案

- **加入**：新节点通过 bootstrap 加入，周期性 FIND_NODE 刷新路由表发现新节点
- **优雅离开**：节点退出前广播 LEAVE_ANNOUNCE，把本地未完成任务重新分发
- **崩溃检测**：通过 lease 租约机制，超时未续约的任务被其他节点回收

### 关键实现

- `MessageType.LEAVE_ANNOUNCE`：节点离开公告
- `Peer.graceful_leave()`：优雅离开流程
- `Peer._refresh_routing()`：周期性路由表刷新
- lease 续约：`_tick_and_should_stop()` 中定期续约当前任务

### 与现有机制的关系

这是对静态成员假设的补充，使系统能应对真实的节点动态变化。

---

## 总结

三个优化互相配合：

- **工作窃取**解决负载不均
- **混合死路上报**减少冗余流量
- **动态成员**应对节点变化

三者共同提升了系统在真实分布式环境下的健壮性和效率。

---

## 实现细节与代码走读

本节配合具体代码讲解每个优化的实现逻辑，便于理解和口试讲解。

### 优化一：工作窃取 deque —— 代码走读

**1. 数据结构（`scheduler.py`）**

旧版用一个 `dict[str, Task]` 存开放任务，取任务要遍历找"离自己最近"的。新版改用 `collections.deque`：

```python
self.task_deque: deque[Task] = deque()   # 双端队列
self.task_map: dict[str, Task] = {}      # tid -> Task，O(1) 去重/查找
```

**2. 本地取任务：从队尾（LIFO）**

```python
def pop_own(self) -> Task | None:
    while self.task_deque:
        task = self.task_deque.pop()   # 从尾部取
        ...
```

**3. 被窃取：从队首（FIFO）**

```python
def steal(self) -> Task | None:
    while self.task_deque:
        task = self.task_deque.popleft()  # 从头部取
        ...
```

**4. 窃取流程（`peer.py`）**

空闲 peer 随机选一个邻居发 `STEAL_REQUEST`，对方从 deque 队首取任务回复 `STEAL_REPLY`。

### 优化二：混合死路上报 —— 代码走读

**1. 任务携带父节点地址（`task.py`）**

```python
@dataclass
class Task:
    parent_host: str | None = None
    parent_port: int | None = None
```

**2. 死路上报决策（`peer.py`）**

根据任务是否有父节点地址，选择点对点上报或浅层 gossip。

### 优化三：动态成员 —— 代码走读

**1. 优雅离开（`peer.py`）**

```python
async def graceful_leave(self):
    # 1. 重新分发本地任务
    # 2. 广播 LEAVE_ANNOUNCE
    # 3. 关闭 transport
```

**2. 周期性路由刷新（`peer.py`）**

```python
asyncio.create_task(self._refresh_routing())
```

### 关键点总结

工作窃取用 deque 实现 O(1) 的本地/远程取任务；混合死路上报在流量和共享间平衡；动态成员让系统健壮。

最后强调：这些优化都是对主分支的**增量增强**，不改变原有的核心协议和数据流。
