# SwarmSolve — 詳細架構（繁體中文）

本文是 SwarmSolve 的**程式碼層級**走讀。專案概覽、快速開始與分工請見根目錄
[`README.md`](../README.md)。

---

## 1. 分層架構

```
┌──────────────────────────────────────────────────────────────┐
│ Peer（一個作業系統行程 / 一台機器）                            │
│                                                                │
│  求解層 Solver     約束傳播 + DFS              solver/         │
│  任務層 Task       切分 / 去重 / 租約 / 再平衡  tasks/         │
│  傳播層 Gossip     流行病式擴散 + 去重          gossip/        │
│  探索層 Discovery  Kademlia DHT（XOR, k桶）     discovery/     │
│  傳輸層 Transport  TCP（任務）+ UDP（探索）     transport/     │
└──────────────────────────────────────────────────────────────┘
        ▲                                                  ▲
        └──────────── 本機/區域網路上的 TCP / UDP ─────────┘
```

| 層 | 檔案 | 課程知識點 |
|----|------|-----------|
| 傳輸 | [`transport/messages.py`](../src/swarmsolve/transport/messages.py)、[`transport.py`](../src/swarmsolve/transport/transport.py) | 第2章 — TCP、訊息 |
| 探索 | [`discovery/node_id.py`](../src/swarmsolve/discovery/node_id.py)、[`routing.py`](../src/swarmsolve/discovery/routing.py)、[`kademlia.py`](../src/swarmsolve/discovery/kademlia.py) | **第6章 — Kademlia** |
| 傳播 | [`gossip/gossip.py`](../src/swarmsolve/gossip/gossip.py) | 第2章 Gossip + 第7章 BubbleStorm |
| 任務 | [`tasks/task.py`](../src/swarmsolve/tasks/task.py)、[`scheduler.py`](../src/swarmsolve/tasks/scheduler.py) | 第5章 負載平衡 + 容錯 |
| 求解 | [`solver/board.py`](../src/swarmsolve/solver/board.py)、[`search.py`](../src/swarmsolve/solver/search.py) | 應用核心 |
| 編排 | [`peer.py`](../src/swarmsolve/peer.py)、[`cli.py`](../src/swarmsolve/cli.py) | 黏合 + 演示 |

---

## 2. 儲存庫結構

```
src/swarmsolve/
├── transport/
│   ├── messages.py   # Message 資料類別 + MessageType 列舉 + encode/decode
│   └── transport.py  # asyncio TCP 伺服器 + UDP 端點
├── discovery/
│   ├── node_id.py    # 160 位元 NodeID、XOR 距離、task_key()
│   ├── routing.py    # k桶 RoutingTable、Contact
│   └── kademlia.py   # PING/PONG/FIND_NODE、迭代查找、bootstrap
├── gossip/
│   └── gossip.py     # 推式 gossip：seen 去重 + TTL 扇出
├── tasks/
│   ├── task.py       # Task（一棵子樹）+ TaskStatus + path_repr/task_key
│   └── scheduler.py  # open/claimed/dead/done 集合、租約、依 XOR 選任務
├── solver/
│   ├── board.py      # 位元遮罩棋盤 + 約束傳播
│   └── search.py     # DFS、子樹切分、enumerate、node_delay
├── puzzles.py        # 解析 / 產生題目（瞬時建構完整解再挖空）
├── peer.py           # Peer：串起所有層 + 工作迴圈
└── cli.py            # gen / solve / demo / benchmark / dashboard / fault / peer
```

---

## 3. 逐模組走讀

### 3.1 傳輸層 — `transport/`

**`messages.py`** 定義唯一的線上型別 [`Message`](../src/swarmsolve/transport/messages.py)
（`type, sender, payload, msg_id, ttl, ts`）與 [`MessageType`](../src/swarmsolve/transport/messages.py)
列舉。作業要求的三類*應用*訊息是 `OPEN_TASK`、`DEAD_END`、`SOLUTION`；其餘是協調
（`TASK_CLAIM`、`TASK_DONE`）與探索（`PING/PONG/FIND_NODE/FIND_NODE_REPLY`）。序列化採用
換行分隔的 JSON（`encode`/`decode`），便於演示除錯，日後可換成 msgpack。

**`transport.py`** — [`Transport`](../src/swarmsolve/transport/transport.py) 持有一個
`asyncio` **TCP 伺服器**（每連線一請求，用於任務/解負載）與一個 **UDP 端點**（資料報，
用於 Kademlia）。統一回呼 `handler(msg, addr, kind)` 接收所有訊息；`send_tcp` 在對端
離線時回傳 `False`（用於偵測故障），`send_udp` 則盡力而為。

### 3.2 探索層（Kademlia） — `discovery/`

**`node_id.py`** — [`NodeID`](../src/swarmsolve/discovery/node_id.py) 是 160 位元 ID。
關鍵函式：`xor_distance`、`shared_prefix_len`（桶索引），以及最關鍵的
[`task_key`](../src/swarmsolve/discovery/node_id.py)：把搜尋樹路徑雜湊進與節點 ID **同一個**
XOR 空間——這是 Solver 與 DHT 之間的橋樑。

**`routing.py`** — [`RoutingTable`](../src/swarmsolve/discovery/routing.py) 持有
`ID_BITS` 個 k桶（`K=8`）。`add` 在桶內採 LRU（偏好長壽節點 → 抗日蝕攻擊）；
`closest(target, n)` 回傳 XOR 距離最近的 n 個聯絡人——同時用於路由與任務歸屬。

**`kademlia.py`** — [`KademliaNode`](../src/swarmsolve/discovery/kademlia.py) 實作
`PING/PONG`、`FIND_NODE`、迭代 `lookup`（O(log n) 輪）、`bootstrap`，以及
`is_responsible_for(key, replicas)`（我是否在離該 key 最近的若干 peer 之內？）。刻意
省略 STORE/FIND_VALUE——我們只用鍵空間把*任務路由到最近 peer*，不做值儲存。

### 3.3 傳播層 — `gossip/`

[`Gossip`](../src/swarmsolve/gossip/gossip.py) 採推式：收到訊息時 (1) 用有界
`seen` `OrderedDict` 丟棄重複，(2) 投遞給本地 `deliver` 回呼，(3) 若 `ttl > 0` 則遞減
並轉發給隨機 `fanout`（=3）個鄰居。既限制流量，又能高機率覆蓋整個網路（第7章 BubbleStorm
思想）。

### 3.4 任務層 — `tasks/`

**`task.py`** — [`Task`](../src/swarmsolve/tasks/task.py) 是搜尋空間的一棵子樹，由其
賦值 `path` 標識。`path_repr` 是正規化（與順序無關）字串；`Task.key` 即
`task_key(path_repr)` → 它在 XOR 空間中的位置。`lease_active()` 判斷認領是否仍有效。

**`scheduler.py`** — [`Scheduler`](../src/swarmsolve/tasks/scheduler.py) 是每個 peer
的大腦。狀態：`open`、`claimed`、`dead_ends`、`done`。亮點：

* `add_open` 忽略已 dead/done/被有效認領的任務（去重）。
* `next_task` 選取**與本機 ID XOR 距離最小**的開放任務——這就是結構化、低衝突的放置
  （第5/6章）。
* `reclaim_expired` 把租約過期的任務移回 `open` → peer 崩潰時自動重分配（容錯）。

### 3.5 求解層 — `solver/`

**`board.py`** — [`Board`](../src/swarmsolve/solver/board.py) 每個格子存一個**候選位元
遮罩**。`assign` 做消去 + 唯一候選（naked singles）傳播（AC-3 風格），衝突時擲出
`Contradiction`。`most_constrained_cell` 實作 MRV 啟發式。支援任意 N=k²（9/16/25）。

**`search.py`** — 三個原語：
* [`expand_subtasks`](../src/swarmsolve/solver/search.py) — 對 MRV 格子的每個候選產生
  一個子路徑（即時矛盾者直接剪除）。
* [`solve_subtree`](../src/swarmsolve/solver/search.py) — DFS 一棵子樹，帶掛勾
  `is_dead_end` / `record_dead_end` / `should_stop`，外加 `node_delay`（演示成本旋鈕）
  與 `enumerate_all`（走遍整棵樹 / 統計解數）。
* [`solve_local`](../src/swarmsolve/solver/search.py) — 單機基準。

### 3.6 編排 — `peer.py`

[`Peer`](../src/swarmsolve/peer.py) 串起所有層並執行工作迴圈。關鍵方法：
`start`/`bootstrap`、`_dispatch`（區分探索 vs gossip）、`_on_gossip`（套用
OPEN_TASK/DEAD_END/TASK_DONE/TASK_CLAIM/SOLUTION）、`seed_frontier`+`submit`（生產者）、
`run`+`_work_on`（消費者）以及剪枝掛勾。重要開關：`split_depth`（工作竊取）、
`enumerate_mode`、`lease_seconds`、`idle_limit`、`node_delay`、`dead_end_share_depth`、
`on_tick`（儀表板）。

### 3.7 命令列 — `cli.py`

[`cli.py`](../src/swarmsolve/cli.py) 公開所有命令與共用的多行程機制（`_peer_worker`、
`_spawn`、`_collect`）。`_collect` 對被 kill 的 peer 具韌性（輪詢存活而非死等 N 個結果）。

---

## 4. 訊息協定

| 型別 | 傳輸 | 負載 | 用途 |
|------|------|------|------|
| `PING`/`PONG` | UDP | host, port | 存活偵測 / 桶更新 |
| `FIND_NODE` | UDP | target | 迭代查找 |
| `FIND_NODE_REPLY` | UDP | target, nodes[], reply_to | 查找回覆 |
| `OPEN_TASK` | TCP/gossip | task | 公佈一棵未探索子樹 |
| `TASK_CLAIM` | TCP/gossip | task（owner, lease） | 「我來做這個」 |
| `DEAD_END` | TCP/gossip | path | 全網裁剪該子樹 |
| `TASK_DONE` | TCP/gossip | path | 子樹已探索完畢 |
| `SOLUTION` | TCP/gossip | board（扁平） | 最終答案 → 全員停止 |

---

## 5. 端到端流程

```mermaid
sequenceDiagram
    participant S as 提交者
    participant A as 節點 A
    participant B as 節點 B
    S->>S: seed_frontier() 把根切成子任務
    S-->>A: OPEN_TASK*
    S-->>B: OPEN_TASK*
    A->>A: next_task() = 離我最近，認領（租約）
    B->>B: next_task()（另一個任務），認領（租約）
    A->>A: DFS；淺層矛盾
    A-->>B: DEAD_END(path)
    B->>B: 裁剪該子樹
    B->>B: DFS → SOLUTION
    B-->>S: SOLUTION
    B-->>A: SOLUTION
    Note over S,A,B: should_stop() 於各處觸發
```

---

## 6. 關鍵機制（深入）

* **XOR 任務放置。** `task_key(path)` 位於節點 ID 空間，因此 `next_task` 偏好離本機最近
  的任務，使工作確定性分佈且衝突少——Kademlia（第6章）兼作負載平衡器（第5章）。
* **工作竊取（`split_depth`）。** 當任務深度小於 `split_depth` 時，`_work_on` 把它再切成
  更細的 OPEN_TASK 並 gossip 出去，而非自己求解。粒度自適應叢集規模，讓閒置 peer 有事可做。
* **租約與重分配。** `claim_local` 設 `lease_expires = now + lease`；`reclaim_expired`
  （於 `next_task` 內呼叫）把過期任務移回 `open`，於是崩潰 peer 的工作被重做。`idle_limit`
  讓 peer 存活夠久以等待租約過期。
* **去重。** `add_open` 加上 `_work_on` 內最後一刻的檢查，跳過已 done/dead/被有效認領的
  任務，削減由 gossip 延遲造成的大部分重複（即作業的「避免重複工作」挑戰）。
* **死路深度上限（`dead_end_share_depth`）。** 只 gossip *淺層*死路；深層葉子死路太多太
  具體。若無此限制，難題會用上萬則訊息把網路淹沒。
* **`node_delay`。** 僅供演示的人為每節點成本。真實數獨節點太廉價，無法暴露網路效應，
  故以它代理「昂貴」計算（25×25 / 拼圖），用於量測加速、復原與儀表板。

---

## 7. 三個演示

### A）容錯 — `swarmsolve fault`
以**窮舉模式** + 較大 `idle_limit` 執行，因此每個任務*必須*完成。它在求解中途 kill 一個
peer（`--kill-peer`、`--kill-after`）；該 peer 的租約（`--lease`）過期後其任務被存活者
接管。唯有重分配成功，整個執行才會結束。
```bash
uv run swarmsolve fault --file examples/puzzles/hard_9x9.txt \
    --peers 4 --kill-peer 2 --kill-after 1.5 --lease 1.5 --node-delay 0.0008
```
留意：*「killed peer #2 returned a result: no」* 與 *「swarm STILL solved …」*。

### C）即時儀表板 — `swarmsolve dashboard`
每個 peer 透過 `on_tick` 回報快照；父行程以 `rich.Live` 渲染表格（每個 peer 的
鄰居 / open / claimed / dead / done / nodes / found）。
```bash
uv run swarmsolve dashboard --file examples/puzzles/hard_9x9.txt --peers 4 --node-delay 0.003
```

### B）真實加速 — `swarmsolve benchmark`
誠實的加速敘事。**首解**搜尋（`demo`）把答案放在一條無法平行化的深 DFS 路徑上；
**窮舉**搜尋（`benchmark`——統計所有解 / 驗證唯一性）天然可平行，呈現近線性加速。
```bash
uv run swarmsolve benchmark --file examples/puzzles/hard_9x9.txt \
    --peers 4 --node-delay 0.0012 --split-depth 4
# 基準 ~14.5s ；叢集 ~8.7s ；加速 ~1.67x ；解數一致
```

---

## 8. 效能 — 誠實討論

* 對**首解**數獨，牆鐘加速有限：解位於一條深路徑，沿之的 DFS 本質上是串列。協調開銷甚至會
  讓極小的 9×9 比單機更*慢*。
* 對**窮舉**型負載，加速是真實的（我們 4 peer 實測約 1.3–1.7×）。低於理想的 4× 源於
  (a) 非同步 gossip 造成的重複探索，(b) 當某子樹遠大於其他時的負載不均。更深的 `split_depth`
  改善平衡但增加重複——這是分散式搜尋的經典取捨，也是報告的絕佳討論點。
* 進一步提升加速的方向：基於 XOR key 的確定性單一負責人執行（消除重複）、感知抖動的歸屬，
  以及更細的隨需自適應切分（隨需工作竊取）。

---

## 9. 擴充：拼圖

框架與具體謎題無關：凡能表達為「搜尋樹切分子任務 + 死路剪枝 + 首解/全解」的問題皆適用。
對拼圖，每次**拼塊放置**是一個分支，非法的局部拼裝就是死路。只需替換 `solver/` 套件，
傳輸/探索/傳播/任務層完全重用。

---

## 10. 課程知識點對應

| 知識點 | 程式碼位置 |
|--------|-----------|
| Gnutella 式訊息、TTL 氾濫（第2章） | `transport/`、`gossip/` |
| Gossip / 流行病式擴散（第2章） | `gossip/gossip.py` |
| Kademlia：XOR 度量、k桶、FIND_NODE（第6章） | `discovery/` |
| 結構化放置 / 負載平衡（第5章） | `task_key` + `scheduler.next_task` |
| 機率覆蓋（第7章 BubbleStorm） | gossip 扇出 + seen 集合 |
| 容錯 / 抖動 | 租約 + `reclaim_expired` + `is_responsible_for` |

---

## 11. 課程知識點詳解

每個課程章節在程式碼中的體現。

* **第1章 — P2P 基礎。** SwarmSolve 是*純* P2P 系統：每個節點既是用戶端又是伺服器，
  **沒有中央索引**（不同於 Napster），節點自組織，系統**自擴展**（節點越多搜尋吞吐越高）
  且**具韌性**（容忍崩潰）。見 [`peer.py`](../src/swarmsolve/peer.py)。
* **第2章 — 非結構化覆蓋網與 gossip。** Gnutella 以 TTL 氾濫查詢。我們保留其優點
  （gossip + TTL），並用 **seen 集合**去重修正冗餘。三類應用訊息都搭載於此 gossip 之上。
  見 [`gossip/gossip.py`](../src/swarmsolve/gossip/gossip.py)。
* **第3章 — 隨機圖模型。** Kademlia 建構的覆蓋網是低直徑圖（O(log n) 跳）；每個節點保存
  O(k·log n) 狀態——這是度與直徑的經典取捨（小世界 / 無尺度）。
* **第4章 — DHT（CAN / Chord）。** 結構化覆蓋網以定向 O(log n) 路由與 put/get 鍵介面
  取代氾濫。我們採用此*結構化*哲學，並選 Kademlia（第6章）作為具體 DHT。
* **第5章 — 負載平衡（Distance-Halving 思想）。** 目標是均勻分攤負載。我們把任務路徑
  均勻雜湊進 XOR 鍵空間，*免費*得到均勻的任務分佈；離 key 最近的節點擁有該任務。見
  [`task_key`](../src/swarmsolve/discovery/node_id.py) + [`Scheduler.next_task`](../src/swarmsolve/tasks/scheduler.py)。
* **第6章 — Kademlia（我們的探索層）。** XOR 距離、偏好長壽節點的 k桶（抗日蝕）、基於
  UDP 的迭代 FIND_NODE——既是探索也是任務放置的骨幹。見 [`discovery/`](../src/swarmsolve/discovery)。
* **第7章 — BubbleStorm（機率覆蓋）。** 隨機副本使查詢以高機率遇到資料。我們的 gossip
  扇出 + TTL 實現同樣思想：訊息以高機率覆蓋整個網路，同時流量受控。

---

## 12. 訊息鏈路詳解（資料流走讀）

每一步都標註了執行的函式，便於端到端追蹤鏈路。

### 12.1 節點加入（bootstrap）
```
Peer.start(boot)
  → KademliaNode.bootstrap([boot])      # discovery/kademlia.py
      → PING boot（UDP）
      → lookup(self)：多輪 FIND_NODE      # 迭代，O(log n)
      → RoutingTable.add(contacts)       # k桶逐漸填滿
```
結果：加入者認識足夠多的鄰居以進行 gossip。

### 12.2 OPEN_TASK（生產 → 消費）
```
Peer.submit(target)                       # 僅提交者
  → seed_frontier()：expand_subtasks(root)
  → gossip.broadcast(OPEN_TASK)           # 去重(seen) → 轉發給 fanout，ttl--
遠端 Peer._on_gossip(OPEN_TASK)
  → Scheduler.add_open(task)              # 對 done/dead/claimed 去重
Peer.run() → Scheduler.next_task()        # 選離我最近(XOR)的任務
```

### 12.3 TASK_CLAIM（分散式租約）
```
Peer._work_on(task)
  → Scheduler.claim_local(task)           # lease_expires = now + lease
  → gossip.broadcast(TASK_CLAIM)
遠端 Peer._on_gossip(TASK_CLAIM)
  → Scheduler.note_claim(task)            # 從 open 移除 → 去重
```

### 12.4 DEAD_END（共享剪枝）
```
solve_subtree(record_dead_end=_publish_dead_end)
  → 在淺層(≤ dead_end_share_depth)發生矛盾
  → _publish_dead_end：mark_dead(path) + gossip.broadcast(DEAD_END)
遠端 Peer._on_gossip(DEAD_END) → Scheduler.mark_dead(path)
之後 DFS → _is_dead_end(path)==True → 跳過該子樹
```

### 12.5 SOLUTION（全域停止）
```
solve_subtree → 完整棋盤
  → self.solution = board；gossip.broadcast(SOLUTION)；_stop.set()
遠端 Peer._on_gossip(SOLUTION) → 重建棋盤；_stop.set()
  → 每個執行中的 DFS 內 should_stop() 觸發
```

### 12.6 故障復原（租約回收）
```
節點 C 持有任務 T（在每個節點上狀態為 CLAIMED）時崩潰
  → C 的 lease_expires 過期
  → 任一 Peer.run() → next_task() → reclaim_expired()：T → OPEN
  → 某個存活者認領並重做 T                 # idle_limit 讓節點存活夠久
```

---

## 13. 演示流程與預期輸出

### A）`swarmsolve fault` — 容錯
1. 以**窮舉**模式啟動 N 個行程，並設較大 `idle_limit`。
2. 在 `--kill-after` 後，父行程 `terminate()` 掉 `--kill-peer` 指定的節點。
3. 該節點租約過期；存活者接管其任務；所有任務完成。

預期（4 節點，殺 #2）：
```
>>> killed peer #2 (PID …)
Result
   killed peer #2 returned a result: no (as expected)
   surviving peers that finished: [0, 1, 3]
   swarm STILL solved the puzzle in ~13s despite the failure
```

### C）`swarmsolve dashboard` — 即時視覺化
`rich.Live` 表格透過 `on_tick` 掛勾更新各節點計數（鄰居 / open / claimed / dead /
done / nodes / found），隨後印出最終的每節點報告與解出的棋盤。

### B）`swarmsolve benchmark` — 誠實加速
窮舉搜尋（統計所有解 / 驗證唯一性）天然可平行。預期（難 9×9，4 節點，
`--node-delay 0.0012 --split-depth 4`）：
```
baseline : ~14.5s, 9309 nodes, 1 solutions
swarm    : ~8.7s wall, ~13k nodes across 4 peers
correctness OK: all 1 solution(s) covered exactly once
speedup  : ~1.67x (wall clock)
```
為何達不到理想的 4×：重複探索（非同步 gossip）+ 負載不均——即第 8 節討論的取捨。
