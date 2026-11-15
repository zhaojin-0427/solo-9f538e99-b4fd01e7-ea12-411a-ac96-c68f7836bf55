# Battery Cycle-Test Schema Registry

电池循环测试 Parquet 批次的统一汇入服务。不同固件版本导出的文件列名可能变化，
本服务用**稳定字段 ID** 作为分析表的对齐基准，把"物理列名"降级为每批次自己的快照，
从而在改名、加列、收紧必填之后仍能读取旧批次，且旧列名被新字段复用也不会串值。

## 技术栈

- **FastAPI** — HTTP API
- **PyArrow** — Parquet 读取、schema/类型/空值校验、按字段 ID 投影
- **SQLite (WAL)** — 结构版本、字段登记、批次与列名快照、收紧提案
- **Docker Compose** — 一键启动，数据落在具名卷 `/data`

## 快速启动

```bash
docker compose up --build
# 服务监听 http://localhost:8000 ，交互式文档 /docs
curl http://localhost:8000/health
```

本地（无 Docker）：

```bash
pip install -r requirements.txt
DATA_DIR=./data uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 数据模型与不变量

| 表 | 内容 |
| --- | --- |
| `schema_versions` | 每次结构变更生成一条不可变版本 |
| `fields` | 版本 × 字段 ID：列名、类型(`int64`/`float64`/`string`)、可空性 |
| `batches` | 批次元数据 + `field_id -> 物理列名` 快照 |
| `tightening_proposals` | 收紧为必填的两阶段提案及裁决结果 |

只允许三种结构演进，其余一律拒绝：

1. **改名** — 字段 ID 不变，产生新版本；旧批次按自己的列名快照读取
2. **增加可空列** — 新字段必须 nullable；旧批次缺列查询时补 `null`
3. **收紧为必填** — 两阶段提案，核查全部已入库批次后方可生效

类型变更、删字段、必填改可空均不支持。

### 关键正确性保证

- **按字段 ID 对齐**：查询用批次自己的 `field_id -> 列名` 快照投影物理列；
  旧列名被新字段采用时，两个字段 ID 各取各的列，绝不串值。
- **收紧裁决**：有空值或缺列的批次会被拒绝，并在响应
  `details.offending_batches` 中逐个指出批次 ID 与原因；拒绝本身被提交，
  规则不生效，提案状态为 `rejected`。
- **核查期间可上传**：Parquet 扫描不持有写锁，上传照常完成；裁决的短事务会
  补扫扫描窗口内新落地的批次，新批次同样纳入裁决（见
  `batches_uploaded_during_check`）。
- **旧结构不能绕过新必填**：上传按声明版本解析结构，但必填性取**当前已生效**
  结构；缺列或含空值即 400。
- **同一已提交状态**：`/api/v1/catalog` 在单个 SQLite 读事务（一个快照）内
  返回结构与批次清单，两者不可能来自不同版本。
- **失败保留原状态**：上传先写临时文件、校验通过后原子改名并在一个事务内登记；
  任一步失败即回滚事务并删除临时文件。Parquet 一旦入库不再改写。
- **重启可查**：所有状态都在 `DATA_DIR`（SQLite + 不可变 Parquet 文件），
  容器重启后直接可查；待决提案也持久化，重启后仍可裁决。

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/v1/schema/init` | 初始化首版结构 |
| GET | `/api/v1/schema/current` | 当前结构 |
| GET | `/api/v1/schema/versions` / `/{version}` | 历史版本 |
| POST | `/api/v1/schema/rename` | 字段改名（新版本） |
| POST | `/api/v1/schema/fields` | 新增可空字段（新版本） |
| POST | `/api/v1/schema/tighten/proposals` | 创建收紧提案（202） |
| POST | `/api/v1/schema/tighten/proposals/{id}/decide` | 裁决（成功 200 / 拒绝 409） |
| GET | `/api/v1/schema/tighten/proposals` | 提案清单 |
| POST | `/api/v1/batches` | 上传 Parquet（表单：`file` + `schema_version`） |
| GET | `/api/v1/catalog` | 结构 + 批次清单（同一快照） |
| GET | `/api/v1/batches` | 批次清单 |
| GET | `/api/v1/query` | 按字段 ID 查询（`field_ids`、`batch_ids`、`limit`、`offset`） |
| GET | `/health` | 存活探针 |

### 典型调用

```bash
# 1. 初始化结构
curl -X POST localhost:8000/api/v1/schema/init -H 'Content-Type: application/json' -d '{
  "fields": [
    {"field_id": "cycle_id", "name": "cycle", "type": "int64", "nullable": false},
    {"field_id": "voltage",  "name": "volt",  "type": "float64", "nullable": true}
  ]}'

# 2. 上传批次（注明结构版本）
curl -X POST localhost:8000/api/v1/batches \
  -F schema_version=1 -F file=@batch_fw1.parquet

# 3. 改名 -> 新版本
curl -X POST localhost:8000/api/v1/schema/rename -H 'Content-Type: application/json' \
  -d '{"field_id": "voltage", "new_name": "voltage_v"}'

# 4. 收紧为必填（两阶段）
PID=$(curl -s -X POST localhost:8000/api/v1/schema/tighten/proposals \
  -H 'Content-Type: application/json' -d '{"field_id":"voltage"}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["proposal_id"])')
curl -X POST localhost:8000/api/v1/schema/tighten/proposals/$PID/decide

# 5. 按稳定字段 ID 查询跨固件批次
curl 'localhost:8000/api/v1/query?field_ids=cycle_id,voltage&limit=1000'
```

## 端到端演示与测试

```bash
pip install pytest httpx
python3 -m pytest tests/ -q          # 31 个单元/并发/重启测试

# 真实 HTTP 端到端（先启动服务）
DATA_DIR=/tmp/demo uvicorn app.main:app --port 8000 &
python3 scripts/e2e_demo.py          # 18 项断言的完整业务场景
```

## 目录

```
app/            FastAPI 应用与领域逻辑
  config.py     数据卷/数据库路径与限额
  database.py   SQLite 连接、WAL、读写事务
  locks.py      进程内串行写锁
  schemas.py    请求模型
  registry.py   结构登记、版本演进、收紧两阶段裁决
  parquet_store.py  Parquet 校验、不可变存储、字段 ID 对齐读取
  ingest.py     上传编排（临时文件 -> 校验 -> 原子登记）
  query.py      快照目录与分页查询
tests/          pytest 套件
scripts/        端到端演示脚本
docker-compose.yml / Dockerfile
```

## 部署备注

- 单进程 uvicorn 即可满足一致性；写操作由进程锁 + SQLite `BEGIN IMMEDIATE`
  串行化，读走 WAL 快照。多副本部署需把写路径收敛到单实例，或改用带行锁的
  外部数据库（当前范围未实现）。
- Parquet 文件以 `<batch_id>.parquet` 存于 `DATA_DIR/parquet`，只增不改。
