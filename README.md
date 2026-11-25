# CDP-1 充电数据智能平台

`0.40.0-dev` 是面向合成数据的开发沙箱。它演示一笔订单如何经过契约校验、ODS 原始归档、DWD 版本化入账，再由 FastAPI 返回站点日指标。技术分工和生产演进见 [架构基线](docs/architecture.md)。

## 启动与回放

在仓库根目录运行，需要 Docker 与 Compose v2。

```sh
docker compose up --build --detach --wait
docker compose exec -T api python -m cdp.cli samples/order.json
docker compose exec -T api python -m cdp.cli samples/order.json
curl -fsS http://127.0.0.1:8000/health/ready
curl -fsS 'http://127.0.0.1:8000/v1/demo/station-daily?day=2026-01-15'
docker compose exec -T api python - < tests/test_orders.py
```

空数据卷上首次导入输出 `inserted`，后续重放输出 `duplicate`。查询结果应始终为 1 单、10,000 Wh、电费 800 分、服务费 300 分。订单在上海时间 1 月 14 日开始、15 日结束，归属 15 日。

如果构建机的 HTTPS 代理使用企业 CA，可将已受信的 CA 证书包临时传给 BuildKit。云环境使用以下命令。

```sh
docker build --secret id=build_ca,src=/etc/ssl/certs/ca-certificates.crt -t cdp1:0.40.0-dev .
docker compose up --no-build --detach --wait
```

复制 `.env.example` 为 `.env` 可修改本地访问端口 `CDP_PORT`。Compose 仅在宿主机回环地址发布端口。应用容器以 UID 10001 运行，`CDP_DATA_DIR=/data` 指向命名卷。使用 `docker compose down` 停止环境并保留数据卷。此服务使用无鉴权的演示接口，输入限合成样本。

### 合成订单跨日对账与三次回放

在已安装项目依赖的开发环境中运行：

```bash
bash scripts/rehearse-batch-reconciliation.sh
```

脚本用临时 SQLite 库验证上海午夜前后的订单归日、金额修订、重复和旧版本投递。对账以固定样例为基准，也验证金额不符时返回退出码 2。随后导出已提交的订单快照，分别导入三个空库，逐日比对对账结果。此演练覆盖 0.1 演示订单及其站点日报，运行结束会清理临时数据。

### S7 24 个月全量回放演练

在已安装依赖的开发环境运行 `bash scripts/rehearse-full-replay.sh`。脚本为 2024 年 1 月至 2025 年 12 月的每个上海业务日生成两个站点的演示订单，校验导出的原始文件摘要，再按三种顺序导入三个空库。每轮额外完整重投一次，逐日与固定口径对账，并比对日报 SHA-256。演练使用临时 SQLite，覆盖 0.1 演示订单的批量重放一致性。

```bash
bash scripts/rehearse-full-replay.sh --large
```

大样本模式覆盖 120 个合成站点和 731 个上海业务日，每站每天一笔，共 87,720 笔。三轮回放各从空库开始，在完整重投前后均逐站逐日核对订单数、电量、电费和服务费，并与源库日报摘要比对。每轮结束后清理目标库。可通过 `TMPDIR` 指定临时存储位置。该模式覆盖试点站点数量，目标环境的实际订单量和回放耗时仍需独立验收。

## 合成数据沙箱验收

在已安装 Pydantic 2 的 Linux 开发环境运行：

```bash
bash scripts/rehearse-sandbox-acceptance.sh
```

入口按七组脚本执行 S1–S9，逐组汇总通过和失败结果。单组失败后会继续执行其余脚本，最终以非零状态退出。演练使用临时 SQLite、合成订单、本机 HTTP 服务和模拟时钟，无需 PostgreSQL 驱动或新容器。若云盘空间不足且 Linux 提供可写的 `/dev/shm`，可在命令前加 `TMPDIR=/dev/shm`。

| 场景 | 沙箱验证 |
| --- | --- |
| S1、S2、S8 | 重复事件去重、迟到归窗与幂等回填、异常设备时间隔离 |
| S3 | 第 50 笔提交后强制终止，核验检查点与恢复对账 |
| S4 | 金额字段改名触发契约阻断及质量指标，修复后补采 |
| S5 | V1 退款后重放旧版本，核验拒绝计数、历史与归档 |
| S6 | 本机 HTTP 服务返回 429，验证退避、恢复与去重 |
| S7 | 24 个月合成历史三次回放，核对日报摘要 |
| S9 | 跨峰谷订单的分段电量、两项费用、归档及一分钱差错阻断 |

### 扩展验收入口

以下命令均从仓库根目录执行。Compose 场景各自启动并清理独立项目。按场景构建所需镜像：

| 场景 | 命令 | 前置条件 |
| --- | --- | --- |
| S3 · 第 50,000 笔中断恢复 | `bash scripts/rehearse-collector-crash.sh --large` | 本地 Python 环境与项目依赖 |
| S4 · 告警到达与解除 | `bash scripts/rehearse-source-schema.sh` | Compose、API 和 Prometheus 镜像 |
| S7 · 120 站点历史回放 | `bash scripts/rehearse-full-replay.sh --large` | 本地 Python 环境与项目依赖、临时存储空间 |
| I1 · Kafka leader 宕机 | `bash scripts/rehearse-kafka-broker-failover.sh` | Compose 和 API 镜像 |
| I2 · PostgreSQL 切换与恢复 | `bash scripts/rehearse-postgres-primary-recovery.sh` | Compose |
| M3 · 联合链路时效 | `bash scripts/rehearse-pipeline-latency.sh` | Compose、API 和 Spark 镜像 |

上述场景使用合成数据。目标预生产环境仍需按项目书验证 72 小时稳定运行、真实规模回放、正式告警送达、基础设施故障恢复及业务对账。

## PostgreSQL 沙箱

默认 Compose 使用 SQLite。以下命令启动独立的 PostgreSQL 16 环境，使用单独的数据卷和端口。首次启动自动执行版本化迁移。云端先运行上节的 `docker build --secret ...`，再执行下列命令。

```sh
export CDP_PORT=8001
docker compose -p cdp-pg -f compose.yaml -f compose.postgres.yaml up --no-build --detach --wait
docker compose -p cdp-pg -f compose.yaml -f compose.postgres.yaml exec -T api python -m cdp.cli samples/order.json
docker compose -p cdp-pg -f compose.yaml -f compose.postgres.yaml exec -T api python -m cdp.reconcile samples/reference-day.json
docker compose -p cdp-pg -f compose.yaml -f compose.postgres.yaml exec -T api python - < tests/test_postgres.py
```

对账参考文件包含指定业务日的完整站点集合及四项整数指标。报告列出缺失、多出的站点以及指标差异，匹配退出码为 0，有差异为 2，输入无效为 1。样例参考来自手工录入的合成数据预期值。正式对账需使用经授权的计费源快照，并统一数据截止点及指标口径。

`CDP_POSTGRES_DSN` 可为独立运行的 CLI 或 API 指定数据库。示例连接和数据库角色只供开发环境使用。PostgreSQL 未在宿主机发布端口。两套数据库可通过归档导出、再导入迁移数据。Compose 切换只会改变后端配置。

## 契约与归档

[订单样本契约](contracts/order-demo-0.1.schema.json) 由 [校验模型](cdp/contract.py) 导出。每个 JSON 文件包含一个已完成订单快照。订单和站点编号为演示业务键，`version` 是同一订单单调递增的整数。时间带显式时区，明细统一存 UTC。时间先后和未来偏差在运行时校验。

ODS 按原始字节的 SHA-256 保存到 `/data/ods/orders/<sha256>.json`。明细行关联对应归档哈希。相同业务版本和内容重放无副作用，旧版本跳过，同版本异内容返回冲突。归档先于明细提交，若中途失败，可重试同一文件。CLI 逐文件提交，失败退出时按已成功文件可重放处理。

运行 `python -m cdp.cli --export-archive /path/to/new-directory` 导出当前已入账版本对应的原始快照，导出前逐份校验 SHA-256。在新的目标库配置下运行 `python -m cdp.cli /path/to/new-directory/*.json` 即可重建这些订单。仅使用退出成功的导出目录。导出依赖仍可读取的明细引用和 ODS，因此生产备份须覆盖两者。此功能用于当前明细快照重建，历史版本、源端增量游标和不可变对象存储接入将单独实现。

## 增量采集演练

计费源模拟表见 [billing-demo.sql](samples/billing-demo.sql)。连接使用 SQLite 只读模式，并通过固定字段投影取得业务快照。启动任一沙箱后，可在对应 API 容器中运行下面的命令。请为不同数据源使用不同的 `--source-id`。

```sh
docker compose exec -T api python -c 'import sqlite3; from pathlib import Path; db=sqlite3.connect("/data/billing-demo.sqlite3"); db.executescript(Path("samples/billing-demo.sql").read_text()); db.close()'
docker compose exec -T api python -m cdp.collect /data/billing-demo.sqlite3 --source-id billing-demo --page-size 1
docker compose exec -T api python -m cdp.collect /data/billing-demo.sqlite3 --source-id billing-demo --page-size 1
docker compose exec -T api python -m cdp.reconcile samples/reference-day.json
docker compose exec -T api python - < tests/test_collect.py
```

源表的 `updated_at` 使用精确到微秒的 UTC 固定格式，如 `2026-01-14T16:31:00.000000+00:00`，并与 `order_id` 建立联合索引。每次采集固定截止时刻，在同一读取快照中分页，成功后把截止时刻写入 `CDP_DATA_DIR/collection.sqlite3`。后续任务回看 15 分钟。若某行失败，之前已入账的行保留，截止时刻保留在上次成功值，修正源数据并重跑即可。生产上的任务实例须共享和备份同一检查点，或将检查点迁移到集中协调存储。

采集器使用合成源中每个订单的最新快照。回看窗口覆盖短迟提交，超过窗口的差异需要通过独立对账和回填处理。硬删除、退款及从真实计费库生成可比较的快照需要确认上游语义和业务口径。

若对账发现超过回看窗口的遗漏，按源 `updated_at` 指定含起止时刻的回填区间。回填不会移动常规采集截止时刻。

```sh
docker compose exec -T api python -m cdp.collect /data/billing-demo.sqlite3 \
  --backfill-from 2026-01-14T16:00:00+00:00 --through 2026-01-14T17:00:00+00:00
```

### S3 采集中断恢复演练

在 Linux 开发环境运行 `bash scripts/rehearse-collector-crash.sh`。脚本生成 120 笔合成订单，在采集子进程提交第 50 笔后发送 SIGKILL。重启后验证检查点从上一次成功位置续跑、已提交订单去重、遗漏订单补齐、归档可读，且日报订单数、电量、电费和服务费与固定参考一致。默认演练使用临时 SQLite，适用于快速沙箱验收。

```bash
bash scripts/rehearse-collector-crash.sh --large
```

大样本模式生成 50,120 笔合成订单，在第 50,000 笔提交后强制终止。恢复时应识别 50,000 笔重复订单并补齐 120 笔，随后核验检查点、日报对账和所有归档订单的标识及摘要。该模式仍使用临时 SQLite，目标预生产部署上的验收需独立执行。

## V1 订单历史

开发环境可写入合成 V1 事件，并检查已接收的订单版本和对应分时明细。

```sh
docker compose exec -T api python -m cdp.billing_ledger /path/in/container/order.json
docker compose exec -T api python -m cdp.billing_ledger --history ORD_20260115_SH001_0001
```

版本历史与当前快照一起提交。已迁移的旧快照以 `null` 标记未知分时明细，后续版本保留源端提供并通过勾稽校验的分时数据。退款业务日和冲减金额待确认，V1 查询限开发明细。

### S5 旧版本重放演练

在已安装依赖的开发环境运行 `bash scripts/rehearse-version-rollback.sh`。脚本从合成 V1 计费源采集完成版与退款版，再用晚两小时的截止时间回填同一批事件。回填报告须记录一笔旧版本和一笔重复投递。演练同时核对当前退款状态、版本历史和归档快照。使用临时 SQLite 库和模拟截止时间，可在几秒内完成。


### S9 跨峰谷计费演练

```bash
bash scripts/rehearse-tou-billing.sh
```

演练为一笔上海时间 21:50–22:30 的合成订单提供 22:00 两侧的分段电量和整数演示费率。按分段电量核算 230 kWh、电费 198 元和服务费 81 元，再通过 V1 采集核对历史与归档。分段电费多一分钱的版本会触发契约阻断，检查点与已入账版本保持原样。重采原订单只记为重复。

演示费率和分段电量由脚本提供。正式费率、分段电量来源和舍入规则应按项目书确认后，以实际结算样本验收。

### 从 V1 合成源增量采集

`--format billing-v1` 通过只读 SQLite 快照读取 V1 JSON 事件，复用增量检查点和手动回填。分页按 `(updated_at, order_id, version)`，可处理同一订单在同一更新时间发生的多次变更。源索引键必须与载荷中的订单、版本和 UTC 更新时间一致。旧演示格式仍为默认，同名数据源的两种格式各有独立检查点。

在 API 容器中创建并采集合成数据。源文件路径须为尚未初始化的演练文件。

```sh
docker compose exec -T api python -c 'import sqlite3; from pathlib import Path; db=sqlite3.connect("/data/billing-v1-demo.sqlite3"); db.executescript(Path("samples/billing-v1-demo.sql").read_text()); db.close()'
docker compose exec -T api python -m cdp.collect /data/billing-v1-demo.sqlite3 \
  --format billing-v1 --source-id billing-v1-demo --page-size 1
docker compose exec -T api python -m cdp.billing_ledger --history ORD_20260115_SH001_0001
```

样例的已完成和退款事件具有相同更新时间，分别使用版本 1 和 2。V1 格式的回填命令同样传入 `--format billing-v1`。

## Kafka 发布演练

`compose.kafka.yaml` 增加单节点 Kafka 4.2.0 KRaft 与初始化任务，创建三分区的 `billing.orders.v1`。Broker 使用独立持久卷，端口只在 Compose 网络内可用。使用上一节的 V1 合成源步骤采集后，可运行一批发布任务。

```sh
export CDP_PORT=8003
docker compose -p cdp-event -f compose.yaml -f compose.postgres.yaml -f compose.kafka.yaml up -d --no-build --wait
docker compose -p cdp-event -f compose.yaml -f compose.postgres.yaml -f compose.kafka.yaml exec -T api \
  python -m cdp.publish --limit 100
```

发布任务从已入账历史中选取未确认事件，包含迁移时保留的旧快照。Kafka 确认后记录分区和偏移；失败时重跑相同命令。事件以订单号作为分区键，并携带基于订单号、版本和更新时间生成的稳定 `event_id`。Broker 已接收但确认丢失，或确认后数据库提交失败时，消息可能再次投递。消费者需要按 `event_id` 去重。当前确认记录对应一套开发 Kafka 及其持久卷，重建 Broker 时请使用独立演练环境。


### I1 Kafka 单 broker 宕机演练

构建当前 API 镜像后运行：

```bash
bash scripts/rehearse-kafka-broker-failover.sh
```

演练在隔离的 Compose 项目启动三节点 Kafka KRaft，测试主题使用三副本、最少两个同步副本，生产者复用应用的 `acks=all` 幂等发送。脚本先写入第一批合成消息，再强制终止该分区的 leader，等待剩余两个副本接管后写入第二批。原节点重启并重新加入同步副本集合后，再写入第三批。每阶段从头核对已确认消息的偏移量、键与完整载荷，验收共 30 条消息，无丢失、无重复。演练完成后清理独立容器与数据卷。目标预生产集群的故障演练需独立执行。

### Kafka 消费与窗口查询

发布 V1 事件后，从同一 PostgreSQL 和 Kafka 沙箱运行消费者。任务完成一批或 Kafka 暂无记录时退出，可重复执行。

```sh
docker compose -p cdp-event -f compose.yaml -f compose.postgres.yaml -f compose.kafka.yaml exec -T api \
  python -m cdp.consume --limit 100
curl -fsS 'http://127.0.0.1:8003/v1/demo/billing-events/windows?day=2026-01-14'
```

消费者按 `event_id` 在数据库持久去重，数据库提交后才提交对应的 Kafka 分区偏移。重复事件不会改变窗口计数；相同 ID 但内容冲突、无法解析的时间戳或不匹配的消息键会停止批次，问题消息的偏移留待处置。复用数据库并与消费组匹配，才能延续消费进度。开发环境可用新组复读主题以验证去重。

查询按 UTC 日期筛选，以订单原结束时间分入 5 分钟窗口，按站点和生命周期状态计数。计数单位为已去重的版本事件，退款事件归入原订单结束时间的窗口，仅用于观察流链路。水位为截至目前接受的最大订单结束时间减两小时。恰好等于水位的事件可进入原窗口，早于水位的首次事件进入迟到队列。比当前 UTC 时间超前超过 5 分钟的事件进入隔离表，持久隔离后提交消费偏移，不推进水位。重放仍按事件 ID 去重。

按 UTC 日期回填迟到事件。

```sh
docker compose -p cdp-event -f compose.yaml -f compose.postgres.yaml -f compose.kafka.yaml exec -T api \
  python -m cdp.consume --apply-late-day 2026-01-14
```

回填只纳入该日已登记且尚未应用的迟到事件，重复执行不增加计数。升级前的事件只有窗口起点可作水位初值，后续接到新事件后使用精确结束时间。该沙箱采用一条共享水位，并串行提交水位与事件路由。

### 重复投递、迟到归窗与时钟漂移演练

在已安装依赖的开发环境运行 `bash scripts/rehearse-stream-acceptance.sh`。脚本在临时 SQLite 库执行项目书 S1、S2 的流处理逻辑，检查 100 次重复投递后窗口计数仍为 1、恰好迟到两小时仍归入 5 分钟窗口、未来时间戳不推进水位，以及迟到三小时的事件只回填一次。退款版本在生命周期窗口单独计数。 S8 另用空库验证设备时间跳快 24 小时后，隔离表只记录一次，流进度保持不变，随后到达的正常事件仍能归窗。预生产验收还需在 Kafka、Spark 和 PostgreSQL 部署上验证传输、调度及财务对账。

### Spark Structured Streaming

Spark 4.2.0 作业可接管开发沙箱的 Kafka 读取，并复用 PostgreSQL 中的去重、水位、隔离和窗口状态。默认部署使用小批次读取和本地双线程。开发镜像在构建时校验 Kafka 连接器及依赖的 SHA-256。

```sh
docker build --secret id=build_ca,src=/etc/ssl/certs/ca-certificates.crt -t cdp1:0.40.0-dev .
docker build --target spark --secret id=build_ca,src=/etc/ssl/certs/ca-certificates.crt -t cdp1:0.24.0-spark-dev .
export CDP_PORT=8004
dc() { docker compose -p cdp-spark -f compose.yaml -f compose.postgres.yaml -f compose.kafka.yaml -f compose.spark.yaml "$@"; }
dc up -d --no-build --wait
# 先在本沙箱中按上节步骤采集并发布 V1 合成事件。
dc run --rm --no-deps spark --once
curl -fsS 'http://127.0.0.1:8004/v1/demo/billing-events/windows?day=2026-01-14'
```

`--once` 读完启动时可见的偏移后退出。用 `--follow` 持续运行，默认每五秒触发微批。重启复用 `spark-checkpoints` 卷，同一 checkpoint 同时只启动一个 Spark 作业。Spark 用自己的 checkpoint 记录 Kafka 偏移。微批有部分记录已落库时，重跑由数据库按事件 ID 去重。验收同一条开发流时选用 Spark 或原 CLI 消费者，避免两个读取端交替推进共享水位。

作业使用 Spark 的 Kafka 来源与 `foreachBatch`，在 driver 逐条调用开发持久化规则。5 分钟窗口和两小时水位仍由 PostgreSQL 路由。生产分布式聚合吞吐、多实例编排与共享 checkpoint 存储待后续验证。

### 运维指标与告警

API 的 `/metrics` 提供 Prometheus 格式的 Kafka 待发布与已确认数、流事件数、待回填与已补算数、隔离数、水位及最近一次 Spark 落库回调。水位和 Spark 完成时间使用 Unix 秒；未观察到事件或微批时，对应指标暂不出现。Spark 回调完成时间描述 PostgreSQL 写入成功的时刻，Spark 自己的 checkpoint 仍是 Kafka 偏移的依据。

可在 Spark 沙箱上叠加 Prometheus。默认宿主端口为 9095，可用 `CDP_PROMETHEUS_PORT` 修改。

```sh
docker build -f docker/prometheus.Dockerfile -t cdp1:0.18.0-prometheus-dev .
export CDP_PORT=8004
docker compose -p cdp-spark -f compose.yaml -f compose.postgres.yaml -f compose.kafka.yaml \
  -f compose.spark.yaml -f compose.monitoring.yaml up -d --no-build --wait
curl -fsS http://127.0.0.1:8004/metrics
```

Prometheus 每 15 秒抓取一次。演练规则在抓取连续失败 1 分钟、Kafka 待发布或迟到待补持续 15 分钟后进入告警。规则可在 Prometheus 中检查。外部通知、生产阈值和消费组延迟指标待接入。

### Spark 中途退出恢复演练

构建当前版本的 API 和 Spark 镜像后，执行下列命令。需要本机 Docker 与 Compose。

```sh
bash scripts/rehearse-spark-recovery.sh
```

脚本创建独立的临时 Compose 项目和数据卷，使用系统分配的 API 端口。它先发布两条合成事件，再在第二条写入暂挂时终止 Spark。重启使用同一 checkpoint，验证首条去重、次条补入以及 API 窗口计数。完成或失败后清理演练资源。失败会输出该项目的容器状态和 Spark 日志。演练不要求提供生产凭据。


### D10 采集阻断规则

`GET /v1/demo/collection-quality` 的来源记录包含既有的 `failure_code` 和细分的 `rule_id`。例如，V1 退款事件缺少金额字段时返回 `contract_validation` 和 `billing_schema`。规则编号只显露固定标识，响应不包含失败字段值。历史记录可能没有规则编号，成功重跑后返回 `rule_id: null`。

| 检查 | 规则编号 |
| --- | --- |
| JSON、V1 结构和演示订单契约 | `billing_json`、`billing_schema`、`demo_order_contract` |
| UTC 时间与订单时间顺序 | `billing_timestamp_utc`、`billing_time_order` |
| 币种、金额勾稽、精度及范围 | `billing_currency`、`billing_amount_balance`、`billing_energy_wh`、`billing_integer_range` |
| 分时区间、汇总和整数范围 | `billing_tou_interval`、`billing_tou_totals`、`billing_tou_integer_range` |
| 源游标、同版本冲突 | `billing_source_position`、`order_version_conflict` |

这些规则由增量采集执行。触发时本轮停止且检查点不推进，已提交版本可在修复后幂等重采。最新状态接口每个来源返回一条记录。另有按上海业务日查询的日报与历史接口：

- `GET /v1/demo/collection-quality/daily?day=2026-01-15` 汇总检查次数、阻断次数，并按来源、状态和规则列出明细计数。
- `GET /v1/demo/collection-quality/history?day=2026-01-15&limit=100&offset=0` 分页列出当日的检查编号、UTC 时间、来源、状态和规则，按检查时间从早到晚排序。限额最多 500 条。

启用驾驶舱权限后，这两个接口只对全站账户开放。迁移前的最新状态仍可查询，历史从启用新版本后开始记录。日报同时返回 `quarantine` 汇总。订单和输入记录在同一来源、同一检查日内按指纹去重，反复阻断仍分别计入检查次数。旧记录若缺少隔离证据，日报单独计数。

只有当 V1 输入的订单标识与源游标一致，且人民币总额可用整数分与费用组件勾稽时，才记录来源申报金额。同一订单的被拒记录金额一致时，日报只计一次；金额缺失或冲突的订单计入金额未知。申报金额用于解释差异，业务入账仍须通过契约。历史接口仅返回是否有隔离证据及可用金额。原始载荷和指纹均不在接口返回。

### 计费源字段变更演练

运行 `bash scripts/rehearse-source-schema.sh --local` 可用临时 SQLite 验证 S4。脚本改名 V1 退款快照的金额字段，核对采集阻断、既有检查点和质量指标，再恢复字段并重采，验证金额、去重和指标恢复。完整 Compose 演练另测告警经 Alertmanager 到达测试接收器。

增量采集拒绝不合法的 V1 记录并保留检查点。质量状态写入目标库，记录来源标识、检查时间和失败类型；原始载荷及字段值不进入质量状态。通过 `/v1/demo/collection-quality` 查询受阻来源。Prometheus 在持续受阻 1 分钟后触发 `CdpCollectionQualityBlocked`；修复数据后，下次成功的增量任务会清除受阻状态。手动回填不会改写增量状态。

构建当前 API 和 Prometheus 镜像后，可执行完整演练。脚本启动独立的 PostgreSQL、API、Prometheus、Alertmanager 和本机测试接收器，结束后清理演练资源。从每次采集命令启动计时，分别要求测试接收器在 300 秒内收到触发和解除通知，并输出实测耗时。该演练使用合成订单及隔离的 webhook，生产通知渠道需单独验收。

```sh
bash scripts/rehearse-source-schema.sh
```

## 充电运营驾驶舱

API 容器启动后，在本机访问 `http://127.0.0.1:8000/dashboard`。如果设置了 `CDP_PORT`，使用对应端口。页面提供上海业务日筛选、站点日报、生命周期事件小时分布和来源质量状态。选定的上海业务日会查询覆盖该业务日的前一 UTC 日和当日 UTC 窗口，再按上海时间筛选和汇总。

默认日期为合成样本的上海业务日 `2026-01-15`。用前述步骤导入 `samples/order.json` 可查看站点日报。按 V1 采集和 Kafka 消费或 Spark 步骤处理合成事件后可查看流窗口和采集状态。各区块独立报告空数据或加载失败。页面沿用现有开发 API，日报口径与生命周期事件计数分别展示。

## 指定时点生成站点特征

`python -m cdp.features --as-of <带时区的时间>` 输出一份 JSON 特征快照。每站包含过去 24 小时的已完成订单数和充电电量 Wh。窗口使用左闭右开，输出时间统一为 UTC。

```sh
docker compose exec -T api python -m cdp.features --as-of 2026-11-23T09:30:00+08:00
```

同时满足「源版本更新时间不晚于切点」和「平台已确认入账可用时间不晚于切点」的版本才有资格参与计算。每笔订单取当时可见的最高版本及同版本最新更新时间，再按订单结束时间和状态聚合。迟到补入或后续退款不会回写较早切点的结果。既有历史版本缺少可用时间证据时默认排除；可验证的重复事件最早从本次成功观察时纳入。空结果表示切点内没有符合条件的记录。该输出用于充电量特征试验，当前只使用订单数据。

### 天气预报特征

在 API 容器里使用 `python -m cdp.weather /path/to/forecast.json` 导入离线预报快照。文件包含 `kind: "forecast"`、`station_id`、UTC 的 `issued_at` 和 `valid_at`、整数 `temperature_centi_c` 与 `precipitation_micrometers`。例如 18.50°C 写作 1850，0.8 mm 写作 800。目标时间必须是 UTC 整点，且晚于发布时间。

`python -m cdp.features --as-of <带时区的时间> --with-weather` 在现有订单特征中附加指向下一个 UTC 整点的天气。每站选用在切点前发布且入库可用的最新预报。缺少可用预报时 `weather` 为 `null`，实况样本会被拒绝。当前离线契约用于合成演练，实际天气源、站点映射和商业授权另行确认。

### S6 气象接口限流演练

`python -m cdp.weather --url URL` 从 HTTP(S) 拉取单条气象快照。收到 HTTP 429 时指数退避并遵守 `Retry-After`，重试预算为 15 分钟。运行 `bash scripts/rehearse-rate-limit.sh` 用本机 HTTP 服务复现 429 和 `Retry-After`。演练用模拟时钟推进 10 分钟限流，要求恢复后 15 分钟内落库，核验快照只入库一次，并验证持续限流时命令超预算退出且无写入。演练输出从恢复到落库的模拟耗时，使用临时 SQLite。

## 下一 24 小时充电量基线

`cdp.baseline` 使用站点过去 24 小时的可用已完成订单电量，作为未来 24 小时的预测量。结果包含模型版本、特征版本、UTC 目标区间和每站电量 Wh，可重定向到 JSON 文件保留。

```sh
docker compose exec -T api python -m cdp.baseline --as-of 2026-11-23T09:30:00+08:00
docker compose exec -T api python -m cdp.baseline --as-of 2026-01-15T08:00:00+08:00 --backtest
```

回测使用目标结束时已可用的订单快照作实绩。只能回测已结束的目标区间。只有预测和实绩均有记录的站点计入误差，缺少任一侧的站点分别列出。MAE 为 Wh，WAPE 为总绝对误差除以配对实绩总量，分母为零时输出 `null`。缺少时点历史证据可能让示例区间返回空结果。这一基线暂未使用天气或正式结算数据。

驾驶舱的「未来 24 小时电量预测」使用服务器当前时刻作为切点，展示上海时区的目标时间范围、合计及逐站预测。页面的业务日筛选仅用于日报与流窗口。预测接口 `GET /v1/demo/station-forecast` 支持可选的带时区 `as_of` 参数，方便复核历史切点。当前时点无可用站点时显示暂无数据。

## 定时采集与发布

开发环境可用 `compose.pipeline.yaml` 周期性执行单一 V1 来源的增量采集和 Kafka 发布。默认一轮结束后等待 60 秒，下轮才开始，可用 `CDP_PIPELINE_INTERVAL_SECONDS` 调整。

```sh
dc() { docker compose -p cdp-pipeline -f compose.yaml -f compose.postgres.yaml -f compose.kafka.yaml -f compose.pipeline.yaml "$@"; }
dc up -d --no-build --wait api
dc exec -T api python -c 'import sqlite3; from pathlib import Path; db=sqlite3.connect("/data/billing-v1-pipeline.sqlite3"); db.executescript(Path("samples/billing-v1-demo.sql").read_text()); db.close()'
dc --profile pipeline up -d --no-build --wait pipeline
dc logs --tail 20 pipeline
```

合成源文件只需初始化一次。每轮先完成采集并保存检查点，再发布最多 100 条待确认事件。契约受阻时跳过本轮发布，修复源数据后下一轮自动重试。Kafka 发布失败时保留待确认事件并在后续轮次重试。消费者或 Spark 沿用各自的运行方式。可用 `dc --profile pipeline stop pipeline` 停止周期任务。

### 同时运行定时采集和 Spark 消费

在定时任务的 Compose 命令中加入 `-f compose.spark.yaml`，初始化合成源文件后启动两个服务。

```sh
dc() { docker compose -p cdp-pipeline -f compose.yaml -f compose.postgres.yaml -f compose.kafka.yaml -f compose.pipeline.yaml -f compose.spark.yaml "$@"; }
dc --profile pipeline up -d --no-build --wait pipeline spark-worker
dc logs --tail 20 pipeline spark-worker
```

`spark-worker` 每五秒检查输入并复用持久 checkpoint。PostgreSQL 会话租约防止同一数据库与同一配置路径的两份作业同时运行。已有 worker 时，手动 Spark 使用同一 checkpoint 会被拒绝。需要切换到手动模式时，先运行 `dc --profile pipeline stop spark-worker`。服务设为 `unless-stopped`，数据库和 Kafka 就绪后才启动。可通过窗口 API 和 Spark 落库指标核实处理进度。


### M3 联合链路时效演练

构建 API 和 Spark 镜像后运行：

```bash
bash scripts/rehearse-pipeline-latency.sh
```

脚本启动独立的 PostgreSQL、Kafka、定时采集和 Spark 环境，分三轮写入 30 笔合成订单。从生成事件计时，每秒查询窗口 API，核对站点、状态、5 分钟事件时间窗口及计数，并报告可见时延的 P95 和最大值。脚本同时检查 Kafka 确认数和 Spark 实际落库数。演练默认将采集间隔设为 5 秒，可用 `CDP_PIPELINE_INTERVAL_SECONDS` 调整。该演练用于短时端到端验证，正式 M3 仍需在目标环境连续运行 72 小时。

### V1 入账归档核验

新接受的 V1 历史版本将规范化事件快照保存在 `CDP_DATA_DIR/ods/billing-v1/<SHA-256>.json`，并在数据库内保存版本与摘要的关联。快照包含订单关键字段、状态、金额和分时明细。用户与车牌散列、原始载荷中的额外字段均不归档。归档先写入，历史和摘要关联随后在同一个数据库事务提交。

```sh
docker compose exec -T api python -m cdp.billing_ledger --verify-archive ORD_20260115_SH001_0001
```

核验逐版本区分成功、旧库无归档、文件丢失、损坏和不可读。仅全部匹配时退出码为 0。Kafka 对带摘要版本先校验文件，校验失败时未记录发布确认，可从备份恢复文件后重试。迁移前历史沿用数据库快照生成发布载荷。数据库备份应与本地归档卷配套。当前归档介质为本地持久卷。

### S3 归档备份与恢复

V1 归档可按订单备份到 S3 兼容桶。备份检查本地 SHA-256，条件写入防止覆盖已有对象，然后重新读取核验。恢复校验远端文件后才写入缺失的本地归档，拒绝覆盖损坏的本地副本。默认对象键为 `cdp1/ods/billing-v1/<SHA-256>.json`。

以下命令使用隔离的开发 SeaweedFS。开发凭据只用于该 Compose 网络，服务未向宿主机发布端口。

```sh
dc() { docker compose -p cdp-s3 -f compose.yaml -f compose.s3.yaml "$@"; }
dc up -d --no-build --wait
dc exec -T api python -c 'import boto3, os; boto3.client("s3", endpoint_url=os.environ["CDP_S3_ENDPOINT_URL"]).create_bucket(Bucket=os.environ["CDP_S3_BUCKET"])'
# 先按 V1 增量采集步骤创建新的归档版本。
dc exec -T api python -m cdp.archive_s3 backup ORD_20260115_SH001_0001
dc exec -T api python -m cdp.archive_s3 restore ORD_20260115_SH001_0001
```

第一次启动时待对象存储就绪再创建桶。使用真实对象存储时，从受管运行环境提供 SDK 凭据，设置 `CDP_S3_BUCKET`，需要时设置 HTTPS `CDP_S3_ENDPOINT_URL` 和归档前缀。跨目录或跨环境恢复还需先恢复包含版本和摘要关联的数据库，再对缺失归档执行恢复。当前对象存储是备份介质，Kafka 发布仍从本地已校验的归档读取。

### PostgreSQL 与归档联合恢复演练

`cdp.archive_s3 backup --all` 和 `restore --all` 分页处理 V1 全部历史版本，汇总各结果及前 20 条失败样例。存在缺失、旧版本无归档或校验失败时返回非零状态，已成功处理的版本可以安全重跑。

```sh
bash scripts/rehearse-archive-recovery.sh
```

演练在临时 PostgreSQL 和 SeaweedFS 环境中执行。先备份归档对象并用 `pg_dump` 导出数据库，再用 `pg_restore` 写入同一沙箱的空数据库，最后恢复空归档目录和采集检查点，对比版本和摘要，逐版本核验文件，并验证后续增量只读取新增版本。脚本结束后清理临时资源。

实际备份时，应暂停对目标数据集的写入，保证对象备份和 PostgreSQL 导出对应同一批版本。此演练覆盖数据库、V1 归档与采集检查点。Kafka 存量、Spark 持久卷和跨地域切换仍需单独演练。管理目标存储、保留周期和恢复时间需按生产场景验证。


采集检查点独立导出为带 SHA-256 的版本化 JSON。应在采集暂停时与 PostgreSQL 导出和归档备份配套保存。

```sh
docker compose exec -T api python -m cdp.collection_state export > collection-checkpoint.json
# 将文件放入恢复容器可读的位置，再执行恢复。
docker compose exec -T api python -m cdp.collection_state restore /path/collection-checkpoint.json
```

恢复拒绝校验失败、重复来源或与目标库不同的已有进度。对同一检查点重复恢复会报告当前已一致。恢复前应确认源标识和数据库快照属于同一批备份。


### I2 PostgreSQL 主库切换与备份恢复

```bash
bash scripts/rehearse-postgres-primary-recovery.sh
```

脚本在独立 Compose 环境创建 PostgreSQL 16 主库、同步流复制备库和单独的恢复库，使用 `pg_verifybackup` 检查基础备份。在备份完成后再提交两笔合成订单，并强制切换 WAL 段，确认归档后终止主库。随后提升备库，同时保留基础备份和 WAL，供另一个空库独立恢复。两条路径都核对故障前已确认的订单键和金额，再提交并读取一笔新订单。

演练分别报告从故障注入到备库可写、从开始恢复到恢复库可写的时间，以及故障时最后一个经验证恢复点的年龄和已确认订单损失。沙箱使用本地卷保存备份与 WAL，强制归档形成已知的恢复点。目标环境的自动切换、对象存储持续归档及真实负载下的 RTO 和 RPO 仍需验收。

### Kafka 历史重建

恢复数据库及本地归档后，可重建独立 Kafka 主题。为每次恢复选用新的运行 ID，并预先创建对应的空主题。以下命令在包含 Kafka 的开发 Compose 项目中执行。

```sh
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
  --create --topic billing.orders.v1.recovery.drill-01 --partitions 3 --replication-factor 1
docker compose exec -T api python -m cdp.kafka_recovery --run-id drill-01 --limit 100
```

按同一 ID 重跑直到 `remaining` 为 0。恢复进度按 Kafka 集群和恢复主题单列，原发布确认保留，原主题不接收重建消息。已有归档先校验 SHA-256，缺归档的旧版本计入 `legacy_without_archive` 并返回非零状态。Kafka 确认丢失时仍可能发生至少一次投递，消费者可按原 `event_id` 去重。若删除并重建主题，请使用新的运行 ID。需要固定恢复范围时，在恢复快照的独立数据库上执行。

```sh
bash scripts/rehearse-kafka-recovery.sh
```

该演练验证归档重放、断点续跑、发布标记和原主题隔离。生产消费者切换、Kafka 自身保留期、Spark checkpoint 恢复需单独演练。

#### 消费恢复主题

恢复主题写入完成后，可用独立消费组将归档事件重放到指定目标数据库。为每个目标数据集选择唯一的目标 ID，同一次恢复的后续批次复用该 ID。

```sh
docker compose exec -T api python -m cdp.consume \
  --recovery-run-id drill-01 --recovery-target-id recovered-db --limit 100
```

恢复组 ID 由运行 ID 和目标 ID 组成。普通消费组和原主题保持不变。恢复指向的数据库已有对应事件时，按 `event_id` 去重；空目标库用新组从恢复主题起始处读取。每条事件仍先写入数据库、再提交 Kafka 偏移。大范围重建可能因分区到达顺序而产生待回填事件，应核对迟到队列并按 UTC 日补算后再比较窗口。当前恢复不会复用旧 Kafka 消费偏移或 Spark checkpoint。

#### 用 Spark 读取恢复主题

用已构建的 Spark 镜像，指定与恢复主题和目标库匹配的两个 ID。把 `CDP_POSTGRES_DSN` 配为目标库的受管连接。

```sh
docker compose -f compose.yaml -f compose.postgres.yaml -f compose.kafka.yaml -f compose.spark.yaml \
  run --rm --no-deps -e CDP_POSTGRES_DSN spark --once \
  --recovery-run-id drill-01 --recovery-target-id recovered-db
```

Spark 自动派生独立 checkpoint，格式为 `<原 checkpoint>.recovery/<集群 ID>.<恢复运行 ID>.<目标 ID>`。同一恢复重启沿用其 checkpoint。目标数据集变化时使用新的目标 ID，防止继承旧目标的读取进度。恢复期间使用独立目标库，并在校验窗口和迟到回填后再考虑切换。

```sh
bash scripts/rehearse-kafka-recovery.sh --spark
```

扩展演练验证普通 checkpoint、恢复 checkpoint 与目标库隔离。使用新主题时从头重建 Spark 进度，已有普通 checkpoint 不迁移到新主题。

#### 恢复验收

在原库和目标库写入都暂停、待回填已处理的情况下，使用只读连接检查覆盖全部恢复数据的 UTC 日期范围。源库沿用 `CDP_POSTGRES_DSN`，目标库连接从 `CDP_RECOVERY_TARGET_DSN` 提供。SQLite 开发库可用 `--target-data-dir`。

```sh
docker compose exec -T -e CDP_RECOVERY_TARGET_DSN api python -m cdp.recovery_check \
  --from-day 2026-01-14 --through-day 2026-01-15 --stream-state
```

验收对比事件标识、载荷摘要、窗口计数和待回填状态。窗口总量一致但事件不同，仍判为失败。退出码为 0 表示匹配，2 表示不匹配或两边均无数据，1 表示无法读取数据库。日期范围用于生命周期事件和待回填状态。加上 `--stream-state` 后，还会比对整条流的最大事件时间、水位和隔离记录摘要。即使窗口一致，隔离记录缺失或水位不一致也不会通过。财务指标、预测结果与真实生产流量切换需要独立核验。

#### 补齐恢复库的隔离记录和水位

恢复窗口后，可从原库导出带 SHA-256 的流状态快照。它包含当前水位、隔离记录的 ID 和摘要，以及整条流中非隔离事件及待回填状态的指纹。文件不包含消息正文。

```sh
docker compose exec -T api python -m cdp.stream_snapshot export > stream-state.json
# 将文件放入目标容器可读的位置，并将连接指向恢复库。
docker compose exec -T -e CDP_POSTGRES_DSN api python -m cdp.stream_snapshot restore /path/stream-state.json
```

恢复仅在目标已有相同事件及待回填状态时执行。命令在同一事务中补齐缺失的隔离记录并恢复水位，拒绝覆盖存在冲突的隔离记录。相同文件可重复恢复，随后使用 `cdp.recovery_check --stream-state` 验收。实际恢复前应暂停对应流的写入，并使用可信备份文件。

### 启用开发驾驶舱的只读访问控制

可在本机或受 TLS 保护的内部入口叠加 `compose.access.yaml`，保护驾驶舱、静态资源及 `/v1/demo/*` 查询。先生成高熵登录令牌。

```sh
docker compose run --rm --no-deps api python -m cdp.dashboard_access reviewer
```

在安全终端运行并保管输出中的 `token`。只将 `verifiers` 对象的 JSON 配为 `CDP_DASHBOARD_USERS_JSON`，使用独立用户键可添加多个只读账户。设置环境变量后启动服务，浏览器用用户名和令牌登录。

```sh
docker compose -f compose.yaml -f compose.access.yaml up -d --no-build --wait
```

配置无效时 API 停止启动。更新或撤销令牌需要重新加载 API 容器环境。

审计日志记录身份、资源类别、方法和状态码，未授权请求不记录对方提交的用户名。凭据、查询参数及返回数据不进入这条审计日志。健康检查和 Prometheus 抓取维持内部可用。该可选配置适用于开发演示，生产仍需接入统一身份和数据权限。

账户可限定可见站点。在 `CDP_DASHBOARD_USERS_JSON` 中，旧的用户名到摘要字符串格式保留全站只读权限。站点级账户使用对象格式。

```json
{
  "station_reader": {
    "verifier": "<生成命令返回的 SHA-256 摘要>",
    "stations": ["ST_SH_001"]
  }
}
```

日报、事件窗口和预测在服务器端按授权站点筛选，`stations: []` 只返回空结果。采集质量涉及全局来源，仅全站账户可查。监控与健康检查属于运维入口，部署时应限制网络访问。本配置是演示用应用权限，数据库角色与正式统一身份需要另行配置。

### 将 API 查询与 PostgreSQL 写入分离

可选的 `compose.db-access.yaml` 为 PostgreSQL 开发沙箱增加专用只读角色。初始化容器先以写入账号完成迁移和授权，API 容器仅收到只读连接，数据卷以只读方式挂载。

```sh
dc() { docker compose -f compose.yaml -f compose.postgres.yaml -f compose.db-access.yaml "$@"; }
dc up -d --no-build --wait api
dc run --rm --no-deps db-access-init python -m cdp.cli samples/order.json
```

使用自备只读角色时，先在独立写入任务中执行数据库迁移，再设置 `CDP_API_POSTGRES_DSN` 启动 API。指定只读连接后，API 不执行迁移，并在启动时检查高权限角色、公共 schema 建表权和业务表写入权。API 对所有数据库事务启用只读模式。启动和 `/health/ready` 检查 `cdp/postgres.py` 中列出的查询关系是否存在且可读取。Compose 中的账号仅供本地开发。生产凭据、角色管理及数据库网络权限应由部署环境提供。

在独立临时数据库验证账户权限与 API 查询，可运行 `bash scripts/rehearse-api-reader.sh`。演练检查日报、流窗口、预测、质量和监控查询，以及业务表更新、公共 schema 建表、归档卷写入均被拒绝。
