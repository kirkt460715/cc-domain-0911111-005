# Hangar Ingest 维修测量数据处理

将设备观测和维修工单归入可重建的日分区，记录时钟校准、隔离复核、水位和数据发布。接收位置和提交版本分别保存；校准更正会影响分区有效性。

## 维修有效性研究

围绕「更换部件后振动是否改善」的复盘场景，`hangar_ingest.studies` 以工单完成时刻为锚点对齐前后观察窗，而不是按自然日粗略比较：

- 筛选维度：工单类型（`order_types`）、部件（`components`）、指标（`metrics`）、机型（`aircraft_types`，需先 `fleet.register`）、单机（`aircraft`）、窗口长度与完成时间区间。
- 配对规则：每个关闭工单与其部件上的每个匹配指标形成一个事件；前窗为 `[完成时刻-窗口, 完成时刻)`，后窗为 `[完成时刻, 完成时刻+窗口)`，维修作业期不进入基线。
- 逐条排除：重叠工单（影响无法隔离）、校准覆盖缺口、重采样覆盖率不足、有效桶数不足；每个排除事件保留结构化原因，可用 `effectiveness.explain` 查看中文说明。
- 按飞机汇总：同一飞机同一指标的多个事件先合并为一个飞机效应（事件差值的中位数），机队统计只对飞机计数，避免读数密集或重复维修的飞机被当成多个样本。
- 不确定区间：飞机数达到 `minimum_aircraft`（默认 5）且至少 2 架时，对飞机整群做确定性 bootstrap 给出均值/中位数的百分位置信区间；否则只输出描述值并注明原因。
- 版本冻结：每次运行冻结请求参数、工单修订号、校准修订号和机队注册修订号，结果按 `study_id` 留存为不可变修订；同输入重跑返回 `unchanged`，工单或校准更正后重跑产生新修订。
- 更正追踪：`effectiveness.compare` 对比两个修订，列出新增/移除/变化的事件、工单修订变化和校准修订变化，旧修订仍可通过 `effectiveness.get` 调阅。

研究相关命令：`fleet.register`、`fleet.get`、`fleet.list`、`effectiveness.run`、`effectiveness.get`、`effectiveness.list`、`effectiveness.revisions`、`effectiveness.compare`、`effectiveness.explain`。

## 运行环境

使用 Python 3.11，核心功能依赖标准库和 SQLite。演示和测试只使用本地合成数据，不需要账户、设备或外部运行服务。请从项目根目录执行命令。

## 检查与运行

构建检查验证语法，测试使用临时数据目录，不会连接远端服务。

```sh
python3 -m compileall -q hangar_ingest
python3 -m unittest discover -s tests -v
python3 -m hangar_ingest --db :memory: demo --fixture fixtures/demo.json
python3 -m hangar_ingest --db :memory: study-demo
```

`study-demo` 构造五架同机型飞机的合成振动数据：四架形成有效配对并输出整群重采样区间，一架因重叠工单被排除；随后修正一架飞机的校准，生成第二修订并给出变化事件清单。

演示会展示产品的实际数据处理和输出，运行结束自行退出。查看现有接口：

```sh
python3 -m hangar_ingest commands
```

## 目录与数据

hangar_ingest/ 保存领域逻辑和请求入口，tests/ 包含正常行为、边界与恢复测试。例子中的标识和记录为合成资料，不包含真实人员或生产测量。业务目录和本地数据库由调用者指定，应与源码分开保存。

## 干净环境复现

容器定义固定运行时版本。构建时准备环境，运行时无需网络；每次启动会复制只读源码到临时工作目录，测试产生的文件不会写回仓库。

```sh
docker build -t local-product-check .
docker run --rm --network none local-product-check
```
