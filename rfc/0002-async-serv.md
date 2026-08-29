# RFC 0002：异步执行与 stdio JSONL 服务协议

- 状态：已接受
- 跟踪 issue：#2
- 创建日期：2026-08-29
- 修订 issue：#3

## 摘要

IMOS 的执行核心迁移到 Tokio 异步模型，并新增 `imos serv`。`serv` 从 stdin 持续接收 JSON Lines 安装请求，通过 stdout 输出带请求 ID 的进度和结果事件。多个请求可以并发执行，同一 key 的下载与安装仍由 store 文件锁合并。

`serv` 是由上层系统管理生命周期的 stdio 子进程，不是监听 socket 的后台守护进程。一次性 `create`、`remove` 和 `gc` 命令继续保留，并调用同一套异步核心。

## 动机

RFC 0001 的同步 CLI 有两个限制：

1. HTTP 下载、锁等待和整条安装调用链会阻塞调用线程；
2. 上层系统每次安装都要启动单独进程，无法在一个稳定会话中提交多个请求并持续获得结构化进度。

IMOS 需要支持异步上层系统自然地复用一个子进程，同时维持原有的确定性、跨进程协调和崩溃安全保证。

## 目标

1. CLI 和 store 的公开执行接口使用 Tokio 异步模型；
2. HTTP、HTTPS、本地文件复制、stdio 和锁等待不阻塞 Tokio worker；
3. SQLite、归档处理及无法异步化的文件系统操作在受控 blocking 任务中执行；
4. `serv` 支持持续输入、多个在途安装请求和结构化进度；
5. 每个有效请求有且仅有一个可关联的成功或失败终态；
6. 保持 RFC 0001 的 store 布局、意愿模型、原子发布及 GC 语义。

## 非目标

- socket、HTTP 或其他网络服务协议；
- 服务发现、自动拉起、脱离上层进程运行；
- 请求取消、优先级和持久队列；
- 通过 `serv` 执行 `remove` 或 `gc`；
- 跨进程汇总所有历史进度；
- 修改 plan v1 的数据结构。

## 异步执行模型

程序入口运行在 Tokio 多线程 runtime 上。`Store::open`、`create`、`remove` 和 `gc` 是异步接口。

以下工作使用原生异步 I/O：

- HTTP 和 HTTPS 请求及响应体读取；
- 本地下载源的读取和临时下载文件写入；
- stdin 请求读取和 stdout 事件写入；
- 文件锁竞争时的定时等待；
- 进度事件传递和任务编排。

以下工作允许通过 `tokio::task::spawn_blocking` 运行：

- SQLite 连接和事务；
- tar、gzip、zstd 解码及安装树构建；
- 摘要复核等纯 CPU 密集工作；
- hard link、metadata、rename、权限修改、目录扫描及文件锁系统调用等没有稳定 Tokio 接口或需要作为短小原子步骤执行的操作。

blocking 任务不得等待网络、锁竞争或 stdio。异步任务不得持有 SQLite 事务跨越 `.await`。

同一 `Store` 可以被多个任务共享。正确性仍来自跨进程文件锁和 SQLite 事务，不能只依赖进程内互斥。

## CLI

外部命令变为：

```text
imos --store <path> create <plan-file>
imos --store <path> remove <plan-file>
imos --store <path> gc
imos --store <path> serv
```

前三个命令保持 RFC 0001 的界面和结果。`create` 的人类可读进度写 stderr，最终安装路径写 stdout。

`serv` 的 stdin 和 stdout 专用于下述 JSONL 协议。stdout 不得出现日志、欢迎信息或非协议内容，stderr 必须保持为空。所有能够表达的结果和诊断都编码为 stdout JSONL 事件。

## 请求协议

每个非空输入行是一个完整 JSON 对象：

```json
{"id":"request-42","plan_file":"/absolute/path/to/plan.json"}
```

shape 为：

```text
InstallRequest {
  id: String,
  plan_file: Path
}
```

- `id` 由调用方生成，在当前 `serv` 进程的在途请求中必须唯一，不能为空；完成后可以复用。
- `plan_file` 使用进程文件系统命名空间中的路径，可以是绝对路径或相对当前工作目录的路径。
- 未知字段、缺少字段、字段类型错误及无效 UTF-8 输入均视为协议错误。
- 空行忽略。

一行错误不能终止服务。能够读取字符串 `id` 时，错误事件使用该 ID；否则使用 `id: null`。重复的在途 ID 被拒绝，但不影响先到达的请求。

## 输出协议

每个输出行是一个完整 JSON 对象。不同请求的事件允许交错；同一请求的事件顺序与实际执行顺序一致。

进度事件：

```json
{"id":"request-42","type":"progress","stage":"download","dl_key":"tool-v1","current":1048576,"total":8388608}
{"id":"request-42","type":"progress","stage":"install","plan_key":"example-plan-key"}
```

成功终态：

```json
{"id":"request-42","type":"result","root":"/store/install/…/root"}
```

失败终态：

```json
{"id":"request-42","type":"error","message":"download size mismatch"}
```

`id` 仅在无法关联输入行的协议错误中为 `null`。`type` 为 `progress`、`result` 或 `error`。`progress.stage` 在本 RFC 中可以是 `waiting`、`download` 或 `install`，并可携带与阶段相关的 `plan_key`、`dl_key`、`current`、`total` 和 `cached` 字段。

每个通过 shape 校验且成功进入执行的请求必须恰好产生一个 `result` 或 `error` 终态。进度是提示信息，调用方不得依赖特定数量，也不得用缺少进度判断请求失败。

下载源不可达、HTTP 非成功状态、读取失败、size 或 digest 校验失败、归档格式错误、展开失败和安装目标冲突，都是可预期的请求级失败。它们必须产生带原请求 `id` 的 `error` 终态，不得写入 stderr，不得终止服务，也不得阻止其他请求继续执行。stdin 正常 EOF 后，即使一个或多个请求失败，只要所有已接收请求都已输出终态且 stdout 正常 flush，服务仍然成功退出。

无法关联具体请求的输入或服务级错误使用 `id: null` 的 `error` 事件。store 初始化失败发生在协议循环启动前，但只要 stdout 可用，也必须以该形式报告。只有 stdout 本身关闭或写入失败时无法返回 JSONL 错误，此时服务直接非零退出，仍不使用 stderr。

所有输出由单一 writer task 串行编码、写入并 flush，防止并发任务产生半行或交错字节。内部使用有界队列施加背压；stdout 消费缓慢时，生产任务异步等待队列容量，不阻塞 runtime worker。

如果 stdout 关闭或写入失败，服务停止读取新请求并结束进程；不承诺为尚未输出终态的请求继续工作。

## 生命周期与并发

`serv` 每读到一个有效请求就启动一个异步安装任务，不要求前一个请求完成。文件锁负责合并同 plan key 或 download key 的实际工作，等待者获得 `waiting` 进度，并在首次执行者完成后复核最终对象。

stdin 到达 EOF 后，服务停止接收新请求，等待所有已接收任务输出终态，随后关闭输出队列并在 stdout flush 完成后成功退出。

本 RFC 不规定固定并发上限。实现可以使用信号量限制同时执行的请求和 blocking 工作，但限制不得改变协议及正确性语义。

收到 SIGINT、SIGTERM 或 SIGKILL 时不保证输出终态；store 的恢复保证仍由 RFC 0001 的临时目录、原子发布和进程退出自动释放文件锁提供。

## 进度来源

进度事件同时服务于当前调用者和同 key 的等待者：

- 首次执行者将事件写入对应的永久锁文件，并发送给本请求；
- 等待者增量读取锁文件中完整的 JSONL 行，转换为本请求 ID 下的 `progress` 事件；
- 等待者获得锁后重新检查最终对象，不盲信锁文件中的 `completed`；
- 每次成为首次执行者时截断锁文件，随后 append-only 写入本次操作事件。

锁文件仍是临时进度与诊断载体，不是结果状态数据库。崩溃留下的不完整末行必须忽略。

## 兼容性

Plan v1、SQLite schema 和 store 布局保持不变。旧版本创建的对象可以直接复用，新版本产生的对象也不要求数据库迁移。

RFC 0001 中“后台服务与更丰富的进度订阅”为后续演进的描述，被本 RFC 的 stdio 子进程服务部分取代；本 RFC 不引入脱离上层进程管理的 daemon。

## 验收标准

1. 源码不使用 `reqwest::blocking` 或阻塞式等待循环；
2. 一次性 `create`、`remove`、`gc` 行为与 RFC 0001 兼容；
3. `serv` 能在同一 stdin 流中处理多个安装请求；
4. 两个并发请求的事件可以交错，但每行完整且都能按 ID 关联；
5. 同 key 并发请求只执行一次实际下载并全部成功返回；
6. 无效 JSON、错误 shape 和重复在途 ID 各自产生错误，后续请求仍可执行；
7. stdin EOF 后等待在途请求完成再退出；
8. stdout 关闭时服务不继续无限执行或挂起；
9. 下载、digest 校验和展开失败分别产生 stdout `error` 终态，服务继续处理后续请求并在正常 EOF 后成功退出；
10. `serv` 的 stderr 在请求成功、请求失败和服务级错误时均保持为空；
11. 既有崩溃恢复、GC 和归档安全测试继续通过；
12. `cargo fmt`、`cargo clippy --all-targets --all-features -- -D warnings` 和全部测试通过。
