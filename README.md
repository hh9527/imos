# IMOS

IMOS（Immutable Object Store）是一个面向上层系统的本地制品存储，提供确定性的下载、安装、复用和垃圾回收。

项目由 [RFC 0001](rfc/0001-mvp.md) 和 [RFC 0002](rfc/0002-async-serv.md) 驱动。当前 MVP 面向本地 Unix 文件系统，不提供包依赖求解、远程缓存或用户级/系统级两层 store。

## 工作方式

一次性 CLI 可以读取一个已经写完的 plan 文件：

```console
$ imos --store /path/to/store create /path/to/plan.json
/path/to/store/install/<key-hash>/root
```

`serve` 的上层系统直接提交 `home` 和完整 Plan JSON；IMOS 将 Plan 确定性编码为紧凑 JSON，使用全新的临时 inode 原子替换 `home/<plan.name>`。IMOS 不比较新旧内容，内容与 key 的确定性关系由上层保证。同一目标文件的请求串行执行，不同目标仍可并发。两种入口最终都由 IMOS 为 plan 文件创建 hard link，以 inode 标识意愿，并使用 SQLite 保存两类关系：

```text
request inode → plan key
plan key → download key
```

SQLite 不记录下载或安装是否完成。最终文件系统路径存在，才表示对应对象已经完整发布。并发操作由永久文件锁协调，下载和安装结果通过临时目录加原子 rename 发布。

## 命令

```text
imos --store <path> create <plan-file>
imos --store <path> remove <plan-file>
imos --store <path> gc
imos --store <path> serve [-e|--events-to-stderr]
```

- `create`：读取 plan、下载和安装缺失对象，并注册上层意愿；重复提交同一 inode 是幂等操作。
- `remove`：主动移除 inode 对应的意愿，不立即删除对象，也不删除上层 plan 文件。
- `gc`：汇总所有仍然存活的意愿，回收其余安装、下载和临时对象。
- `serve`：通过 stdin 持续接收 JSONL 安装请求，通过 stdout 输出可按 ID 关联的终态。

上层也可以直接删除 plan 文件。内部 hard link 的 `nlink` 回落后，下一次 GC 会识别并移除该意愿。

## stdio 服务

`serve` 是供上层应用管理和调用的机器接口，不提供交互式或面向人的输出。多个请求可以同时在途，同 key 的实际下载和安装会由 store 锁合并。

默认模式只使用 stdout：每个已接受请求最终输出一个 `result` 或 `error`，不输出进度，stderr 保持为空。遇到无效 JSON、错误 shape、空 ID 或重复在途 ID 时，stdout 输出一条 JSONL 错误，随后服务立即中止并非零退出。

指定 `-e` 或 `--events-to-stderr` 后，请求完成结果仍写 stdout，可按 key 归约的 Status 和可恢复的协议错误写 stderr。两条流都只包含 JSONL；上层应用必须同时持续消费它们。

输入：

```json
{"type":"Install","id":"request-42","home":"/path/to/upstream-home","plan":{"version":1,"name":"tool.json","key":"tool-v1","items":[]}}
```

stdout 完成结果示例：

```json
{"id":"request-42","type":"result","root":"/path/to/store/install/.../root"}
```

`serve -e` 的 stderr Status 示例：

```json
{"schema":"telora/status","type":"Download","key":"tool-v1","name":"Tool archive","status":"Running","tried":1,"started":"2026-08-29T10:20:30Z","bytes":1048576,"totalBytes":8388608}
```

Status 不包含请求 ID，每行都是 `key` 的完整最新快照；`type` 支持 `Install`、`Download` 和 `Unpack`，`status` 支持 `Waiting`、`Running`、`Completed` 和 `Failed`。Download 和 Unpack 都可以携带 `bytes` 与 `totalBytes`。请求失败终态使用 `type: "error"` 和英文 `message`。下载、校验、展开和安装失败都通过 stdout 带原请求 ID 返回，不终止服务。每个成功进入执行的请求恰好输出一个 `result` 或 `error` 终态。stdin EOF 后，进程等待所有在途请求结束再退出。完整协议见 RFC 0002。

## 异步模型

CLI 和 store 执行接口运行在 Tokio runtime 上。网络下载、本地文件复制、stdio、进度传递和文件锁等待采用异步执行；SQLite、归档处理和必须使用同步系统调用的短任务由 Tokio blocking 线程池承载，不阻塞 runtime worker。

请求和 Status 使用 reducer/effect 队列编排：stdin、网络和文件处理结果作为 Event 进入 reducer，reducer 只更新 State 并产生 Effect，Effect 执行 I/O 后再返回 Event。stdout 请求终态和 stderr Status 都通过 Effect 输出，不存在绕过 reducer 的 Reply 通道。

## Plan v1

Plan 是 JSON 文档。IMOS 从完整 JSON Value 中提取 `version`、`name`、`key` 和 `items`；其他顶层字段由上层系统使用，并由 IMOS 纳入确定性落盘内容。

```json
{
  "version": 1,
  "name": "tool-1_0.json",
  "key": "tool-1_0-linux-x86_64",
  "items": [
    {
      "name": "Tool archive",
      "key": "tool-archive-1_0-linux-x86_64",
      "kind": {
        "type": "UnpackDir",
        "url": "https://example.invalid/tool.tar.zst",
        "size": 123456,
        "digest": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "archive": "TarZstd",
        "strip": 1,
        "to": "."
      }
    }
  ],
  "upstream": {
    "package": "tool",
    "version": "1.0"
  }
}
```

MVP 支持：

- 下载来源：`file`、`http`、`https`；
- 下载断言：可选 `size` 和 `sha256` digest；
- 归档：`Tar`、`TarGzip`、`TarZstd`；
- item kind：`UnpackDir`、`UnpackFile`、`InstallFile`、`InstallBin`。

HTTP/HTTPS 下载使用 Rustls，并默认读取平台原生证书库和平台代理配置；`HTTP_PROXY`、`HTTPS_PROXY`、`ALL_PROXY`、`NO_PROXY` 及对应小写环境变量按 Reqwest 的平台惯例处理。MVP 不提供关闭证书校验的选项。

Plan 和 Item 的 `name` 必须是 1 到 64 字节的 UTF-8 字符串；Plan name 同时是 request 文件名，必须是单个安全路径组件。`key` 用于寻址，最长 64 个 ASCII 字节，必须匹配 `[a-z][0-9a-z]*([-_][a-z0-9]+)*`，不允许使用 `.`。download key 在整个 store 内全局唯一；不同 key 在安装前并发下载，随后严格按 `items` 顺序执行。所有 `to` 都是不可变安装 root 内的安全相对路径。

## 构建与验证

```console
$ cargo build
$ cargo test
$ cargo clippy --all-targets --all-features -- -D warnings
```

## 文件系统约束

- 一次性 CLI 的 plan 文件必须是已经完成写入的普通文件；`serve` 每次都由 IMOS 在既有 `home` 目录中创建新 inode，再原子替换 plan 文件；
- plan 文件与 store 必须位于同一文件系统；
- 新 plan 文件必须只有一个外部 hard link；
- store 必须位于支持 hard link、advisory file lock 和原子 rename 的本地 Unix 文件系统；
- store 由 IMOS 独占管理，外部系统不能直接修改其内容。
