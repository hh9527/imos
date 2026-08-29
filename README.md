# IMOS

IMOS（Immutable Object Store）是一个面向上层系统的本地制品存储，提供确定性的下载、安装、复用和垃圾回收。

项目由 [RFC 0001](rfc/0001-mvp.md) 和 [RFC 0002](rfc/0002-async-serv.md) 驱动。当前 MVP 面向本地 Unix 文件系统，不提供包依赖求解、远程缓存或用户级/系统级两层 store。

## 工作方式

上层系统创建一个写完后不再修改的 plan 文件，并将它提交给 IMOS：

```console
$ imos --store /path/to/store create /path/to/plan.json
/path/to/store/install/<key-hash>/root
```

IMOS 为 plan 文件创建 hard link，以 inode 标识意愿，并使用 SQLite 保存两类关系：

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
imos --store <path> serv
```

- `create`：读取 plan、下载和安装缺失对象，并注册上层意愿；重复提交同一 inode 是幂等操作。
- `remove`：主动移除 inode 对应的意愿，不立即删除对象，也不删除上层 plan 文件。
- `gc`：汇总所有仍然存活的意愿，回收其余安装、下载和临时对象。
- `serv`：通过 stdin 持续接收 JSONL 安装请求，通过 stdout 输出可按 ID 关联的进度和终态。

上层也可以直接删除 plan 文件。内部 hard link 的 `nlink` 回落后，下一次 GC 会识别并移除该意愿。

## stdio 服务

`serv` 由上层系统作为子进程管理。stdin 和 stdout 只承载 JSONL 协议，诊断写入 stderr。多个请求可以同时在途，同 key 的实际下载和安装会由 store 锁合并。

输入：

```json
{"id":"request-42","plan_file":"/path/to/plan.json"}
```

输出示例：

```json
{"id":"request-42","type":"progress","stage":"download","dl_key":"tool-v1","current":1048576,"total":8388608}
{"id":"request-42","type":"progress","stage":"install","plan_key":"tool-plan-v1"}
{"id":"request-42","type":"result","root":"/path/to/store/install/.../root"}
```

失败终态使用 `type: "error"` 和英文 `message`。每个成功进入执行的请求恰好输出一个 `result` 或 `error` 终态；不同 ID 的进度可以交错。stdin EOF 后，进程等待所有在途请求结束再退出。完整协议见 RFC 0002。

## 异步模型

CLI 和 store 执行接口运行在 Tokio runtime 上。网络下载、本地文件复制、stdio、进度传递和文件锁等待采用异步执行；SQLite、归档处理和必须使用同步系统调用的短任务由 Tokio blocking 线程池承载，不阻塞 runtime worker。

## Plan v1

Plan 是 JSON 文档。IMOS 只解释顶层 `imos` 字段；其他顶层字段可由上层系统使用。

```json
{
  "imos": {
    "version": 1,
    "key": "tool-1.0-linux-x86_64",
    "items": [
      {
        "key": "tool-archive-1.0-linux-x86_64",
        "url": "https://example.invalid/tool.tar.zst",
        "size": 123456,
        "digest": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "action": {
          "type": "unpack_dir",
          "kind": "tar_zstd",
          "strip": 1,
          "to": "."
        }
      }
    ]
  },
  "upstream": {
    "package": "tool",
    "version": "1.0"
  }
}
```

MVP 支持：

- 下载来源：`file`、`http`、`https`；
- 下载断言：可选 `size` 和 `sha256` digest；
- 归档：`tar`、`tar_gzip`、`tar_zstd`；
- action：`unpack_dir`、`unpack_file`、`install_file`、`install_bin`。

完整字段、路径规则和归档安全约束见 RFC 0001。

## 构建与验证

```console
$ cargo build
$ cargo test
$ cargo clippy --all-targets --all-features -- -D warnings
```

## 文件系统约束

- plan 文件必须是普通文件，并在提交前完成写入；
- plan 文件与 store 必须位于同一文件系统；
- 新 plan 文件必须只有一个外部 hard link；
- store 必须位于支持 hard link、advisory file lock 和原子 rename 的本地 Unix 文件系统；
- store 由 IMOS 独占管理，外部系统不能直接修改其内容。
