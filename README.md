# 限定邮箱 TCP 服务

Go 实现的自定义文本邮箱协议（IMAP 风格子集），SQLite 持久化。

## 运行

```sh
go run . -addr 127.0.0.1:9143 -db mailbox.db
# 测试
go test -race ./...
```

## 协议

命令均以 CRLF 结尾，首字段为客户端标签。支持：

| 命令 | 说明 |
| --- | --- |
| `TAG SELECT mailbox` | 选中邮箱，返回 EXISTS / UIDVALIDITY / UIDNEXT / REVISION；同一邮箱的多个连接按修订顺序收到变更通知 |
| `TAG APPEND mailbox (flags) {N}` + N 字节正文 + CRLF | 按声明字节数读取正文（正文中的 CRLF、命令样式文字均为载荷） |
| `TAG UID FETCH uid-set (UID FLAGS BODY[])` | 按 **UID** 查询；响应含当前序号、UID、标记、正文 |
| `TAG UID STORE uid-set FLAGS/+FLAGS/-FLAGS (...) [.SILENT]` | 按 UID 改标记 |
| `TAG EXPUNGE` | 永久删除带 `\Deleted` 的消息，按删除前序号升序上报 |
| `TAG NOOP` / `TAG LOGOUT` | 心跳 / 退出 |

示例：

```
A1 APPEND INBOX (\Seen) {11}
hello world
A1 OK [APPENDUID 1 1] [REVISION 1] APPEND completed
```

## 关键语义

- **UID 单调递增、删除后不复用**；`uidnext` 与 `revision` 存在 `mailboxes` 表，
  消息存 `messages(mailbox_id, uid, flags, body)`。
- **序号是查询/删除时刻按 UID 排序的位置**，删除会重排；所有修改命令只接受
  UID 集合，因此后续命令不会把重排后的序号错当成旧消息身份。
- 每次提交（APPEND / STORE 实际变化 / EXPUNGE）令邮箱 `revision` 加一；
  同一邮箱的修改经每邮箱互斥锁串行化，通知与该次提交原子发布，所有选中该邮箱
  的连接看到相同的修订顺序。通知与命令回复走同一连接输出通道，顺序不错乱。
- APPEND 只有在 **N 个声明字节与结尾 CRLF 全部读完后** 才在单个事务中插入；
  客户端中途断开不会留下半条消息或空邮箱（空邮箱仅在首次成功追加时创建）。
- 命令可任意分片到达（`bufio.Reader` 流式读取），也可在一个 TCP 段中连续发送多条。
- 限制：单封正文 ≤ 16 KiB；每箱 ≤ 200 封。超限返回带标签的 `NO`
  （超限字面量仍按声明字节读完以保持命令流同步；超过 1 MiB 的声明直接断连）。

## 代码结构

- `main.go`：服务、SQLite schema、事务化的 APPEND/STORE/EXPUNGE/FETCH。
- `session.go`：TCP 连接、命令分帧（长度前缀字面量）、分片读取。
- `protocol.go`：命令解析与响应在同一帧内组装。
- `hub.go`：按邮箱的订阅者与修订事件发布。
- `flags.go` / `tokenizer.go` / `events.go` / `util.go`：辅助逻辑。
- `server_test.go`：两个真实 TCP 客户端交错追加/改标记/删除、通知顺序、
  UID 查询、分片与连发、16 KiB/200 封限制、中途断开不留消息。


## UID MOVE
The selected mailbox accepts `tag UID MOVE uid-set destination`. Source and destination must already exist and differ (mailbox identity is case-insensitive). Every requested UID must exist, and duplicates are invalid; a valid nonempty batch moves exactly those messages regardless of Deleted flag. Bodies and flags are copied byte-for-byte to fresh consecutive destination UIDs in ascending source UID order, then removed from source. Destination UID allocation never reuses an earlier value. Success returns COPYUID destination-validity old-UID-list new-UID-list; source expunge sequence numbers account for earlier removals in the same batch. Each mailbox advances exactly one revision for the whole move. Selected sessions observe a committed source expunge batch or destination append batch, without intermediate per-message states; the actor receives source changes with its response. Capacity, invalid selection, missing mailbox or any storage failure must leave both mailboxes, UIDNEXT, revisions and notification streams unchanged. Restart preserves mapping results and subsequent allocations. Existing APPEND/FETCH/STORE/EXPUNGE retain their command behavior.
