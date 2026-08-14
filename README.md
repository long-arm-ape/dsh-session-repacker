# dsh-session-repacker

把 DSH 会话日志中成千上万个独立 zstd 帧重打包为官方原生两帧形态，大幅加快旧会话打开速度。

## 背景：为什么会话打开慢

DSH 的 JSONL 会话后端（`dsh-session-persistence-jsonl`）**每次写入批处理都单独压缩成一个 zstd 帧**再追加到 `session.jsonl.zstd` 尾部。流式输出期间每 ~200ms 一批，一个长会话动辄上万帧；而读取时必须**逐帧解压**（且后端没有 seek 式后缀读取，无论只要尾部还是全部都要解码整个文件）。

实测（2026-08-14）：本机最大会话 **5.49MB 压缩 = 14,467 帧 / 19,310 事件**，纯解压 ~660ms，真实读取路径约 1.5~3s。

| 会话 | 重打包前 | 重打包后 |
|---|---|---|
| 最大会话 | 14,467 帧 / 5.49MB / ~661ms | **2 帧** / 2.09MB / **~126ms（6 倍）** |
| 全量 24 个旧会话 | 39,911 帧 / 20.08MB | 48 帧 / 10.67MB |

重打包同时把压缩体积减半（单大帧压缩率远好于万个小帧）。

## 原理

把"万帧"日志重打包成官方写入时的原生形态——**头部帧（恰好一行 header）+ 单个事件帧**（其余全部明文拼接后重压一帧）。明文**逐字节不变**，dsh 读取路径完全兼容（`assertZstdHeaderFrame` 只要求第一帧恰为一行 header）。

覆盖一切从磁盘读旧会话的路径：GUI 打开、新对话接续/分支（fork 前需全量解码父会话）、子会话继承。

## 安装

```bash
# 作为 DSH 插件（推荐）：
dsh plugin --profile web add dsh-session-repacker
# 或克隆到 plugins/ 目录 + link 进 profile（见 DSH 插件开发手册）
```

配置（`profiles/<profile>/cordis.patch.yml`）：

```yaml
- id: session-repacker
  config:
    root: 'E:\DeepSeekHarness\sessions'   # 会话根目录（必填）
    minFrames: 200        # 帧数低于此值不处理（默认 200）
    minAgeMs: 600000      # mtime 距今不足此值不处理，避开活跃写入（默认 10 分钟）
    intervalMs: 3600000   # 周期维护间隔毫秒（默认 1 小时）
```

重启服务器后生效：启动时全量扫描一次，之后按 `intervalMs` 周期维护。

## 独立 CLI（无需 DSH 服务）

```bash
node tools/repack.mjs <root>                  # 扫描并重打包
node tools/repack.mjs <root> --dry-run        # 只统计不落盘
node tools/repack.mjs <root> --file <path>    # 只处理指定文件（可多次）
node tools/repack.mjs <root> --min-frames 64 --min-age-ms 60000
```

## 安全设计

1. 只处理 `.jsonl.zstd`；跳过 mtime 距今 < `minAgeMs` 的文件（避开活跃写入）
2. 跳过当前 live 会话（注入 `sessions` 服务时用 `ctx.sessions.list()`）
3. 替换前做**字节级等价校验**（新文件解压明文 == 原始明文）
4. 替换前再次 stat 比对 size/mtime，文件被并发追加则放弃本次
5. torn tail（未完成帧）跳过，交给 dsh 自身的修复路径
6. 临时文件 + fsync + 原子 rename（Windows 下与官方后端同构）

服务器对重打包后的文件追加新帧（`session/end-seed` 等）实测兼容，后续可被再次重打包（幂等）。

## 构建

```bash
npm i
npx -y tsdown@0.22.2 --config ./tsdown.config.ts   # 产物 lib/index.mjs
```

## 注意事项

- 已产生的旧文件帧数定形后无法回溯压缩，只能靠本插件/CLI 重打包；`writeBatchMaxDelayMs` 调大可减少未来日志的帧数（默认 200ms，属耐久性权衡）
- 插件只做维护，不改变任何读取 API 与数据语义

## License

MIT
