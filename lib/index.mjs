import { open, readFile, readdir, rename, stat, unlink } from "node:fs/promises";
import { join } from "node:path";
import { randomBytes } from "node:crypto";
import z from "node:zlib";
import { performance } from "node:perf_hooks";
//#region src/index.mjs
const name = "session-repacker";
const inject = ["sessions", "timer"];
const ZSTD_MAGIC = 4247762216;
const CHECKSUM_PARAMS = { params: { [z.constants.ZSTD_c_checksumFlag]: 1 } };
/**
* 扫描完整 zstd 帧边界（与 dsh 后端同构的字节级扫描，不解压数据块）。
* @returns {{ frames: {start:number,end:number}[], tornStart?: number }}
*/
function scanZstdFrames(buffer) {
	const frames = [];
	let offset = 0;
	let tornStart;
	while (offset < buffer.length) {
		const start = offset;
		if (buffer.length - offset < 4) {
			tornStart = start;
			break;
		}
		if (buffer.readUInt32LE(offset) !== ZSTD_MAGIC) throw new Error(`corrupt zstd frame magic at byte ${offset}`);
		offset += 4;
		if (offset === buffer.length) {
			tornStart = start;
			break;
		}
		const descriptor = buffer.readUInt8(offset);
		offset += 1;
		if ((descriptor & 24) !== 0) throw new Error(`reserved frame-header bit at byte ${offset - 1}`);
		const contentSizeFlag = descriptor >>> 6;
		const singleSegment = (descriptor & 32) !== 0;
		const checksum = (descriptor & 4) !== 0;
		const dictionaryFlag = descriptor & 3;
		const dictionaryBytes = dictionaryFlag === 3 ? 4 : dictionaryFlag;
		const contentSizeBytes = contentSizeFlag === 0 ? singleSegment ? 1 : 0 : 1 << contentSizeFlag;
		const remainingHeaderBytes = (singleSegment ? 0 : 1) + dictionaryBytes + contentSizeBytes;
		if (buffer.length - offset < remainingHeaderBytes) {
			tornStart = start;
			break;
		}
		offset += remainingHeaderBytes;
		for (;;) {
			if (buffer.length - offset < 3) {
				tornStart = start;
				break;
			}
			const blockHeader = buffer.readUIntLE(offset, 3);
			offset += 3;
			const lastBlock = (blockHeader & 1) !== 0;
			const blockType = blockHeader >>> 1 & 3;
			const blockSize = blockHeader >>> 3;
			if (blockType === 3) throw new Error(`reserved block type at byte ${offset - 3}`);
			const payloadBytes = blockType === 1 ? 1 : blockSize;
			if (buffer.length - offset < payloadBytes) {
				tornStart = start;
				break;
			}
			offset += payloadBytes;
			if (lastBlock) break;
			if (tornStart !== void 0) break;
		}
		if (tornStart !== void 0) break;
		if (checksum) {
			if (buffer.length - offset < 4) {
				tornStart = start;
				break;
			}
			offset += 4;
		}
		frames.push({
			start,
			end: offset
		});
	}
	return tornStart === void 0 ? { frames } : {
		frames,
		tornStart
	};
}
function compressFrame(bytes) {
	return z.zstdCompressSync(bytes, CHECKSUM_PARAMS);
}
function decompressFrame(bytes) {
	return z.zstdDecompressSync(bytes);
}
const yieldToEventLoop = () => new Promise((resolve) => setImmediate(resolve));
/**
* 重打包一个会话日志文件（幂等：帧数已很少或形态已标准时自动跳过）。
* @param {string} file session.jsonl.zstd 绝对路径
* @param {object} [opts]
* @param {number} [opts.minFrames=200] 帧数低于此值不处理
* @param {number} [opts.minAgeMs=600000] mtime 距今不足此值不处理（避开活跃写入）
* @param {Set<string>} [opts.liveIds] 当前 live 会话 id 集合（跳过）
* @param {number} [opts.now=Date.now()]
* @param {number} [opts.yieldEvery=200] 每解压多少帧让出一次事件循环
* @param {boolean} [opts.dryRun] 只统计不落盘
* @returns {Promise<object>} 结果或跳过原因
*/
async function repackFile(file, { minFrames = 200, minAgeMs = 6e5, liveIds = /* @__PURE__ */ new Set(), now = Date.now(), yieldEvery = 200, dryRun = false } = {}) {
	let st;
	try {
		st = await stat(file);
	} catch (error) {
		if (error.code === "ENOENT") return {
			file,
			skipped: "missing"
		};
		throw error;
	}
	if (!st.isFile()) return {
		file,
		skipped: "not-a-file"
	};
	if (now - st.mtimeMs < minAgeMs) return {
		file,
		skipped: "too-recent"
	};
	const buffer = await readFile(file);
	const { frames, tornStart } = scanZstdFrames(buffer);
	if (tornStart !== void 0) return {
		file,
		skipped: "torn-tail"
	};
	if (frames.length < minFrames) return {
		file,
		skipped: "few-frames"
	};
	const header = decompressFrame(buffer.subarray(frames[0].start, frames[0].end));
	if (header.length === 0 || header.indexOf(10) !== header.length - 1) return {
		file,
		skipped: "bad-header"
	};
	const chunks = [];
	let bodyLength = 0;
	for (let i = 1; i < frames.length; i++) {
		const plain = decompressFrame(buffer.subarray(frames[i].start, frames[i].end));
		chunks.push(plain);
		bodyLength += plain.length;
		if (yieldEvery > 0 && i % yieldEvery === 0) await yieldToEventLoop();
	}
	const body = Buffer.concat(chunks, bodyLength);
	const repacked = Buffer.concat([compressFrame(header), compressFrame(body)]);
	const firstFrameEnd = findFrameEnd(repacked);
	const plainA = decompressFrame(repacked.subarray(0, firstFrameEnd));
	const plainB = decompressFrame(repacked.subarray(firstFrameEnd));
	if (!Buffer.concat([plainA, plainB]).equals(Buffer.concat([header, body]))) return {
		file,
		skipped: "verify-failed"
	};
	if (dryRun) return {
		file,
		dryRun: true,
		framesBefore: frames.length,
		framesAfter: 2,
		bytesBefore: buffer.length,
		bytesAfter: repacked.length
	};
	const st2 = await stat(file);
	if (st2.size !== st.size || st2.mtimeMs !== st.mtimeMs) return {
		file,
		skipped: "changed"
	};
	const tmp = `${file}.${randomBytes(6).toString("hex")}.repack.tmp`;
	const handle = await open(tmp, "wx");
	try {
		await handle.writeFile(repacked);
		await handle.sync();
	} finally {
		await handle.close();
	}
	try {
		await rename(tmp, file);
	} catch (error) {
		await unlink(tmp).catch(() => {});
		throw error;
	}
	return {
		file,
		framesBefore: frames.length,
		framesAfter: 2,
		bytesBefore: buffer.length,
		bytesAfter: repacked.length,
		headerBytes: header.length,
		bodyBytes: bodyLength
	};
}
/** 定位第一个 zstd 帧的结束偏移（用于校验时拆分新文件的两个帧）。 */
function findFrameEnd(buffer) {
	const { frames } = scanZstdFrames(buffer);
	if (frames.length < 1) throw new Error("repacked output has no frames");
	return frames[0].end;
}
/**
* 扫描 root 下的全部 session.jsonl.zstd 并重打包符合条件者。
* 目录形态：root/<project-key>/<session-id>/session.jsonl.zstd（深度 ≤ 4）。
*/
async function repackAll(root, opts = {}) {
	const results = [];
	const seen = /* @__PURE__ */ new Set();
	async function walk(dir, depth) {
		if (depth > 4) return;
		let entries;
		try {
			entries = await readdir(dir, { withFileTypes: true });
		} catch {
			return;
		}
		for (const entry of entries) {
			const full = join(dir, entry.name);
			if (entry.isDirectory()) await walk(full, depth + 1);
			else if (entry.isFile() && entry.name === "session.jsonl.zstd") {
				if (seen.has(full)) continue;
				seen.add(full);
				const sessionId = full.split(/[\\/]/).slice(-2, -1)[0] ?? "";
				if (opts.liveIds?.has(sessionId)) {
					results.push({
						file: full,
						skipped: "live"
					});
					continue;
				}
				try {
					const r = await repackFile(full, opts);
					results.push(r);
				} catch (error) {
					results.push({
						file: full,
						skipped: `error: ${error.message}`
					});
				}
			}
		}
	}
	const t0 = performance.now();
	await walk(root, 0);
	const repacked = results.filter((r) => !r.skipped);
	return {
		results,
		summary: {
			root,
			scanned: results.length,
			repacked: repacked.length,
			skipped: results.length - repacked.length,
			totalMs: Math.round(performance.now() - t0),
			framesBefore: repacked.reduce((s, r) => s + (r.framesBefore ?? 0), 0),
			framesAfter: repacked.reduce((s, r) => s + (r.framesAfter ?? 0), 0),
			bytesBefore: repacked.reduce((s, r) => s + (r.bytesBefore ?? 0), 0),
			bytesAfter: repacked.reduce((s, r) => s + (r.bytesAfter ?? 0), 0)
		}
	};
}
/** cordis 插件入口：启动时 + 周期维护。 */
function apply(ctx, config = {}) {
	const root = config.root || join(process.cwd(), "sessions");
	const logger = ctx.logger("session-repacker");
	const minFrames = config.minFrames ?? 200;
	const minAgeMs = config.minAgeMs ?? 6e5;
	const dryRun = config.dryRun === true;
	const liveIds = () => {
		try {
			return new Set(ctx.sessions.list().map((s) => s.id));
		} catch {
			return /* @__PURE__ */ new Set();
		}
	};
	let running = false;
	async function run(reason) {
		if (running) return;
		running = true;
		performance.now();
		try {
			const { summary } = await repackAll(root, {
				minFrames,
				minAgeMs,
				liveIds: liveIds(),
				dryRun,
				now: Date.now()
			});
			if (summary.repacked.length > 0) {
				const saved = summary.framesBefore - summary.framesAfter;
				logger.info(`[${reason}] repacked ${summary.repacked}/${summary.scanned} logs: ${summary.framesBefore} -> ${summary.framesAfter} frames (${saved} fewer), ${(summary.bytesBefore / 1048576).toFixed(1)}MB -> ${(summary.bytesAfter / 1048576).toFixed(1)}MB, ${summary.totalMs}ms`);
			} else logger.info(`[${reason}] nothing to repack (${summary.scanned} logs scanned, ${summary.totalMs}ms)`);
		} catch (error) {
			logger.warn(`[${reason}] repack failed: ${error.message}`);
		} finally {
			running = false;
		}
	}
	if (config.runOnStart !== false) run("startup");
	const intervalMs = config.intervalMs ?? 36e5;
	if (intervalMs > 0) ctx.setInterval(() => void run("interval"), intervalMs);
}
//#endregion
export { apply, inject, name, repackAll, repackFile, scanZstdFrames };
