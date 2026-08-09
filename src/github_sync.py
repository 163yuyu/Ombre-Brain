"""
github_sync.py — GitHub 仓库同步（用于 bucket 数据云端备份）

策略：
- 同步 buckets_dir 下的 .md 记忆和 _sources 下内容寻址的原文证据
- embeddings.db 不上传（二进制，可由 /api/embedding/migrate 重算）
- 使用 GitHub Git Trees API 批量提交（一次同步 = 一个 commit）
- 支持手动触发 + 可选的定时自动同步

依赖：httpx（已在 requirements.txt）
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import os
import tempfile
import uuid
from collections.abc import Iterator, Mapping
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

import httpx
from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from ombrebrain.storage.source_store import (
    HARD_MAX_SOURCE_BYTES,
    SOURCE_REF_RE,
    SourceStore,
    referenced_source_ids_from_markdown,
)
from utils import _win_long_path

logger = logging.getLogger("ombre_brain.github_sync")

_API = "https://api.github.com"
_TIMEOUT = 60.0
_MAX_FILE_BYTES = HARD_MAX_SOURCE_BYTES  # GitHub blob 上限更高；与证据读取硬上限一致
_TREE_CHUNK = 200                  # 每个 /git/trees 请求最多内联多少文件，避免单请求过大
_TREE_CHUNK_BYTES = 2 * 1024 * 1024  # Bound decoded bodies retained by one request.
_MAX_BACKUP_FILES = 10_000
_MAX_BACKUP_PATH_BYTES = 1024
_MAX_MANIFEST_PAYLOAD_BYTES = 4 * 1024 * 1024
_MAX_MANIFEST_BASE64_BYTES = ((_MAX_MANIFEST_PAYLOAD_BYTES + 2) // 3) * 4 + 64 * 1024
_MAX_RESTORE_TOTAL_BYTES = 512 * 1024 * 1024
_MANIFEST_FILENAME = "_ombre_backup_manifest.json"
_SOURCE_REFS_COMPLETE_FIELD = "source_refs_complete"
_PRIVATE_STATE_FILENAME = "_ombre_private_state.enc"
_PRIVATE_STATE_FILES = (".oauth_clients.json", ".dashboard_mcp_tokens.json")
_PRIVATE_STATE_MAGIC = b"OMBRE-PRIVATE-STATE-V1\n"
_PRIVATE_STATE_AAD = b"ombre-brain/private-state/v1"
_PRIVATE_STATE_SOURCE_MAX_BYTES = 8 * 1024 * 1024
_PRIVATE_STATE_MAX_BYTES = 12 * 1024 * 1024
_PRIVATE_STATE_MIN_KEY_BYTES = 32


class _LazyMarkdownFiles(Mapping[str, bytes]):
    """A path index whose file bodies are read only when requested.

    GitHub backup used to retain every Markdown file as ``bytes`` and then
    retain a second decoded ``str`` copy for every tree entry.  On a 512 MiB
    instance that makes the scheduled backup itself an OOM trigger.  Keeping
    only paths here lets ``_batch_commit`` hold at most one bounded chunk of
    bodies while preserving the historical mapping-shaped private API.
    """

    def __init__(self, paths: Mapping[str, str]) -> None:
        self._paths = dict(paths)

    def __len__(self) -> int:
        return len(self._paths)

    def __iter__(self) -> Iterator[str]:
        return iter(self._paths)

    def __getitem__(self, relative_path: str) -> bytes:
        full_path = self._paths[relative_path]
        if os.path.islink(full_path):
            raise RuntimeError(
                f"GitHub backup refuses symbolic-link file: {relative_path}"
            )
        size = os.path.getsize(full_path)
        if size > _MAX_FILE_BYTES:
            raise RuntimeError(
                f"GitHub backup file grew beyond {_MAX_FILE_BYTES} bytes: "
                f"{relative_path} ({size} bytes)"
            )
        # Re-check through a bounded read: a file may grow between stat() and
        # open(), and an unbounded read here would defeat the memory guarantee.
        with open(full_path, "rb") as handle:
            content = handle.read(_MAX_FILE_BYTES + 1)
        if len(content) > _MAX_FILE_BYTES:
            raise RuntimeError(
                f"GitHub backup file grew beyond {_MAX_FILE_BYTES} bytes: "
                f"{relative_path}"
            )
        if _is_source_relative_path(relative_path):
            expected = relative_path.rsplit("/", 1)[-1][:-len(".source")]
            if hashlib.sha256(content).hexdigest() != expected.removeprefix("src_"):
                raise RuntimeError(
                    f"GitHub backup source integrity mismatch: {relative_path}"
                )
            try:
                content.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise RuntimeError(
                    f"GitHub backup source is not UTF-8: {relative_path}"
                ) from exc
        return content


def _is_source_relative_path(relative_path: str) -> bool:
    parts = str(relative_path).replace("\\", "/").split("/")
    return bool(
        len(parts) == 2
        and parts[0] == "_sources"
        and parts[1].endswith(".source")
        and SOURCE_REF_RE.fullmatch(parts[1][:-len(".source")])
    )


def _is_backup_relative_path(relative_path: str) -> bool:
    return str(relative_path).endswith(".md") or _is_source_relative_path(
        relative_path
    )


def _iter_backup_paths(root_dir: str) -> Iterator[str]:
    """Depth-first scandir traversal without materializing directory listings."""
    iterators: list[os.ScandirIterator] = []
    try:
        try:
            iterators.append(os.scandir(root_dir))
        except OSError as exc:
            logger.warning(f"[github_sync] cannot scan {root_dir}: {exc}")
            return

        while iterators:
            current = iterators[-1]
            try:
                entry = next(current)
            except StopIteration:
                current.close()
                iterators.pop()
                continue
            except OSError as exc:
                logger.warning(f"[github_sync] directory scan failed: {exc}")
                current.close()
                iterators.pop()
                continue

            try:
                # Match os.walk(..., followlinks=False): recurse into real
                # directories, but never follow a symlinked directory tree.
                if entry.is_dir(follow_symlinks=False):
                    try:
                        iterators.append(os.scandir(entry.path))
                    except OSError as exc:
                        logger.warning(f"[github_sync] cannot scan {entry.path}: {exc}")
                    continue
                if entry.is_file(follow_symlinks=False):
                    if entry.name.endswith(".md"):
                        yield entry.path
                    elif entry.name.endswith(".source"):
                        relative = os.path.relpath(entry.path, root_dir).replace("\\", "/")
                        if relative.startswith("_sources/"):
                            if not _is_source_relative_path(relative):
                                raise RuntimeError(
                                    f"invalid source evidence filename: {relative}"
                                )
                            yield entry.path
            except OSError as exc:
                logger.warning(f"[github_sync] skip {entry.path}: {exc}")
    finally:
        for iterator in reversed(iterators):
            iterator.close()


class GitHubSync:
    """向 GitHub 仓库批量上传记忆和原文证据。"""

    def __init__(
        self,
        token: str,
        repo: str,
        branch: str = "main",
        path_prefix: str = "ombre",
        max_source_bytes: int = 2 * 1024 * 1024,
        state_key: str = "",
    ):
        self.token = token
        self.repo = repo.strip()          # "owner/repo"
        self.branch = branch.strip() or "main"
        self.path_prefix = path_prefix.strip().strip("/")
        configured_source_limit = max(0, int(max_source_bytes))
        self.max_source_bytes = min(
            configured_source_limit or HARD_MAX_SOURCE_BYTES,
            HARD_MAX_SOURCE_BYTES,
        )
        self._state_key_secret = state_key.strip()
        if self._state_key_secret and len(self._state_key_secret.encode("utf-8")) < _PRIVATE_STATE_MIN_KEY_BYTES:
            raise ValueError(
                f"OMBRE_GITHUB_STATE_KEY must be at least {_PRIVATE_STATE_MIN_KEY_BYTES} UTF-8 bytes"
            )

        self._headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

        self.last_sync: str | None = None
        self.last_status: str = "idle"   # idle | ok | error
        self.last_error: str = ""
        self.last_count: int = 0
        self.is_validated: bool = False   # validate() 成功后置 True
        # A3：连续失败计数。自动备份可能连挂几次而用户毫无察觉（以为有备份其实没有）。
        # 每次成功归零、每次失败 +1，供诊断面板判断要不要升级为醒目告警。
        self.consecutive_failures: int = 0
        self.last_private_state_backup: str | None = None
        self.last_private_state_restore: str | None = None
        # Manual and scheduled backups share one instance.  Serializing them
        # prevents two bounded jobs from adding up to an unbounded peak.
        self._sync_lock = asyncio.Lock()

    # --------------------------------------------------------
    # 公开接口
    # --------------------------------------------------------

    async def sync(self, buckets_dir: str) -> dict[str, Any]:
        """同步 buckets_dir 下记忆与原文证据到 GitHub。"""
        async with self._sync_lock:
            try:
                files = self._collect_files(buckets_dir)
                private_state = self._collect_private_state(buckets_dir)
                if not files and private_state is None:
                    self.last_status = "ok"
                    self.last_error = ""
                    self.last_sync = _now_iso()
                    self.last_count = 0
                    return {"ok": True, "uploaded": 0, "message": "无可同步文件"}

                if files:
                    count = await self._batch_commit(files)
                else:
                    # Do not publish an empty manifest over an existing remote
                    # backup merely because a local ephemeral vault is empty.
                    # A truly empty repository still needs one initial commit
                    # before the Contents API can write encrypted state.
                    await self._ensure_branch_for_private_state()
                    count = 0
                private_state_backed_up = False
                if private_state is not None:
                    await self._upload_private_state(private_state)
                    self.last_private_state_backup = _now_iso()
                    private_state_backed_up = True
                self.last_sync = _now_iso()
                self.last_status = "ok"
                self.last_error = ""
                self.last_count = count
                self.consecutive_failures = 0
                return {
                    "ok": True,
                    "uploaded": count,
                    "private_state_backed_up": private_state_backed_up,
                }
            except Exception as e:
                self.last_status = "error"
                self.last_error = str(e)
                self.consecutive_failures += 1
                logger.error(f"[github_sync] sync failed (连续 {self.consecutive_failures} 次): {e}")
                return {"ok": False, "error": str(e)}

    async def import_from_github(self, buckets_dir: str) -> dict[str, Any]:
        """从 GitHub 仓库恢复记忆和原文证据。

        这是 sync() 的逆操作。合并覆盖语义：同名（同相对路径）文件用 GitHub 上的覆盖，
        本地独有的文件保留不动。embeddings.db 不在仓库里，调用方应在导入后跑一次
        backfill 重建向量。带 path-traversal 防护（仓库内容不可信，防 ../ 逃逸）。
        """
        async with self._sync_lock:
            return await self._import_from_github_locked(buckets_dir)

    async def restore_private_state(
        self,
        buckets_dir: str,
        *,
        overwrite: bool = False,
    ) -> dict[str, Any]:
        """Restore encrypted OAuth client/grant state from a private repo.

        The secret key is supplied only through deployment configuration.  The
        GitHub repository must itself be private, and existing local state is
        never replaced during normal boot restoration.
        """
        if not self.private_state_enabled:
            return {
                "ok": True,
                "enabled": False,
                "restored": 0,
                "message": "encrypted private-state backup is not configured",
            }
        async with self._sync_lock:
            try:
                blob = await self._download_private_state()
                if blob is None:
                    return {
                        "ok": True,
                        "enabled": True,
                        "restored": 0,
                        "message": "no encrypted private-state backup exists yet",
                    }
                restored, skipped = self._install_private_state(
                    buckets_dir, blob, overwrite=overwrite
                )
                self.last_private_state_restore = _now_iso()
                return {
                    "ok": True,
                    "enabled": True,
                    "restored": restored,
                    "skipped": skipped,
                }
            except Exception as exc:
                logger.error("[github_sync] private-state restore failed: %s", exc)
                return {
                    "ok": False,
                    "enabled": True,
                    "restored": 0,
                    "error": str(exc)[:500],
                }

    async def _import_from_github_locked(self, buckets_dir: str) -> dict[str, Any]:
        """Serialized implementation shared with the backup lock."""
        failure_context: dict[str, Any] = {}
        try:
            async with httpx.AsyncClient(headers=self._headers, timeout=_TIMEOUT) as c:
                # 取 branch HEAD → commit tree → 递归列出全部 blob
                r = await self._request(c, "GET", f"{_API}/repos/{self.repo}/git/ref/heads/{self.branch}")
                if _is_empty_repo_response(r):
                    return {
                        "ok": True,
                        "imported": 0,
                        "skipped": 0,
                        "message": f"GitHub 仓库 {self.repo} 还是空仓库，暂无可导入的记忆文件",
                    }
                if r.status_code == 404:
                    return {"ok": False, "error": f"分支 {self.branch} 不存在"}
                r.raise_for_status()
                head_sha = r.json()["object"]["sha"]
                r = await self._request(c, "GET", f"{_API}/repos/{self.repo}/git/commits/{head_sha}")
                r.raise_for_status()
                tree_sha = r.json()["tree"]["sha"]
                r = await self._request(c, "GET", f"{_API}/repos/{self.repo}/git/trees/{tree_sha}?recursive=1")
                r.raise_for_status()
                tj = r.json()
                tree = tj.get("tree", [])
                truncated = bool(tj.get("truncated"))
                if truncated:
                    raise RuntimeError(
                        "GitHub returned a truncated tree; refusing an incomplete restore"
                    )
                if not isinstance(tree, list):
                    raise RuntimeError("GitHub tree response is not a list")

                prefix = (self.path_prefix + "/") if self.path_prefix else ""
                manifest_path = f"{prefix}{_MANIFEST_FILENAME}"
                manifest_item = next(
                    (
                        t for t in tree
                        if t.get("type") == "blob" and t.get("path") == manifest_path
                    ),
                    None,
                )
                backup_manifest = await self._read_backup_manifest_summary(c, manifest_item) if manifest_item else {"present": False}
                if manifest_item and not backup_manifest.get("present"):
                    raise RuntimeError(
                        "GitHub backup manifest is unreadable; refusing an unverified restore"
                    )
                manifest_entries = backup_manifest.pop("_entries", None)
                targets = []
                for target in tree:
                    remote_path = str(target.get("path", ""))
                    if target.get("type") != "blob" or not remote_path.startswith(prefix):
                        continue
                    relative_path = remote_path[len(prefix):]
                    if _is_backup_relative_path(relative_path):
                        targets.append(target)
                if len(targets) > _MAX_BACKUP_FILES:
                    raise RuntimeError(
                        f"GitHub restore has more than {_MAX_BACKUP_FILES} files"
                    )
                target_by_rel: dict[str, dict[str, Any]] = {}
                declared_total = 0
                for target in targets:
                    rel = str(target.get("path", ""))[len(prefix):]
                    if (
                        not rel
                        or len(rel.encode("utf-8")) > _MAX_BACKUP_PATH_BYTES
                        or rel in target_by_rel
                    ):
                        raise RuntimeError(f"unsafe or duplicate GitHub restore path: {rel[:200]}")
                    try:
                        declared_size = int(target.get("size", 0) or 0)
                    except (TypeError, ValueError, OverflowError) as exc:
                        raise RuntimeError(f"invalid GitHub blob size: {rel}") from exc
                    if declared_size < 0 or declared_size > _MAX_FILE_BYTES:
                        raise RuntimeError(
                            f"GitHub restore file exceeds {_MAX_FILE_BYTES} bytes: {rel}"
                        )
                    declared_total += declared_size
                    if declared_total > _MAX_RESTORE_TOTAL_BYTES:
                        raise RuntimeError(
                            "GitHub restore exceeds the total decoded-byte limit"
                        )
                    target_by_rel[rel] = target

                expected_manifest: dict[str, dict[str, Any]] | None = None
                if backup_manifest.get("present"):
                    expected_manifest = self._validate_restore_manifest(
                        manifest_entries, target_by_rel
                    )
                    manifest_total = sum(
                        item["bytes"] for item in expected_manifest.values()
                    )
                    if (
                        backup_manifest.get("schema_version") != 1
                        or backup_manifest.get("file_count") != len(expected_manifest)
                        or backup_manifest.get("total_bytes") != manifest_total
                    ):
                        raise RuntimeError("backup manifest summary is inconsistent")
                    # Git Trees are cumulative; files deleted locally can remain
                    # in an older base tree.  A valid manifest is the canonical
                    # file set, so ignore stale remote files outside it.
                    target_by_rel = {
                        rel: target_by_rel[rel] for rel in expected_manifest
                    }
                    targets = list(target_by_rel.values())
                if not targets:
                    return {"ok": True, "imported": 0, "skipped": 0,
                            "message": f"GitHub 仓库 {self.repo} 的 {prefix or '/'} 下没有可恢复的记忆文件",
                            "backup_manifest": backup_manifest}

                failure_context = {
                    "total": len(targets),
                    "backup_manifest": backup_manifest,
                }
                base = os.path.abspath(buckets_dir)
                imported = 0
                buckets_imported = 0
                sources_imported = 0
                skipped = 0
                errors: list[str] = []
                # 先把远端不可变 blob 全部下载到临时区并完成清单/证据校验。
                # 只有所有文件都通过，才安装证据；只有所有证据都安装成功，
                # 才开始覆盖 Markdown。这样 tree 顺序不会造成“桶已恢复但证据
                # 最后才失败”的悬空 source_refs。
                with tempfile.TemporaryDirectory(
                    prefix="ombre-github-restore-"
                ) as staging_dir:
                    staged: dict[str, str] = {}
                    restored_bytes = 0
                    for index, t in enumerate(targets):
                        rel = t["path"][len(prefix):]
                        if not rel:
                            continue
                        dest = os.path.abspath(os.path.join(base, rel))
                        if dest != base and not dest.startswith(base + os.sep):
                            raise RuntimeError(f"{rel}: restore path escapes the vault")
                        self._assert_safe_restore_destination(base, rel)
                        rb = await self._request(c, "GET", f"{_API}/repos/{self.repo}/git/blobs/{t['sha']}")
                        rb.raise_for_status()
                        bj = rb.json()
                        if bj.get("encoding") == "base64":
                            encoded = "".join(str(bj.get("content", "") or "").split())
                            data = base64.b64decode(encoded, validate=True)
                        else:
                            data = (bj.get("content", "") or "").encode("utf-8")
                        if len(data) > _MAX_FILE_BYTES:
                            raise RuntimeError(
                                f"decoded file exceeds {_MAX_FILE_BYTES} bytes"
                            )
                        restored_bytes += len(data)
                        if restored_bytes > _MAX_RESTORE_TOTAL_BYTES:
                            raise RuntimeError("restore exceeds total decoded-byte limit")
                        if expected_manifest is not None:
                            expected = expected_manifest[rel]
                            if (
                                len(data) != expected["bytes"]
                                or hashlib.sha256(data).hexdigest()
                                != expected["sha256"]
                            ):
                                raise RuntimeError("backup manifest integrity mismatch")
                        if _is_source_relative_path(rel):
                            filename = rel.rsplit("/", 1)[-1]
                            ref = filename[:-len(".source")]
                            if hashlib.sha256(data).hexdigest() != ref.removeprefix("src_"):
                                raise RuntimeError("source evidence integrity mismatch")
                            try:
                                data.decode("utf-8")
                            except UnicodeDecodeError as exc:
                                raise RuntimeError("source evidence is not UTF-8") from exc

                        stage_path = os.path.join(staging_dir, f"{index:05d}.blob")
                        with open(stage_path, "wb") as stage_handle:
                            stage_handle.write(data)
                        staged[rel] = stage_path

                    referenced_sources: set[str] = set()
                    available_sources = {
                        rel.rsplit("/", 1)[-1][:-len(".source")]
                        for rel in staged
                        if _is_source_relative_path(rel)
                    }
                    for rel in sorted(staged):
                        if not rel.endswith(".md"):
                            continue
                        with open(staged[rel], "rb") as markdown_handle:
                            markdown = markdown_handle.read(_MAX_FILE_BYTES + 1)
                        if len(markdown) > _MAX_FILE_BYTES:
                            raise RuntimeError(f"{rel}: staged Markdown exceeds limit")
                        try:
                            referenced_sources.update(
                                referenced_source_ids_from_markdown(markdown)
                            )
                        except ValueError as exc:
                            raise RuntimeError(
                                f"{rel}: invalid source evidence references: {exc}"
                            ) from exc

                    missing_sources = sorted(
                        referenced_sources - available_sources
                    )
                    integrity_warning = ""
                    if missing_sources:
                        preview = ", ".join(missing_sources[:3])
                        if backup_manifest.get(_SOURCE_REFS_COMPLETE_FIELD) is True:
                            raise RuntimeError(
                                "GitHub backup declares complete source evidence but has "
                                f"dangling references: {preview}"
                            )
                        integrity_warning = (
                            f"备份中有 {len(missing_sources)} 个原文证据引用没有对应文件；"
                            "这通常来自 v2.10.0 及更早的 GitHub 备份，事件记忆仍已恢复，"
                            "但这些原文无法核对"
                        )

                    source_store = SourceStore(
                        base, max_bytes=self.max_source_bytes
                    )
                    for rel in sorted(staged):
                        if not _is_source_relative_path(rel):
                            continue
                        with open(staged[rel], "rb") as source_handle:
                            data = source_handle.read(_MAX_FILE_BYTES + 1)
                        if len(data) > _MAX_FILE_BYTES:
                            raise RuntimeError(f"{rel}: staged source exceeds limit")
                        ref = rel.rsplit("/", 1)[-1][:-len(".source")]
                        installed_ref = source_store.put(data.decode("utf-8"))
                        if installed_ref != ref:
                            raise RuntimeError(f"{rel}: source evidence reference mismatch")
                        imported += 1
                        sources_imported += 1

                    for rel in sorted(staged):
                        if _is_source_relative_path(rel):
                            continue
                        dest = os.path.abspath(os.path.join(base, rel))
                        try:
                            self._assert_safe_restore_destination(base, rel)
                            with open(staged[rel], "rb") as stage_handle:
                                data = stage_handle.read(_MAX_FILE_BYTES + 1)
                            if len(data) > _MAX_FILE_BYTES:
                                raise RuntimeError("staged file exceeds limit")
                            # _win_long_path 前缀绕开 Windows 260 字符 MAX_PATH。
                            dest_long = _win_long_path(dest)
                            os.makedirs(
                                _win_long_path(os.path.dirname(dest)), exist_ok=True
                            )
                            # 单个 Markdown 原子覆盖；证据已经全部安装成功。
                            temp_path = f"{dest}.{uuid.uuid4().hex}.tmp"
                            temp_long = _win_long_path(temp_path)
                            try:
                                with open(temp_long, "wb") as output_handle:
                                    output_handle.write(data)
                                    output_handle.flush()
                                    os.fsync(output_handle.fileno())
                                os.replace(temp_long, dest_long)
                            except Exception:
                                if os.path.exists(temp_long):
                                    try:
                                        os.remove(temp_long)
                                    except OSError:
                                        pass
                                raise
                            imported += 1
                            buckets_imported += 1
                        except Exception as exc:
                            skipped += 1
                            errors.append(f"{rel}: {exc}")

                self.last_sync = _now_iso()
                restore_ok = skipped == 0
                self.last_status = "ok" if restore_ok else "error"
                return {
                    "ok": restore_ok,
                    "imported": imported,
                    "buckets_imported": buckets_imported,
                    "sources_imported": sources_imported,
                    "skipped": skipped,
                    "total": len(targets),
                    "truncated": False,
                    "error": errors[0] if errors else "",
                    "errors": errors[:10],
                    "backup_manifest": backup_manifest,
                    "integrity_warning": integrity_warning,
                }
        except Exception as e:
            logger.error(f"[github_sync] import failed: {e}")
            self.last_status = "error"
            self.last_error = str(e)
            if failure_context:
                total = int(failure_context["total"])
                return {
                    "ok": False,
                    "imported": 0,
                    "buckets_imported": 0,
                    "sources_imported": 0,
                    "skipped": total,
                    "total": total,
                    "truncated": False,
                    "error": str(e)[:500],
                    "errors": [str(e)[:500]],
                    "backup_manifest": failure_context["backup_manifest"],
                }
            return {"ok": False, "error": str(e)}

    async def validate(self) -> dict[str, Any]:
        """验证 token + repo 可访问，且具有写权限（contents: write）。"""
        try:
            async with httpx.AsyncClient(headers=self._headers, timeout=15.0) as c:
                r = await c.get(f"{_API}/repos/{self.repo}")
                if r.status_code == 404:
                    return {"ok": False, "error": f"仓库 {self.repo} 不存在或无权限访问"}
                if r.status_code == 401:
                    return {"ok": False, "error": "Token 无效或已过期"}
                r.raise_for_status()
                data = r.json()

                # Check write permission via `permissions.push` field
                # (GitHub returns this field when authenticated)
                perms = data.get("permissions", {})
                can_push = perms.get("push", False) or perms.get("admin", False)
                if perms and not can_push:
                    return {
                        "ok": False,
                        "error": "Token 只有读权限，无法上传文件。请在 GitHub → Settings → Developer settings → Fine-grained tokens 中将 Contents 权限设为 Read and write",
                    }

                self.is_validated = True
                return {
                    "ok": True,
                    "repo_full_name": data.get("full_name", self.repo),
                    "private": data.get("private", False),
                    "default_branch": data.get("default_branch", "main"),
                    "can_push": can_push,
                }
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def status(self) -> dict[str, Any]:
        return {
            "enabled": bool(self.token and self.repo),
            "repo": self.repo,
            "branch": self.branch,
            "path_prefix": self.path_prefix,
            "last_sync": self.last_sync,
            "last_status": self.last_status,
            "last_error": self.last_error,
            "last_count": self.last_count,
            "is_validated": self.is_validated,
            "consecutive_failures": self.consecutive_failures,
            "private_state_enabled": self.private_state_enabled,
            "last_private_state_backup": self.last_private_state_backup,
            "last_private_state_restore": self.last_private_state_restore,
        }

    @property
    def private_state_enabled(self) -> bool:
        return bool(self._state_key_secret)

    # --------------------------------------------------------
    # 内部实现
    # --------------------------------------------------------

    def _collect_files(self, buckets_dir: str) -> Mapping[str, bytes]:
        """Index eligible memory/evidence paths without retaining their bodies."""
        paths: dict[str, str] = {}
        if not os.path.isdir(buckets_dir):
            return _LazyMarkdownFiles(paths)
        base_real = os.path.realpath(buckets_dir)
        for full in _iter_backup_paths(buckets_dir):
            try:
                full_real = os.path.realpath(full)
                if os.path.commonpath((base_real, full_real)) != base_real:
                    logger.warning(
                        "[github_sync] skip path outside vault: %s", full
                    )
                    continue
                size = os.path.getsize(full)
                if size > _MAX_FILE_BYTES:
                    relative = os.path.relpath(full, buckets_dir).replace("\\", "/")
                    if _is_source_relative_path(relative):
                        raise RuntimeError(
                            f"GitHub backup source exceeds {_MAX_FILE_BYTES} bytes: {relative}"
                        )
                    logger.warning(
                        f"[github_sync] skip {os.path.basename(full)}: "
                        f"too large ({size} bytes)"
                    )
                    continue
                relative = os.path.relpath(full, buckets_dir).replace("\\", "/")
                if len(relative.encode("utf-8")) > _MAX_BACKUP_PATH_BYTES:
                    raise RuntimeError(
                        f"GitHub backup path exceeds {_MAX_BACKUP_PATH_BYTES} bytes: "
                        f"{relative[:200]}"
                    )
                if relative not in paths and len(paths) >= _MAX_BACKUP_FILES:
                    raise RuntimeError(
                        f"GitHub backup has more than {_MAX_BACKUP_FILES} files; "
                        "split or archive the vault before retrying"
                    )
                paths[relative] = full
            except OSError as e:
                logger.warning(f"[github_sync] skip {os.path.basename(full)}: {e}")
        return _LazyMarkdownFiles(paths)

    def _private_state_key(self) -> bytes:
        if not self._state_key_secret:
            raise RuntimeError("encrypted private-state backup is not configured")
        return hashlib.sha256(
            b"ombre-brain/private-state/key/v1\0"
            + self._state_key_secret.encode("utf-8")
        ).digest()

    def _collect_private_state(self, buckets_dir: str) -> bytes | None:
        if not self.private_state_enabled:
            return None
        payload_files: dict[str, str] = {}
        total = 0
        for filename in _PRIVATE_STATE_FILES:
            path = os.path.join(buckets_dir, filename)
            if not os.path.exists(path):
                continue
            if os.path.islink(path) or not os.path.isfile(path):
                raise RuntimeError(f"private-state source is not a regular file: {filename}")
            with open(path, "rb") as handle:
                data = handle.read(_PRIVATE_STATE_SOURCE_MAX_BYTES + 1)
            total += len(data)
            if (
                len(data) > _PRIVATE_STATE_SOURCE_MAX_BYTES
                or total > _PRIVATE_STATE_SOURCE_MAX_BYTES
            ):
                raise RuntimeError("private OAuth state exceeds the encrypted backup limit")
            try:
                parsed = json.loads(data.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise RuntimeError(f"private-state source is not valid JSON: {filename}") from exc
            if not isinstance(parsed, dict):
                raise RuntimeError(f"private-state source must be a JSON object: {filename}")
            payload_files[filename] = base64.b64encode(data).decode("ascii")
        if not payload_files:
            return None
        plaintext = json.dumps(
            {
                "schema_version": 1,
                "created_at": _now_iso(),
                "files": payload_files,
            },
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        nonce = os.urandom(12)
        ciphertext = AESGCM(self._private_state_key()).encrypt(
            nonce, plaintext, _PRIVATE_STATE_AAD
        )
        return _PRIVATE_STATE_MAGIC + nonce + ciphertext

    def _install_private_state(
        self,
        buckets_dir: str,
        blob: bytes,
        *,
        overwrite: bool,
    ) -> tuple[int, int]:
        if not blob.startswith(_PRIVATE_STATE_MAGIC):
            raise RuntimeError("encrypted private-state backup has an unknown format")
        encrypted = blob[len(_PRIVATE_STATE_MAGIC):]
        if len(encrypted) < 12 + 16 or len(encrypted) > _PRIVATE_STATE_MAX_BYTES:
            raise RuntimeError("encrypted private-state backup has an invalid size")
        nonce, ciphertext = encrypted[:12], encrypted[12:]
        try:
            plaintext = AESGCM(self._private_state_key()).decrypt(
                nonce, ciphertext, _PRIVATE_STATE_AAD
            )
        except InvalidTag as exc:
            raise RuntimeError(
                "encrypted private-state backup cannot be decrypted; check OMBRE_GITHUB_STATE_KEY"
            ) from exc
        try:
            payload = json.loads(plaintext.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RuntimeError("decrypted private-state payload is invalid JSON") from exc
        if not isinstance(payload, dict) or payload.get("schema_version") != 1:
            raise RuntimeError("decrypted private-state payload has an unsupported schema")
        files = payload.get("files")
        if not isinstance(files, dict) or any(name not in _PRIVATE_STATE_FILES for name in files):
            raise RuntimeError("decrypted private-state payload contains unexpected files")
        os.makedirs(buckets_dir, exist_ok=True)
        restored = 0
        skipped = 0
        for filename in _PRIVATE_STATE_FILES:
            encoded = files.get(filename)
            if encoded is None:
                continue
            if not isinstance(encoded, str):
                raise RuntimeError(f"decrypted private-state entry is invalid: {filename}")
            try:
                data = base64.b64decode(encoded, validate=True)
            except Exception as exc:
                raise RuntimeError(f"decrypted private-state entry is not base64: {filename}") from exc
            if len(data) > _PRIVATE_STATE_SOURCE_MAX_BYTES:
                raise RuntimeError(f"decrypted private-state entry is too large: {filename}")
            try:
                parsed = json.loads(data.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise RuntimeError(f"decrypted private-state entry is not valid JSON: {filename}") from exc
            if not isinstance(parsed, dict):
                raise RuntimeError(f"decrypted private-state entry must be an object: {filename}")
            destination = os.path.join(buckets_dir, filename)
            if os.path.exists(destination) and not overwrite:
                skipped += 1
                continue
            if os.path.lexists(destination) and os.path.islink(destination):
                raise RuntimeError(f"private-state destination is a symbolic link: {filename}")
            temp_path = f"{destination}.{uuid.uuid4().hex}.tmp"
            try:
                with open(temp_path, "wb") as handle:
                    handle.write(data)
                    handle.flush()
                    os.fsync(handle.fileno())
                try:
                    os.chmod(temp_path, 0o600)
                except OSError:
                    pass
                os.replace(temp_path, destination)
            finally:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
            restored += 1
        return restored, skipped

    def _private_state_remote_path(self) -> str:
        return (
            f"{self.path_prefix}/{_PRIVATE_STATE_FILENAME}"
            if self.path_prefix
            else _PRIVATE_STATE_FILENAME
        )

    async def _require_private_repository(self, client: httpx.AsyncClient) -> None:
        response = await self._request(client, "GET", f"{_API}/repos/{self.repo}")
        if response.status_code == 404:
            raise RuntimeError(f"repository {self.repo} does not exist or is not accessible")
        if response.status_code == 401:
            raise RuntimeError("GitHub token is invalid or expired")
        response.raise_for_status()
        if response.json().get("private") is not True:
            raise RuntimeError(
                "refusing to back up OAuth state: the configured GitHub repository is not private"
            )

    async def _upload_private_state(self, blob: bytes) -> None:
        remote_path = self._private_state_remote_path()
        encoded_path = quote(remote_path, safe="/")
        async with httpx.AsyncClient(headers=self._headers, timeout=_TIMEOUT) as client:
            await self._require_private_repository(client)
            get_url = (
                f"{_API}/repos/{self.repo}/contents/{encoded_path}"
                f"?ref={quote(self.branch, safe='')}"
            )
            current = await self._request(client, "GET", get_url)
            current_sha = ""
            if current.status_code == 200:
                current_sha = str(current.json().get("sha") or "")
            elif current.status_code != 404:
                current.raise_for_status()
            body: dict[str, Any] = {
                "message": f"Ombre Brain encrypted auth state — {_now_iso()}",
                "content": base64.b64encode(blob).decode("ascii"),
                "branch": self.branch,
            }
            if current_sha:
                body["sha"] = current_sha
            response = await self._request(
                client,
                "PUT",
                f"{_API}/repos/{self.repo}/contents/{encoded_path}",
                json=body,
            )
            response.raise_for_status()

    async def _ensure_branch_for_private_state(self) -> None:
        async with httpx.AsyncClient(
            headers=self._headers, timeout=_TIMEOUT
        ) as client:
            response = await self._request(
                client,
                "GET",
                f"{_API}/repos/{self.repo}/git/ref/heads/{self.branch}",
            )
            if _is_empty_repo_response(response):
                await self._batch_commit({})
                return
            if response.status_code == 404:
                raise RuntimeError(
                    f"branch {self.branch} does not exist; create it before backing up OAuth state"
                )
            response.raise_for_status()

    async def _download_private_state(self) -> bytes | None:
        remote_path = self._private_state_remote_path()
        encoded_path = quote(remote_path, safe="/")
        async with httpx.AsyncClient(headers=self._headers, timeout=_TIMEOUT) as client:
            await self._require_private_repository(client)
            response = await self._request(
                client,
                "GET",
                f"{_API}/repos/{self.repo}/contents/{encoded_path}"
                f"?ref={quote(self.branch, safe='')}",
            )
            if response.status_code == 404:
                return None
            response.raise_for_status()
            encoded = response.json().get("content", "")
            if not isinstance(encoded, str) or len(encoded) > (_PRIVATE_STATE_MAX_BYTES * 2):
                raise RuntimeError("encrypted private-state response is too large")
            try:
                blob = base64.b64decode("".join(encoded.split()), validate=True)
            except Exception as exc:
                raise RuntimeError("encrypted private-state response is not valid base64") from exc
            if len(blob) > _PRIVATE_STATE_MAX_BYTES:
                raise RuntimeError("encrypted private-state backup exceeds the size limit")
            return blob

    @staticmethod
    def _assert_safe_restore_destination(base: str, relative_path: str) -> None:
        """Reject symlink/junction parents before a restore write."""
        base_real = os.path.realpath(base)
        current = base
        parts = relative_path.replace("\\", "/").split("/")
        if any(part in ("", ".", "..") for part in parts):
            raise RuntimeError("unsafe restore path components")
        for part in parts:
            current = os.path.join(current, part)
            if os.path.lexists(current):
                if os.path.islink(current):
                    raise RuntimeError("restore path contains a symbolic link")
                current_real = os.path.realpath(current)
                if os.path.commonpath((base_real, current_real)) != base_real:
                    raise RuntimeError("restore path resolves outside the vault")

    @staticmethod
    def _validate_restore_manifest(
        raw_entries: object,
        targets: Mapping[str, dict[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        if not isinstance(raw_entries, list) or len(raw_entries) > len(targets):
            raise RuntimeError("backup manifest file set does not match GitHub tree")
        expected: dict[str, dict[str, Any]] = {}
        total = 0
        for item in raw_entries:
            if not isinstance(item, dict):
                raise RuntimeError("backup manifest entry must be an object")
            path = item.get("path")
            digest = item.get("sha256")
            try:
                size = int(item.get("bytes"))
            except (TypeError, ValueError, OverflowError) as exc:
                raise RuntimeError("backup manifest contains an invalid size") from exc
            if (
                not isinstance(path, str)
                or path not in targets
                or path in expected
                or len(path.encode("utf-8")) > _MAX_BACKUP_PATH_BYTES
                or not isinstance(digest, str)
                or len(digest) != 64
                or any(ch not in "0123456789abcdefABCDEF" for ch in digest)
                or size < 0
                or size > _MAX_FILE_BYTES
            ):
                raise RuntimeError("backup manifest contains an invalid entry")
            total += size
            if total > _MAX_RESTORE_TOTAL_BYTES:
                raise RuntimeError("backup manifest exceeds total decoded-byte limit")
            expected[path] = {"bytes": size, "sha256": digest.lower()}
        return expected

    def _build_backup_manifest(self, files: Mapping[str, bytes]) -> dict[str, Any]:
        """Build a JSON-safe manifest for the markdown files in one sync."""
        if len(files) > _MAX_BACKUP_FILES:
            raise RuntimeError(
                f"GitHub backup has more than {_MAX_BACKUP_FILES} files; "
                "split or archive the vault before retrying"
            )
        entries = []
        total_bytes = 0
        available_sources = {
            str(path).rsplit("/", 1)[-1][:-len(".source")]
            for path in files
            if _is_source_relative_path(str(path))
        }
        referenced_sources: set[str] = set()
        # Sorting keys is cheap.  Sorting ``files.items()`` would force a lazy
        # mapping to materialize every body at once and reintroduce the OOM.
        for rel_path in sorted(files):
            if len(str(rel_path).encode("utf-8")) > _MAX_BACKUP_PATH_BYTES:
                raise RuntimeError(
                    f"GitHub backup path exceeds {_MAX_BACKUP_PATH_BYTES} bytes: "
                    f"{str(rel_path)[:200]}"
                )
            content = files[rel_path]
            size = len(content)
            if str(rel_path).endswith(".md"):
                try:
                    referenced_sources.update(
                        referenced_source_ids_from_markdown(content)
                    )
                except ValueError as exc:
                    raise RuntimeError(
                        f"GitHub backup cannot validate {rel_path}: {exc}"
                    ) from exc
            total_bytes += size
            entries.append({
                "path": rel_path,
                "bytes": size,
                "sha256": hashlib.sha256(content).hexdigest(),
            })
        missing_sources = sorted(referenced_sources - available_sources)
        if missing_sources:
            preview = ", ".join(missing_sources[:3])
            raise RuntimeError(
                "GitHub backup has dangling source evidence references: "
                f"{preview}"
            )
        return {
            "schema_version": 1,
            "source": "ombre-brain",
            _SOURCE_REFS_COMPLETE_FIELD: True,
            "generated_at": _now_iso(),
            "repo": self.repo,
            "branch": self.branch,
            "path_prefix": self.path_prefix,
            "file_count": len(entries),
            "total_bytes": total_bytes,
            "files": entries,
        }

    def _iter_file_chunks(
        self,
        files: Mapping[str, bytes],
        manifest: Mapping[str, Any],
    ) -> Iterator[list[tuple[str, bytes]]]:
        """Yield verified bodies bounded by both count and decoded byte size."""
        expected_items = manifest.get("files", [])
        if not isinstance(expected_items, list) or len(expected_items) != len(files):
            raise RuntimeError("GitHub backup manifest does not match the file index")
        chunk: list[tuple[str, bytes]] = []
        chunk_bytes = 0
        # The manifest is already sorted.  Iterating its entries directly avoids
        # materializing a second O(N) path→metadata dictionary.
        for expected_item in expected_items:
            if not isinstance(expected_item, Mapping) or not expected_item.get("path"):
                raise RuntimeError("GitHub backup manifest contains an invalid file entry")
            relative_path = str(expected_item["path"])
            try:
                content = files[relative_path]
            except KeyError as exc:
                raise RuntimeError(
                    f"GitHub backup source changed while being indexed: {relative_path}; retry"
                ) from exc
            if not isinstance(content, bytes):
                content = bytes(content)
            expected_size = int(expected_item.get("bytes", -1))
            if (
                expected_size != len(content)
                or str(expected_item.get("sha256") or "")
                != hashlib.sha256(content).hexdigest()
            ):
                raise RuntimeError(
                    f"GitHub backup source changed while being read: {relative_path}; retry"
                )
            if chunk and (
                len(chunk) >= _TREE_CHUNK
                or chunk_bytes + len(content) > _TREE_CHUNK_BYTES
            ):
                yield chunk
                chunk = []
                chunk_bytes = 0
            chunk.append((relative_path, content))
            chunk_bytes += len(content)
        if chunk:
            yield chunk

    async def _batch_commit(self, files: Mapping[str, bytes]) -> int:
        """用 Git Trees API 一次性提交所有文件，返回上传文件数。

        关键点：tree entry 直接内联 `content`（UTF-8 文本），由 GitHub 在建
        tree 时顺带创建 blob —— 几百个文件只需 1~N 个 /git/trees 请求，而不是
        每个文件一个 /git/blobs 请求。后者会瞬间打满 GitHub 的 *secondary rate
        limit*（返回 403），正是之前同步莫名 403 的根因。

        大批量时分块提交（每块 _TREE_CHUNK 个），块与块之间用 base_tree 串联，
        最后只打一个 commit。所有请求都带指数退避重试以应对偶发的二级限流。
        """
        async with httpx.AsyncClient(headers=self._headers, timeout=_TIMEOUT) as c:
            # 1. 获取 branch HEAD commit SHA。GitHub 空仓库没有任何 ref，会在这里返回 409。
            r = await self._request(c, "GET", f"{_API}/repos/{self.repo}/git/ref/heads/{self.branch}")
            bootstrap_branch = _is_empty_repo_response(r)
            head_sha: str | None = None
            base_tree_sha: str | None = None
            if r.status_code == 404:
                raise RuntimeError(f"分支 {self.branch} 不存在，请先在 GitHub 上创建该分支")
            if not bootstrap_branch:
                r.raise_for_status()
                head_sha = r.json()["object"]["sha"]

                # 2. 获取 HEAD commit 对应的 tree SHA
                r = await self._request(c, "GET", f"{_API}/repos/{self.repo}/git/commits/{head_sha}")
                r.raise_for_status()
                base_tree_sha = r.json()["tree"]["sha"]

            # 3. Build a hard-bounded manifest first.  Lazy production mappings
            # read one body at a time during this pass and retain metadata only.
            manifest_path = f"{self.path_prefix}/{_MANIFEST_FILENAME}" if self.path_prefix else _MANIFEST_FILENAME
            manifest = self._build_backup_manifest(files)
            manifest_content = json.dumps(
                manifest, ensure_ascii=False, indent=2, sort_keys=True
            )
            manifest_entry = {
                "path": manifest_path,
                "mode": "100644",
                "type": "blob",
                "content": manifest_content,
            }
            # Account for the outer Git Trees JSON escaping too.  Count/path
            # limits bound construction; this payload limit bounds the request.
            manifest_payload_bytes = len(json.dumps(
                {"tree": [manifest_entry]},
                ensure_ascii=True,
                separators=(",", ":"),
            ).encode("utf-8"))
            if manifest_payload_bytes > _MAX_MANIFEST_PAYLOAD_BYTES:
                raise RuntimeError(
                    "GitHub backup manifest exceeds the bounded request limit "
                    f"({_MAX_MANIFEST_PAYLOAD_BYTES} bytes); split or archive the vault"
                )

            # 4. Read/decode/upload one bounded file chunk at a time.
            cur_base = base_tree_sha
            for file_chunk in self._iter_file_chunks(files, manifest):
                tree_entries: list[dict[str, Any]] = []
                for rel_path, content in file_chunk:
                    gh_path = f"{self.path_prefix}/{rel_path}" if self.path_prefix else rel_path
                    try:
                        text = content.decode("utf-8")
                        entry = {"path": gh_path, "mode": "100644", "type": "blob", "content": text}
                    except UnicodeDecodeError:
                        rb = await self._request(
                            c, "POST", f"{_API}/repos/{self.repo}/git/blobs",
                            json={"content": base64.b64encode(content).decode(), "encoding": "base64"},
                        )
                        rb.raise_for_status()
                        blob_sha = rb.json()["sha"]
                        del rb
                        entry = {"path": gh_path, "mode": "100644", "type": "blob", "sha": blob_sha}
                    tree_entries.append(entry)
                tree_payload: dict[str, Any] = {"tree": tree_entries}
                if cur_base:
                    tree_payload["base_tree"] = cur_base
                r = await self._request(
                    c, "POST", f"{_API}/repos/{self.repo}/git/trees",
                    json=tree_payload,
                )
                r.raise_for_status()
                cur_base = r.json()["sha"]
                # httpx responses retain their originating request (including
                # its serialized JSON body).  Drop it before reading the next
                # chunk so two request bodies cannot overlap in memory.
                del r
                del tree_payload, tree_entries, file_chunk

            # Keep the manifest in its own bounded request.  Otherwise a large
            # manifest silently sits on top of the file-chunk byte budget.
            manifest_payload: dict[str, Any] = {"tree": [manifest_entry]}
            if cur_base:
                manifest_payload["base_tree"] = cur_base
            r = await self._request(
                c,
                "POST",
                f"{_API}/repos/{self.repo}/git/trees",
                json=manifest_payload,
            )
            r.raise_for_status()
            cur_base = r.json()["sha"]
            del r, manifest_payload, manifest_entry, manifest_content
            new_tree_sha = cur_base

            # 5. 创建 commit
            now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            r = await self._request(
                c, "POST", f"{_API}/repos/{self.repo}/git/commits",
                json={
                    "message": f"Ombre Brain sync — {now_str} ({len(files)} files)",
                    "tree": new_tree_sha,
                    "parents": [head_sha] if head_sha else [],
                },
            )
            r.raise_for_status()
            commit_sha: str = r.json()["sha"]

            # 6. 更新已有 branch ref；空仓库首次提交则创建 branch ref
            if bootstrap_branch:
                r = await self._request(
                    c, "POST", f"{_API}/repos/{self.repo}/git/refs",
                    json={"ref": f"refs/heads/{self.branch}", "sha": commit_sha},
                )
            else:
                r = await self._request(
                    c, "PATCH", f"{_API}/repos/{self.repo}/git/refs/heads/{self.branch}",
                    json={"sha": commit_sha, "force": False},
                )
            r.raise_for_status()

        return len(files)

    async def _read_backup_manifest_summary(
        self,
        client: httpx.AsyncClient,
        manifest_item: dict[str, Any],
    ) -> dict[str, Any]:
        try:
            declared_size = int(manifest_item.get("size", -1))
            if declared_size < 0 or declared_size > _MAX_MANIFEST_PAYLOAD_BYTES:
                raise ValueError("backup manifest exceeds the decoded-byte limit")
            sha = manifest_item.get("sha", "")
            if not sha:
                return {"present": False}
            rb = await self._request(client, "GET", f"{_API}/repos/{self.repo}/git/blobs/{sha}")
            rb.raise_for_status()
            bj = rb.json()
            if bj.get("encoding") == "base64":
                encoded = bj.get("content", "")
                if not isinstance(encoded, str) or len(encoded) > _MAX_MANIFEST_BASE64_BYTES:
                    raise ValueError("backup manifest base64 payload is too large")
                compact = "".join(encoded.split())
                decoded = base64.b64decode(compact, validate=True)
                if len(decoded) > _MAX_MANIFEST_PAYLOAD_BYTES:
                    raise ValueError("backup manifest exceeds the decoded-byte limit")
                raw = decoded.decode("utf-8")
            else:
                raw = str(bj.get("content", "") or "")
                if (
                    len(raw) > _MAX_MANIFEST_PAYLOAD_BYTES
                    or len(raw.encode("utf-8")) > _MAX_MANIFEST_PAYLOAD_BYTES
                ):
                    raise ValueError("backup manifest exceeds the decoded-byte limit")
            data = json.loads(raw)
            if not isinstance(data, dict):
                raise ValueError("backup manifest root must be an object")
            summary = {
                "present": True,
                "schema_version": data.get("schema_version"),
                "generated_at": data.get("generated_at", ""),
                "file_count": int(data.get("file_count") or 0),
                "total_bytes": int(data.get("total_bytes") or 0),
                "_entries": data.get("files"),
            }
            if data.get(_SOURCE_REFS_COMPLETE_FIELD) is True:
                summary[_SOURCE_REFS_COMPLETE_FIELD] = True
            return summary
        except Exception as e:
            return {"present": False, "error": str(e)[:200]}

    async def _request(
        self,
        client: httpx.AsyncClient,
        method: str,
        url: str,
        *,
        json: dict | None = None,
        _max_retries: int = 4,
    ) -> httpx.Response:
        """带退避重试的请求。专治 GitHub 二级限流（403/429 + Retry-After）。

        普通 4xx（权限/404 等）直接返回交由上层 raise_for_status 处理，不重试。
        """
        for attempt in range(_max_retries + 1):
            resp = await client.request(method, url, json=json)
            if resp.status_code not in (403, 429):
                return resp
            # 判断是否二级限流（而非真正的权限 403）
            body_l = resp.text.lower()
            is_rate = (
                "rate limit" in body_l
                or "retry-after" in {k.lower() for k in resp.headers}
                or resp.headers.get("x-ratelimit-remaining") == "0"
            )
            if not is_rate or attempt == _max_retries:
                return resp
            # 计算等待时长：优先 Retry-After，其次指数退避
            retry_after = resp.headers.get("retry-after")
            if retry_after and retry_after.isdigit():
                wait = int(retry_after)
            else:
                wait = min(2 ** attempt, 30)
            logger.warning(f"[github_sync] secondary rate limit, retry in {wait}s (attempt {attempt + 1})")
            await asyncio.sleep(wait)
        return resp


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _is_empty_repo_response(resp: httpx.Response) -> bool:
    """GitHub returns 409 when refs are requested from a zero-commit repo."""
    if resp.status_code != 409:
        return False
    try:
        message = str(resp.json().get("message", ""))
    except Exception:
        message = resp.text
    return "empty" in message.lower()
