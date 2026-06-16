"""
本地 HTTP 管理服务器。

Agent 启动时在后台启动一个仅监听 127.0.0.1 的 HTTP 服务，
CLI 通过这个端口查询状态、查日志、调整采样规则、导出诊断数据。

设计原则：
- 仅监听 127.0.0.1，不对外暴露
- 轻量：使用 Python 标准库 http.server，零依赖
- 支持可选的简单 token 鉴权
- 所有操作线程安全
"""

import json
import threading
import os
import time
import logging
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from log_agent import LogAgent

logger = logging.getLogger(__name__)


def _parse_bool(s: Optional[str], default: bool = False) -> bool:
    if s is None:
        return default
    return s.lower() in ("1", "true", "yes", "on")


def _parse_int(s: Optional[str], default: int = 0) -> int:
    try:
        return int(s) if s else default
    except (ValueError, TypeError):
        return default


def _parse_float(s: Optional[str], default: float = 0.0) -> float:
    try:
        return float(s) if s else default
    except (ValueError, TypeError):
        return default


class ManagementServer:
    """
    封装本地 HTTP 管理服务的启动和关闭。
    """

    def __init__(self, agent: "LogAgent"):
        self._agent = agent
        self._config = agent._config
        self._server: Optional[ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None
        self._actual_port: int = 0
        self._lock = threading.Lock()
        self._token_file_path: Optional[str] = None

    @property
    def actual_port(self) -> int:
        return self._actual_port

    @property
    def base_url(self) -> str:
        return f"http://{self._config.management_host}:{self._actual_port}"

    def start(self):
        """启动管理服务器（后台线程）。"""
        if not self._config.management_enabled:
            logger.info("management server disabled by config")
            return

        with self._lock:
            if self._server is not None:
                return

            config = self._config
            host = config.management_host
            port = config.management_port
            token = config.management_token

            # 如果没指定 token，生成一个随机 token 并写入本地文件，方便 CLI 自动发现
            if not token:
                import secrets
                token = secrets.token_hex(16)

            server_self = self

            class Handler(BaseHTTPRequestHandler):
                def log_message(self, format, *args):
                    # 静默，不打到 stderr
                    logger.debug("mgmt %s", format % args)

                def _check_auth(self) -> bool:
                    if not token:
                        return True
                    q = parse_qs(urlparse(self.path).query)
                    req_token = (q.get("token", [None])[0]
                                 or self.headers.get("X-Management-Token"))
                    return req_token == token

                def _send_json(self, data, status: int = 200):
                    body = json.dumps(data, ensure_ascii=False, default=str).encode("utf-8")
                    self.send_response(status)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)

                def _send_text(self, text: str, status: int = 200):
                    body = text.encode("utf-8")
                    self.send_response(status)
                    self.send_header("Content-Type", "text/plain; charset=utf-8")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)

                def _404(self):
                    self._send_json({"error": "not found"}, 404)

                def _401(self):
                    self._send_json({"error": "unauthorized"}, 401)

                def do_GET(self):
                    if not self._check_auth():
                        self._401()
                        return
                    parsed = urlparse(self.path)
                    path = parsed.path
                    q = parse_qs(parsed.query)

                    if path == "/health" or path == "/ping":
                        self._send_json({"ok": True, "pid": os.getpid()})

                    elif path == "/status":
                        self._send_json(server_self._agent.get_status())

                    elif path == "/logs":
                        level = q.get("level", [None])[0]
                        keyword = q.get("keyword", [None])[0]
                        trace_id = q.get("trace_id", [None])[0]
                        service = q.get("service", [None])[0]
                        limit = _parse_int(q.get("limit", ["100"])[0], 100)
                        order = q.get("order", ["desc"])[0]
                        logs = server_self._agent.query_logs(
                            level=level, keyword=keyword, trace_id=trace_id,
                            service=service, limit=limit, order=order,
                        )
                        self._send_json({"count": len(logs), "logs": logs})

                    elif path == "/sampling":
                        self._send_json(server_self._agent.sampler.get_stats())

                    elif path == "/export":
                        output_dir = q.get("output_dir", [None])[0]
                        include_crash = _parse_bool(q.get("include_crash", ["1"])[0], True)
                        service = q.get("service", [None])[0]
                        try:
                            path_exported = server_self._agent.export_diagnostic_data(
                                output_dir=output_dir,
                                include_crash_dumps=include_crash,
                                service=service,
                            )
                            size = os.path.getsize(path_exported) if os.path.exists(path_exported) else 0
                            self._send_json({
                                "ok": True,
                                "path": path_exported,
                                "size_bytes": size,
                            })
                        except Exception as e:
                            self._send_json({"ok": False, "error": str(e)}, 500)

                    elif path == "/target":
                        rep = server_self._agent._reporter
                        if hasattr(rep, "get_target_info"):
                            self._send_json(rep.get_target_info())
                        else:
                            self._send_json({"type": type(rep).__name__})

                    else:
                        self._404()

                def do_POST(self):
                    if not self._check_auth():
                        self._401()
                        return
                    parsed = urlparse(self.path)
                    path = parsed.path

                    length = int(self.headers.get("Content-Length", "0") or "0")
                    raw_body = self.rfile.read(length) if length > 0 else b"{}"
                    try:
                        body = json.loads(raw_body.decode("utf-8")) if raw_body else {}
                    except Exception:
                        body = {}

                    if path == "/sampling/level":
                        level = body.get("level") or parse_qs(parsed.query).get("level", [None])[0]
                        if not level:
                            self._send_json({"ok": False, "error": "level is required"}, 400)
                            return
                        try:
                            server_self._agent.sampler.set_min_level(level)
                            self._send_json({"ok": True, "min_level": level})
                        except ValueError as e:
                            self._send_json({"ok": False, "error": str(e)}, 400)

                    elif path == "/sampling/global_rate":
                        rate_raw = body.get("rate") or parse_qs(parsed.query).get("rate", [None])[0]
                        try:
                            rate = float(rate_raw)
                            server_self._agent.sampler.set_global_sample_rate(rate)
                            self._send_json({"ok": True, "global_sample_rate": rate})
                        except (ValueError, TypeError) as e:
                            self._send_json({"ok": False, "error": str(e)}, 400)

                    elif path == "/sampling/service_rate":
                        svc = body.get("service") or parse_qs(parsed.query).get("service", [None])[0]
                        rate_raw = body.get("rate") or parse_qs(parsed.query).get("rate", [None])[0]
                        if not svc:
                            self._send_json({"ok": False, "error": "service is required"}, 400)
                            return
                        try:
                            rate = float(rate_raw)
                            server_self._agent.sampler.set_service_sample_rate(svc, rate)
                            self._send_json({"ok": True, "service": svc, "rate": rate})
                        except (ValueError, TypeError) as e:
                            self._send_json({"ok": False, "error": str(e)}, 400)

                    elif path == "/sampling/trace_whitelist":
                        action = body.get("action") or parse_qs(parsed.query).get("action", ["add"])[0]
                        tid = body.get("trace_id") or parse_qs(parsed.query).get("trace_id", [None])[0]
                        if not tid:
                            self._send_json({"ok": False, "error": "trace_id is required"}, 400)
                            return
                        if action == "remove":
                            server_self._agent.sampler.remove_trace_whitelist(tid)
                        else:
                            server_self._agent.sampler.add_trace_whitelist(tid)
                        self._send_json({"ok": True, "action": action, "trace_id": tid})

                    elif path == "/sampling/keyword_rule":
                        kw = body.get("keyword") or parse_qs(parsed.query).get("keyword", [None])[0]
                        rate_raw = body.get("rate") or parse_qs(parsed.query).get("rate", [None])[0]
                        if not kw:
                            self._send_json({"ok": False, "error": "keyword is required"}, 400)
                            return
                        try:
                            rate = float(rate_raw)
                            server_self._agent.sampler.add_keyword_rule(kw, rate)
                            self._send_json({"ok": True, "keyword": kw, "rate": rate})
                        except (ValueError, TypeError) as e:
                            self._send_json({"ok": False, "error": str(e)}, 400)

                    elif path == "/stop":
                        # 远程停止 Agent（可选）
                        timeout = _parse_float(
                            body.get("timeout") or parse_qs(parsed.query).get("timeout", ["5.0"])[0],
                            5.0,
                        )
                        self._send_json({"ok": True, "stopping": True, "timeout": timeout})
                        # 延迟在另一个线程执行 stop，避免 HTTP 响应没发完
                        threading.Thread(
                            target=lambda: (time.sleep(0.1), server_self._agent.stop(timeout=timeout)),
                            daemon=True,
                        ).start()

                    else:
                        self._404()

            try:
                self._server = ThreadingHTTPServer((host, port), Handler)
            except OSError as e:
                logger.error("failed to start management server: %s", e)
                self._server = None
                return

            self._actual_port = self._server.server_address[1]

            # 写入端口信息到本地文件，CLI 自动发现
            self._write_runtime_info(token)

            self._thread = threading.Thread(
                target=self._server.serve_forever,
                name="log-agent-mgmt",
                daemon=True,
            )
            self._thread.start()
            logger.info("management server started at %s (token=%s...)",
                        self.base_url, token[:4] if token else "none")

    def _write_runtime_info(self, token: str):
        """把端口和 token 写到 crash_dump_dir，方便 CLI 自动连接。"""
        try:
            info_dir = self._config.crash_dump_dir
            os.makedirs(info_dir, exist_ok=True)
            info_path = os.path.join(info_dir, ".agent_mgmt.json")
            info = {
                "pid": os.getpid(),
                "host": self._config.management_host,
                "port": self._actual_port,
                "token": token,
                "started_at": time.time(),
            }
            with open(info_path, "w", encoding="utf-8") as f:
                json.dump(info, f)
            self._token_file_path = info_path
        except Exception as e:
            logger.warning("failed to write management runtime info: %s", e)

    def stop(self):
        """停止管理服务器。"""
        with self._lock:
            if self._token_file_path and os.path.exists(self._token_file_path):
                try:
                    os.remove(self._token_file_path)
                except Exception:
                    pass
                self._token_file_path = None

            if self._server is not None:
                try:
                    self._server.shutdown()
                except Exception:
                    pass
                self._server = None
                self._thread = None
                self._actual_port = 0


def discover_agent(crash_dump_dir: str) -> Optional[dict]:
    """
    CLI 端自动发现正在运行的 Agent。

    读取 crash_dump_dir/.agent_mgmt.json，如果存在且对端健康则返回连接信息。
    """
    info_path = os.path.join(crash_dump_dir, ".agent_mgmt.json")
    if not os.path.exists(info_path):
        return None
    try:
        with open(info_path, "r", encoding="utf-8") as f:
            info = json.load(f)
    except Exception:
        return None

    # 简单健康检查
    try:
        import urllib.request
        url = f"http://{info['host']}:{info['port']}/health?token={info.get('token', '')}"
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=1.0) as resp:
            if resp.status == 200:
                return info
    except Exception:
        # Agent 可能已经退出但文件没清掉，忽略
        try:
            os.remove(info_path)
        except Exception:
            pass
    return None
