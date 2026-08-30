from datetime import datetime, timezone
import copy
import math
import os
import re
import urllib.parse

import requests


class CustomSourceValidationError(ValueError):
    pass


class CustomSourceRequestError(RuntimeError):
    pass


ADAPTER_CONTRACTS = {
    "bars_json": {
        "label": "Historical bars (JSON)",
        "sections": ["discovery", "market"],
        "description": "Fetches one symbol at a time and returns an array of OHLCV-style rows. Only timestamp/date, close, and volume are consumed.",
        "context_variables": ["symbol"],
        "required_mapping": ["timestamp", "close", "volume"],
        "optional_mapping": [],
        "default_mapping": {"timestamp": "date", "close": "close", "volume": "volume"},
        "sample_context": {"symbol": "SPY"},
        "minimum_records": 25,
    },
    "screener_json": {
        "label": "Symbol screener (JSON)",
        "sections": ["discovery"],
        "description": "Fetches a ranked/list endpoint once per scan and extracts symbols that are added as discovery extras.",
        "context_variables": [],
        "required_mapping": ["symbol"],
        "optional_mapping": ["rank", "score"],
        "default_mapping": {"symbol": "symbol", "rank": "rank", "score": "score"},
        "sample_context": {},
        "minimum_records": 1,
    },
    "quote_json": {
        "label": "Live quote (JSON)",
        "sections": ["market"],
        "description": "Fetches one symbol at a time for the read-only quote router. Price is required; bid, ask, and timestamp are optional.",
        "context_variables": ["symbol"],
        "required_mapping": ["price"],
        "optional_mapping": ["bid", "ask", "timestamp"],
        "default_mapping": {"price": "price", "bid": "bid", "ask": "ask", "timestamp": "timestamp"},
        "sample_context": {"symbol": "SPY"},
        "minimum_records": 1,
    },
    "items_json": {
        "label": "News/discussion items (JSON)",
        "sections": ["news", "social"],
        "description": "Fetches theme/search results and returns an array of headline or discussion rows. Title is required.",
        "context_variables": ["query", "theme", "symbol"],
        "required_mapping": ["title"],
        "optional_mapping": ["link", "source", "published_at"],
        "default_mapping": {"title": "title", "link": "url", "source": "source", "published_at": "published_at"},
        "sample_context": {"query": "technology stocks", "theme": "Technology", "symbol": "AAPL"},
        "minimum_records": 1,
    },
    "fundamentals_json": {
        "label": "Fundamental snapshot (JSON)",
        "sections": ["fundamentals"],
        "description": "Fetches one symbol at a time and maps already-calculated YoY revenue and net-income growth percentages.",
        "context_variables": ["symbol"],
        "required_mapping": ["revenue_yoy"],
        "optional_mapping": ["net_income_yoy", "company", "cik"],
        "default_mapping": {"revenue_yoy": "revenue_yoy", "net_income_yoy": "net_income_yoy", "company": "company", "cik": "cik"},
        "sample_context": {"symbol": "AAPL"},
        "minimum_records": 1,
    },
}


def contracts_for_ui():
    by_section = {}
    for section in ("discovery", "market", "news", "social", "fundamentals"):
        by_section[section] = []
        for adapter_id, contract in ADAPTER_CONTRACTS.items():
            if section in contract["sections"]:
                by_section[section].append({"id": adapter_id, **copy.deepcopy(contract)})
    return by_section


def credential_state(requirements):
    if not requirements:
        return "public"
    for requirement in requirements:
        alternatives = requirement.split("|")
        if not any(os.getenv(name, "") for name in alternatives):
            return "missing"
    return "configured"


def _validate_string_map(label, value, max_items=30):
    if not isinstance(value, dict):
        raise CustomSourceValidationError(f"{label} must be a JSON object")
    if len(value) > max_items:
        raise CustomSourceValidationError(f"{label} has too many entries")
    validated = {}
    for key, item in value.items():
        if not isinstance(key, str) or not key.strip() or not isinstance(item, str):
            raise CustomSourceValidationError(f"{label} keys and values must be text")
        validated[key.strip()] = item.strip()
    return validated


def validate_custom_source(section, source, built_in_ids=None):
    if section not in ("discovery", "market", "news", "social", "fundamentals"):
        raise CustomSourceValidationError(f"Unsupported source section: {section}")
    if not isinstance(source, dict):
        raise CustomSourceValidationError("Source definition must be an object")

    source_id = str(source.get("id") or "").strip().lower()
    if not re.fullmatch(r"[a-z][a-z0-9_]{2,39}", source_id):
        raise CustomSourceValidationError("Source ID must be 3–40 lowercase letters, numbers, or underscores and start with a letter")
    if source_id in set(built_in_ids or []):
        raise CustomSourceValidationError(f"Source ID '{source_id}' is reserved by a built-in provider")

    label = str(source.get("label") or "").strip()
    if len(label) < 2 or len(label) > 80:
        raise CustomSourceValidationError("Display name must be 2–80 characters")

    adapter = str(source.get("adapter") or "").strip()
    contract = ADAPTER_CONTRACTS.get(adapter)
    if not contract or section not in contract["sections"]:
        raise CustomSourceValidationError(f"Adapter '{adapter}' is not supported for {section}")

    method = str(source.get("method") or "GET").strip().upper()
    if method != "GET":
        raise CustomSourceValidationError("Custom data sources are read-only; only GET is supported")

    endpoint = str(source.get("endpoint") or "").strip()
    parsed = urllib.parse.urlparse(endpoint)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise CustomSourceValidationError("Endpoint must be a full HTTP(S) URL")

    auth_type = str(source.get("auth_type") or "none").strip().lower()
    if auth_type not in ("none", "query", "header", "bearer"):
        raise CustomSourceValidationError("Authentication type must be none, query, header, or bearer")
    credential_env = str(source.get("credential_env") or "").strip()
    auth_name = str(source.get("auth_name") or "").strip()
    if auth_type != "none":
        if not re.fullmatch(r"[A-Z][A-Z0-9_]{2,79}", credential_env):
            raise CustomSourceValidationError("Credential environment variable must use uppercase letters, numbers, and underscores")
        if auth_type in ("query", "header") and not auth_name:
            raise CustomSourceValidationError("Query/header authentication requires a parameter or header name")
    else:
        credential_env = ""
        auth_name = ""

    field_mapping = _validate_string_map("Field mapping", source.get("field_mapping") or {})
    missing_fields = [name for name in contract["required_mapping"] if not field_mapping.get(name)]
    if missing_fields:
        raise CustomSourceValidationError(f"Field mapping is missing required fields: {', '.join(missing_fields)}")
    allowed_fields = set(contract["required_mapping"] + contract["optional_mapping"])
    unknown_fields = sorted(set(field_mapping) - allowed_fields)
    if unknown_fields:
        raise CustomSourceValidationError(f"Field mapping contains unsupported output fields: {', '.join(unknown_fields)}")

    query_params = _validate_string_map("Query parameters", source.get("query_params") or {})
    static_headers = _validate_string_map("Static headers", source.get("static_headers") or {})
    root_path = str(source.get("root_path") or "").strip()
    timeout = source.get("timeout_seconds", 12)
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not math.isfinite(float(timeout)):
        raise CustomSourceValidationError("Timeout must be a finite number")
    timeout = float(timeout)
    if timeout < 2 or timeout > 60:
        raise CustomSourceValidationError("Timeout must be between 2 and 60 seconds")
    priority = source.get("priority", 50)
    if isinstance(priority, bool) or not isinstance(priority, (int, float)):
        raise CustomSourceValidationError("Priority must be a number")
    priority = int(priority)
    if priority < 1 or priority > 999:
        raise CustomSourceValidationError("Priority must be between 1 and 999; lower runs first")

    return {
        "id": source_id,
        "label": label,
        "adapter": adapter,
        "purpose": str(source.get("purpose") or contract["description"]).strip()[:240],
        "enabled": bool(source.get("enabled", True)),
        "priority": priority,
        "method": "GET",
        "endpoint": endpoint,
        "auth_type": auth_type,
        "credential_env": credential_env,
        "auth_name": auth_name,
        "query_params": query_params,
        "static_headers": static_headers,
        "root_path": root_path,
        "field_mapping": field_mapping,
        "timeout_seconds": timeout,
    }


def _path_tokens(path):
    if not path or path == "$":
        return []
    clean = path.strip()
    if clean.startswith("$."):
        clean = clean[2:]
    clean = re.sub(r"\[(\d+)\]", r".\1", clean)
    return [token for token in clean.split(".") if token]


def extract_path(payload, path):
    value = payload
    for token in _path_tokens(path):
        if isinstance(value, list):
            try:
                value = value[int(token)]
            except (ValueError, IndexError):
                return None
        elif isinstance(value, dict):
            value = value.get(token)
        else:
            return None
    return value


def _format_template(value, context, quote=False):
    result = str(value)
    for key, item in context.items():
        replacement = urllib.parse.quote(str(item), safe="") if quote else str(item)
        result = result.replace("{" + key + "}", replacement)
    return result


def _safe_float(value):
    if value is None or value == "":
        return None
    try:
        parsed = float(value)
        return parsed if math.isfinite(parsed) else None
    except (TypeError, ValueError):
        return None


def _sanitize_error(error, credential=""):
    message = str(error)
    if credential:
        message = message.replace(credential, "***")
    message = re.sub(r"([?&](?:apiKey|apikey|token|key|secret)=)[^&\s]+", r"\1***", message, flags=re.I)
    return message[:300]


class GenericJsonSource:
    def __init__(self, definition, http_get=None):
        self.definition = definition
        self.http_get = http_get or requests.get

    @property
    def source_id(self):
        return self.definition["id"]

    def _request(self, context):
        definition = self.definition
        endpoint = _format_template(definition["endpoint"], context, quote=True)
        params = {
            key: _format_template(value, context)
            for key, value in definition.get("query_params", {}).items()
        }
        headers = {
            key: _format_template(value, context)
            for key, value in definition.get("static_headers", {}).items()
        }
        credential = ""
        if definition.get("auth_type") != "none":
            credential = os.getenv(definition.get("credential_env", ""), "")
            if not credential:
                raise CustomSourceRequestError(f"{definition['credential_env']} is not configured")
            if definition["auth_type"] == "query":
                params[definition["auth_name"]] = credential
            elif definition["auth_type"] == "header":
                headers[definition["auth_name"]] = credential
            elif definition["auth_type"] == "bearer":
                headers["Authorization"] = f"Bearer {credential}"
        try:
            response = self.http_get(
                endpoint,
                params=params,
                headers=headers,
                timeout=definition["timeout_seconds"],
            )
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            raise CustomSourceRequestError(_sanitize_error(exc, credential)) from exc

    def _mapped_rows(self, context):
        payload = self._request(context)
        root = extract_path(payload, self.definition.get("root_path", ""))
        if self.definition["adapter"] in ("quote_json", "fundamentals_json"):
            rows = root if isinstance(root, list) else [root]
        else:
            rows = root
        if not isinstance(rows, list):
            raise CustomSourceRequestError("Root path did not resolve to an array")
        mapped = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            mapped.append({
                output: extract_path(row, path)
                for output, path in self.definition["field_mapping"].items()
            })
        return mapped

    def fetch_bars(self, symbol):
        rows = self._mapped_rows({"symbol": symbol})
        bars = []
        for row in rows:
            close = _safe_float(row.get("close"))
            volume = _safe_float(row.get("volume"))
            timestamp = row.get("timestamp")
            if close is None or timestamp is None:
                continue
            if isinstance(timestamp, (int, float)):
                date_value = datetime.fromtimestamp(float(timestamp), timezone.utc).date().isoformat()
            else:
                date_value = str(timestamp)[:10]
            bars.append({"date": date_value, "close": close, "volume": int(volume or 0)})
        if len(bars) < ADAPTER_CONTRACTS["bars_json"]["minimum_records"]:
            raise CustomSourceRequestError(f"Only {len(bars)} valid bars returned; at least 25 are required")
        return bars

    def fetch_symbols(self):
        rows = self._mapped_rows({})
        symbols = []
        for row in rows:
            symbol = str(row.get("symbol") or "").strip().upper()
            if symbol and re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,9}", symbol):
                symbols.append({"symbol": symbol, "rank": row.get("rank"), "score": row.get("score")})
        if not symbols:
            raise CustomSourceRequestError("No valid symbols were mapped")
        return symbols

    def fetch_items(self, query, theme="", symbol=""):
        rows = self._mapped_rows({"query": query, "theme": theme, "symbol": symbol})
        items = []
        for row in rows:
            title = str(row.get("title") or "").strip()
            if title:
                items.append({
                    "title": title,
                    "link": row.get("link"),
                    "source": row.get("source") or self.definition["label"],
                    "published_at": row.get("published_at"),
                    "provider_id": self.source_id,
                })
        if not items:
            raise CustomSourceRequestError("No valid items were mapped")
        return items

    def fetch_quote(self, symbol):
        rows = self._mapped_rows({"symbol": symbol})
        row = rows[0] if rows else {}
        price = _safe_float(row.get("price"))
        if price is None:
            raise CustomSourceRequestError("Mapped quote did not contain a numeric price")
        return {
            "symbol": symbol,
            "provider": self.source_id,
            "price": price,
            "bid": _safe_float(row.get("bid")),
            "ask": _safe_float(row.get("ask")),
            "timestamp": row.get("timestamp"),
        }

    def fetch_fundamentals(self, symbol):
        rows = self._mapped_rows({"symbol": symbol})
        row = rows[0] if rows else {}
        revenue_yoy = _safe_float(row.get("revenue_yoy"))
        if revenue_yoy is None:
            raise CustomSourceRequestError("Mapped fundamentals did not contain numeric revenue_yoy")
        return {
            "symbol": symbol,
            "company": row.get("company"),
            "cik": row.get("cik"),
            "revenue_yoy": revenue_yoy,
            "net_income_yoy": _safe_float(row.get("net_income_yoy")),
            "source_id": self.source_id,
        }

    def test(self, context=None):
        context = {**ADAPTER_CONTRACTS[self.definition["adapter"]]["sample_context"], **(context or {})}
        adapter = self.definition["adapter"]
        if adapter == "bars_json":
            result = self.fetch_bars(context["symbol"])
        elif adapter == "screener_json":
            result = self.fetch_symbols()
        elif adapter == "items_json":
            result = self.fetch_items(context["query"], context.get("theme", ""), context.get("symbol", ""))
        elif adapter == "quote_json":
            result = [self.fetch_quote(context["symbol"])]
        elif adapter == "fundamentals_json":
            result = [self.fetch_fundamentals(context["symbol"])]
        else:
            raise CustomSourceRequestError(f"Unsupported adapter: {adapter}")
        return {
            "status": "ok",
            "source_id": self.source_id,
            "adapter": adapter,
            "records": len(result),
            "preview": result[:3],
        }
