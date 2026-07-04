"""LLM provider factory for the incident response graphs.

Switch between Groq and Ollama using `LLM_PROVIDER` in `.env`.
"""

from __future__ import annotations

import importlib
import inspect
from typing import Any

try:
    from config import cfg
except ImportError:
    import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
    from rca_agent_workflow.config import cfg
   


def _load_class(module_name: str, candidates: list[str]) -> type[Any]:
    module = importlib.import_module(module_name)
    for class_name in candidates:
        if hasattr(module, class_name):
            return getattr(module, class_name)
    raise ImportError(
        f"Could not find any of {candidates} in package '{module_name}'."
    )


def _filter_kwargs(cls: type[Any], kwargs: dict[str, Any]) -> dict[str, Any]:
    try:
        params = inspect.signature(cls.__init__).parameters
    except (ValueError, TypeError):
        return kwargs
    if any(p.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD) for p in params.values()):
        return kwargs
    return {key: value for key, value in kwargs.items() if key in params}


def _build_llm(temperature: float = 0) -> Any:
    provider = cfg.LLM_PROVIDER.lower().strip()

    if provider == "groq":
        ChatGroq = _load_class("langchain_groq", ["ChatGroq"])
        kwargs = {
            "model": cfg.GROQ_MODEL,
            "api_key": cfg.GROQ_API_KEY,
            "temperature": temperature,
        }
        return ChatGroq(**_filter_kwargs(ChatGroq, kwargs))

    if provider == "ollama":
        OllamaClass = _load_class("langchain_ollama", ["ChatOllama", "Ollama"])
        kwargs = {
            "model": cfg.OLLAMA_MODEL,
            "temperature": temperature,
        }
        if cfg.OLLAMA_URL:
            kwargs["base_url"] = cfg.OLLAMA_URL
        if cfg.OLLAMA_API_KEY:
            kwargs["api_key"] = cfg.OLLAMA_API_KEY
        return OllamaClass(**_filter_kwargs(OllamaClass, kwargs))

    raise ValueError(
        f"Unsupported LLM_PROVIDER '{cfg.LLM_PROVIDER}'. "
        "Set LLM_PROVIDER=groq or LLM_PROVIDER=ollama in .env."
    )


def get_llm(temperature: float = 0) -> Any:
    return _build_llm(temperature=temperature)


def get_llm_with_tools(tools: list[Any], temperature: float = 0) -> Any:
    llm = _build_llm(temperature=temperature)
    if not tools:
        return llm
    if hasattr(llm, "bind_tools"):
        return llm.bind_tools(tools)
    raise RuntimeError(
        "The selected LLM provider does not support tool binding via bind_tools()."
    )
