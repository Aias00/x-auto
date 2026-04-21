"""Optional Langfuse runtime helpers for LangGraph observability."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
import importlib
import inspect
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

@dataclass(slots=True)
class WorkflowObservation:
    """Small wrapper for a live Langfuse workflow observation."""

    context_manager: Any | None = None
    observation: Any | None = None
    trace_context: dict[str, str] | None = None


@dataclass(slots=True)
class LangfuseRuntime:
    """Runtime wrapper that degrades to a no-op when Langfuse is unavailable."""

    client: Any | None = None
    callback_handler_factory: Callable[..., Any] | None = None

    def is_enabled(self) -> bool:
        return self.client is not None and self.callback_handler_factory is not None

    def start_workflow_observation(
        self,
        *,
        run_name: str,
        metadata: dict[str, Any],
    ) -> WorkflowObservation | None:
        if not self.is_enabled():
            return None

        try:
            context_manager = self.client.start_as_current_observation(
                name=run_name,
                as_type="chain",
                metadata=metadata,
                end_on_exit=True,
            )
            observation = context_manager.__enter__()
        except Exception:
            logger.exception("langfuse workflow observation start failed")
            return None

        trace_context: dict[str, str] | None = None
        trace_id = getattr(observation, "trace_id", None)
        observation_id = getattr(observation, "id", None)
        if isinstance(trace_id, str):
            trace_context = {"trace_id": trace_id}
            if isinstance(observation_id, str):
                trace_context["parent_span_id"] = observation_id
        return WorkflowObservation(
            context_manager=context_manager,
            observation=observation,
            trace_context=trace_context,
        )

    def build_graph_config(
        self,
        *,
        run_name: str,
        observation: WorkflowObservation | None = None,
    ) -> dict[str, Any] | None:
        if not self.is_enabled():
            return None

        graph_config: dict[str, Any] = {"run_name": run_name}
        try:
            callback = self._build_callback_handler(
                trace_context=observation.trace_context if observation is not None else None,
            )
        except Exception:
            logger.exception("langfuse callback creation failed")
            return graph_config

        graph_config["callbacks"] = [callback]
        return graph_config

    def finish_workflow_observation(
        self,
        observation: WorkflowObservation | None,
        *,
        output: dict[str, Any] | None = None,
        error: Exception | None = None,
    ) -> None:
        if observation is None:
            return

        try:
            wrapped_observation = observation.observation
            update = getattr(wrapped_observation, "update", None)
            if callable(update):
                if error is not None:
                    update(output=output, level="ERROR", status_message=str(error))
                elif output is not None:
                    update(output=output)
        except Exception:
            logger.exception("langfuse workflow observation finish failed")
        finally:
            try:
                if observation.context_manager is not None:
                    observation.context_manager.__exit__(None, None, None)
                    return
                wrapped_observation = observation.observation
                end = getattr(wrapped_observation, "end", None)
                if callable(end):
                    end()
            except Exception:
                logger.exception("langfuse workflow observation close failed")

    def shutdown(self) -> None:
        if self.client is None:
            return

        try:
            shutdown = getattr(self.client, "shutdown", None)
            if callable(shutdown):
                shutdown()
        except Exception:
            logger.exception("langfuse shutdown failed")

    def _build_callback_handler(self, *, trace_context: dict[str, str] | None) -> Any:
        if self.callback_handler_factory is None:
            raise RuntimeError("callback handler factory is not configured")

        if trace_context is None:
            return self.callback_handler_factory()

        try:
            signature = inspect.signature(self.callback_handler_factory)
        except (TypeError, ValueError):
            return self.callback_handler_factory(trace_context=trace_context)

        supports_kwargs = any(
            parameter.kind == inspect.Parameter.VAR_KEYWORD
            for parameter in signature.parameters.values()
        )
        if "trace_context" in signature.parameters or supports_kwargs:
            return self.callback_handler_factory(trace_context=trace_context)
        return self.callback_handler_factory()


def build_langfuse_runtime(_config: Any) -> LangfuseRuntime:
    """Build a Langfuse runtime from ambient environment configuration."""

    public_key = os.getenv("LANGFUSE_PUBLIC_KEY")
    secret_key = os.getenv("LANGFUSE_SECRET_KEY")
    if not public_key or not secret_key:
        return LangfuseRuntime()

    try:
        langfuse_module = importlib.import_module("langfuse")
        langchain_module = importlib.import_module("langfuse.langchain")
    except Exception:
        logger.exception("langfuse import failed")
        return LangfuseRuntime()

    try:
        client = langfuse_module.get_client()
        callback_handler_factory = langchain_module.CallbackHandler
    except Exception:
        logger.exception("langfuse initialization failed")
        return LangfuseRuntime()

    return LangfuseRuntime(
        client=client,
        callback_handler_factory=callback_handler_factory,
    )
