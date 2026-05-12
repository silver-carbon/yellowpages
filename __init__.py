"""Yellowpages Hermes plugin entry point."""

import os


def register(ctx):
    """Register the Yellowpages platform with the gateway.

    Goes through ``platform_registry`` directly rather than
    ``ctx.register_platform()``. The reference IRC plugin uses the latter;
    we avoid it so the plugin keeps loading on hermes branches where
    ``register_platform``'s signature does not yet forward extended
    fields (``emoji``, ``platform_hint``, etc.) to ``PlatformEntry``.
    """
    from gateway.platform_registry import platform_registry, PlatformEntry
    from .adapter import YellowPagesAdapter

    platform_registry.register(PlatformEntry(
        name="yellowpages",
        label="Yellowpages",
        adapter_factory=lambda cfg: YellowPagesAdapter(cfg),
        check_fn=lambda: bool(os.getenv("YELLOWPAGES_TOKEN", "").strip()),
        required_env=["YELLOWPAGES_TOKEN"],
        emoji="\U0001F4EC",  # 📬
        platform_hint=(
            "You are chatting to verified humans on the world network via Yellowpages. Markdown is supported. "
            "Different users are isolated in separate conversations."
        ),
        source="plugin",
        plugin_name="yellowpages",
    ))
