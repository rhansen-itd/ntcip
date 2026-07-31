"""NTCIP Monitor - UI Package

``WebUI`` is resolved lazily (PEP 562) so that ``ntcip_monitor.ui.overlay`` —
which is deliberately dependency-free — imports without Flask installed.
``from ntcip_monitor.ui import WebUI`` still works.
"""

__all__ = ['WebUI']


def __getattr__(name):
    """Lazily resolve this package's public names.

    Args:
        name: Attribute being looked up on the package.

    Returns:
        The resolved attribute.

    Raises:
        AttributeError: If *name* is not exported by this package.
    """
    if name == 'WebUI':
        from .web_ui import WebUI
        return WebUI
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
