"""
NTCIP Monitor Application

``NTCIPMonitorApp`` is resolved lazily (PEP 562) so that importing a
dependency-free submodule — e.g. ``ntcip_monitor.ui.overlay`` — does not drag
in ``pysnmp``. ``from ntcip_monitor import NTCIPMonitorApp`` still works.
"""
__version__ = "1.0.0"

__all__ = ['NTCIPMonitorApp']


def __getattr__(name):
    """Lazily resolve this package's public names.

    Args:
        name: Attribute being looked up on the package.

    Returns:
        The resolved attribute.

    Raises:
        AttributeError: If *name* is not exported by this package.
    """
    if name == 'NTCIPMonitorApp':
        from .main import NTCIPMonitorApp
        return NTCIPMonitorApp
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
