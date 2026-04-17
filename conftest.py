"""Root conftest — suppress third-party import-time warnings early."""
import warnings


def pytest_configure(config):
    """Runs before collection; filters noisy third-party warnings."""
    try:
        from urllib3.exceptions import NotOpenSSLWarning
        warnings.filterwarnings("ignore", category=NotOpenSSLWarning)
    except ImportError:
        pass
    warnings.filterwarnings("ignore", category=FutureWarning, module=r"google\..*")
