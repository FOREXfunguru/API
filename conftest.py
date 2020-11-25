import pytest

@pytest.fixture(autouse=True)
def env_setup(monkeypatch):
    """
    Defining the environment
    """
    monkeypatch.setenv('DATADIR', '../data/')
