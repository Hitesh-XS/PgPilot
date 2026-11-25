import sys


def test_python_version():
    assert sys.version_info >= (3, 11)


def test_core_libraries_import():
    import psycopg
    import redis
    import dotenv
    import hypothesis

    assert psycopg.__version__
    assert redis.__version__
    assert dotenv and hypothesis


def test_project_packages_importable():
    import proxy.protocol
    import proxy.session
    import proxy.pool
    import proxy.classify
    import proxy.lag
    import proxy.router
    import proxy.cache
    import proxy.telemetry

    assert proxy