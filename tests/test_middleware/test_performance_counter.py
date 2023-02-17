from gemini_python import GeminiConfiguration
from gemini_python.middleware.performance_counter import PerformanceCounterMiddleware


def test_performance_counter_middleware_counts_queries(caplog):
    config = GeminiConfiguration()
    middleware = PerformanceCounterMiddleware(config=config)
    middleware.run()
    middleware.run()
    middleware.run()
    middleware.teardown()
    assert "Queries per second" in caplog.text
    assert "Executed 3 queries" in caplog.text
