from gemini_python import CqlDto
from gemini_python.subprocess_executor import SubprocessQueryExecutor


def test_can_query_using_executor_in_subprocess():
    executor = SubprocessQueryExecutor()
    result = executor.execute(CqlDto(statement="select * from test", values=()))
    assert (
        result is None
    )  # uses NoOpQueryExecutor so it's always None but at least we run this code and verify it ends
