from gemini_python import CqlDto
from gemini_python.subprocess_query_driver import SubprocessQueryDriver


def test_can_query_using_query_driver_in_subprocess():
    query_driver = SubprocessQueryDriver()
    result = query_driver.execute(CqlDto(statement="select * from test", values=()))
    assert (
        result is None
    )  # uses NoOpQueryDriver, so it's always None but at least we run this code and verify it ends
