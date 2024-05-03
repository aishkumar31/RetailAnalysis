import pytest
from lib.Utils import get_pyspark_session
@pytest.fixture
def spark():
    spark = get_pyspark_session("LOCAL")
    return spark