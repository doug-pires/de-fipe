import pytest

from fipe.elt.load import join_path_table, read_delta_table


def test_if_join_path_correctly():
    # Given a Dummy Path and Delta Table Name
    dummy_path = "mnt/bronze"
    delta_table_name = "my_table"
    # WHen I call the function to Join the Path, must return exatcly what I want
    expected_path = "mnt/bronze/my_table"
    current_path = join_path_table(path=dummy_path, delta_table_name=delta_table_name)

    # Then assert the Path are correct

    assert current_path == expected_path


def test_read_table_with_incorrect_path(spark_session):
    # Given the WRONG PATH
    dummy_path = "/mnt/bronze/any/path"
    # When we TRY to save the Delta Table we EXPECT raise an EXCEPTION because the PATH is WRONG

    # Then
    with pytest.raises(FileNotFoundError):
        read_delta_table(
            spark=spark_session, path=dummy_path, delta_table_name="delta_table_name"
        )


if __name__ == "__main__":
    pytest.main(["-v", "--setup-show", "-k", "load"])
