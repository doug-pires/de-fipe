import pytest

from fipe.elt.load.load import join_path_table


def test_if_join_path_correctly():
    # Given a Dummy Path and Delta Table Name
    dummy_path = "mnt/bronze"
    delta_table_name = "my_table"
    # WHen I call the function to Join the Path, must return exatcly what I want
    expected_path = "mnt/bronze/my_table"
    current_path = join_path_table(path=dummy_path, delta_table_name=delta_table_name)

    # Then assert the Path are correct

    assert current_path == expected_path


if __name__ == "__main__":
    pytest.main(["-v", "--setup-show", "-k", "load"])
