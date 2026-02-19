from skidname import main


def test_get_secrets_from_gcp_location(mocker):
    mocker.patch("pathlib.Path.exists", return_value=True)
    mocker.patch("pathlib.Path.read_text", return_value='{"foo":"bar"}')

    secrets = main._get_secrets()

    assert secrets == {"foo": "bar"}


def test_get_secrets_from_local_location(mocker):
    exists_mock = mocker.Mock(side_effect=[False, True])
    mocker.patch("pathlib.Path.exists", new=exists_mock)
    mocker.patch("pathlib.Path.read_text", return_value='{"foo":"bar"}')

    secrets = main._get_secrets()

    assert secrets == {"foo": "bar"}
    assert exists_mock.call_count == 2
