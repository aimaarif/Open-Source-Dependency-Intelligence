
import requests
import pytest

from unittest.mock import Mock

from src.extract.pypi import fetch_package


def make_response(status_code, payload=None):
    response = Mock()
    response.status_code = status_code
    response.headers = {}
    response.json.return_value = payload or {
        "info": {
            "name": "django",
            "version": "5.2",
        },
        "releases": {},
    }

    if status_code >= 400:
        error = requests.HTTPError(f"HTTP {status_code}")
        error.response = response
        response.raise_for_status.side_effect = error

    return response


def test_fetch_package_success():
    session = Mock()
    session.get.return_value = make_response(200)

    payload, status, url = fetch_package(
        "django",
        session=session,
    )

    assert status == 200
    assert payload["info"]["name"] == "django"
    assert url == "https://pypi.org/pypi/django/json"

    session.get.assert_called_once_with(
        "https://pypi.org/pypi/django/json",
        timeout=15,
    )


def test_fetch_package_retries_server_error(monkeypatch):
    monkeypatch.setattr("src.extract.pypi.time.sleep", lambda _: None)

    session = Mock()
    session.get.side_effect = [
        make_response(503),
        make_response(200),
    ]

    payload, status, _ = fetch_package(
        "django",
        session=session,
        max_attempts=3,
    )

    assert status == 200
    assert payload["info"]["name"] == "django"
    assert session.get.call_count == 2


def test_fetch_package_does_not_retry_not_found():
    session = Mock()
    session.get.return_value = make_response(404)

    with pytest.raises(requests.HTTPError):
        fetch_package(
            "nonexistent-example-package",
            session=session,
        )

    session.get.assert_called_once()


def test_fetch_package_retries_timeout(monkeypatch):
    monkeypatch.setattr("src.extract.pypi.time.sleep", lambda _: None)

    session = Mock()
    session.get.side_effect = [
        requests.Timeout("Request timed out"),
        make_response(200),
    ]

    payload, status, _ = fetch_package(
        "django",
        session=session,
    )

    assert status == 200
    assert payload["info"]["name"] == "django"
    assert session.get.call_count == 2


def test_fetch_package_rejects_empty_name():
    with pytest.raises(ValueError):
        fetch_package("   ")


def test_fetch_package_rejects_zero_attempts():
    with pytest.raises(ValueError):
        fetch_package("django", max_attempts=0)
