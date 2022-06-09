from datasette.app import Datasette
import sqlite_utils
import pytest
import httpx


@pytest.fixture
def non_mocked_hosts():
    return ["localhost"]


@pytest.fixture
def datasette(tmpdir):
    db_path = str(tmpdir / "data.db")
    sqlite_utils.Database(db_path).vacuum()
    return Datasette([db_path])


@pytest.mark.asyncio
async def test_import_shows_preview(datasette, httpx_mock):
    httpx_mock.add_response(
        url="https://data.edmonton.ca/api/views/24uj-dj8v.json",
        json={
            "id": "24uj-dj8v",
            "name": "General Building Permits",
            "description": "List of issued building permits from the City of Edmonton",
        },
    )
    httpx_mock.add_response(
        url="https://data.edmonton.ca/resource/24uj-dj8v.json?$select=count(*)",
        json=[{"count": "123"}],
    )

    response = await datasette.client.get(
        "/-/import-socrata",
        params={
            "url": "https://data.edmonton.ca/Urban-Planning-Economy/General-Building-Permits/24uj-dj8v"
        },
    )
    assert response.status_code == 200
    request = httpx_mock.get_requests()[0]
    assert request.url == "https://data.edmonton.ca/api/views/24uj-dj8v.json"
    html = response.text
    assert '<p class="message-error">' not in html
    assert "<p><strong>General Building Permits</strong> - 123 rows</p>" in html
    assert ">List of issued building permits from the City of Edmonton<" in html


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "scenario,expected_error",
    (
        ("404", "Dataset not found"),
        ("http_error", "HTTP error fetching metadata for dataset"),
        ("invalid_url", "Missing domain"),
        ("invalid_id", "Last element of path was not a valid ID"),
    ),
)
async def test_import_shows_preview_errors(
    datasette, httpx_mock, scenario, expected_error
):
    url = "https://data.edmonton.ca/Urban-Planning-Economy/General-Building-Permits/24uj-dj8v"
    if scenario == "404":
        httpx_mock.add_response(
            url="https://data.edmonton.ca/api/views/24uj-dj8v.json",
            status_code=404,
        )
    elif scenario == "http_error":
        httpx_mock.add_exception(httpx.ReadTimeout("Unable to read within timeout"))
    elif scenario == "invalid_url":
        url = "htt"
    elif scenario == "invalid_id":
        url = "https://data.edmonton.ca/api/views/24uj-dj8.json"
    response = await datasette.client.get(
        "/-/import-socrata",
        params={
            "url": url,
        },
    )
    assert response.status_code == 200
    html = response.text
    assert '<p class="message-error">' in html
    assert expected_error in html
