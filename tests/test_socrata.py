from datasette.app import Datasette
import sqlite_utils
import pytest
import httpx
import asyncio


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
        cookies={"ds_actor": datasette.sign({"a": {"id": "root"}}, "actor")},
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
        cookies={"ds_actor": datasette.sign({"a": {"id": "root"}}, "actor")},
    )
    assert response.status_code == 200
    html = response.text
    assert '<p class="message-error">' in html
    assert expected_error in html


@pytest.mark.asyncio
async def test_import(datasette, httpx_mock):
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
        json=[{"count": "2"}],
    )
    httpx_mock.add_response(
        url="https://data.edmonton.ca/api/views/24uj-dj8v/rows.csv",
        text="id,species\r\n1,Dog\r\n2,Chicken",
    )

    # Hit preview page, mainly to get a CSRFtoken
    response = await datasette.client.get(
        "/-/import-socrata",
        params={
            "url": "https://data.edmonton.ca/Urban-Planning-Economy/General-Building-Permits/24uj-dj8v"
        },
        cookies={"ds_actor": datasette.sign({"a": {"id": "root"}}, "actor")},
    )
    assert response.status_code == 200
    csrftoken = response.cookies["ds_csrftoken"]

    # Now POST to kick off the import
    import_response = await datasette.client.post(
        "/-/import-socrata",
        data={
            "url": "https://data.edmonton.ca/Urban-Planning-Economy/General-Building-Permits/24uj-dj8v",
            "csrftoken": csrftoken,
        },
        cookies={
            "ds_actor": datasette.sign({"a": {"id": "root"}}, "actor"),
            "ds_csrftoken": csrftoken,
        },
    )
    assert import_response.status_code == 302
    assert import_response.headers["location"] == "/data/socrata_24uj_dj8v"
    await asyncio.sleep(1.5)
    requests = httpx_mock.get_requests()
    assert [str(req.url) for req in requests] == [
        "https://data.edmonton.ca/api/views/24uj-dj8v.json",
        "https://data.edmonton.ca/resource/24uj-dj8v.json?$select=count(*)",
        "https://data.edmonton.ca/api/views/24uj-dj8v.json",
        "https://data.edmonton.ca/resource/24uj-dj8v.json?$select=count(*)",
        "https://data.edmonton.ca/api/views/24uj-dj8v/rows.csv",
    ]
    # Was the db correctly created?
    db = sqlite_utils.Database(datasette.get_database("data").connect())
    assert set(db.table_names()) == {"socrata_24uj_dj8v", "socrata_imports"}
    assert (
        list(db["socrata_imports"].rows)[0].items()
        >= {
            "id": "24uj-dj8v",
            "name": "General Building Permits",
            "url": "https://data.edmonton.ca/Urban-Planning-Economy/General-Building-Permits/24uj-dj8v",
            "metadata": '{"id": "24uj-dj8v", "name": "General Building Permits", "description": "List of issued building permits from the City of Edmonton"}',
            "row_count": 2,
            "row_progress": 2,
        }.items()
    )
    assert list(db["socrata_24uj_dj8v"].rows) == [
        {"id": 1, "species": "Dog"},
        {"id": 2, "species": "Chicken"},
    ]


@pytest.mark.asyncio
async def test_permissions(datasette):
    response = await datasette.client.get("/-/import-socrata")
    assert response.status_code == 403
    # Now try with a root actor
    response2 = await datasette.client.get(
        "/-/import-socrata",
        cookies={"ds_actor": datasette.sign({"a": {"id": "root"}}, "actor")},
    )
    assert response2.status_code == 200
