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
    sqlite_utils.Database(db_path).enable_wal()
    return Datasette([db_path])


@pytest.fixture
def datasette2(tmpdir):
    # Datasette with two attached WAL databases, and one not-WAL
    db_paths = []
    for name in ("data2.db", "data3.db", "notwal.db"):
        db_path = str(tmpdir / name)
        if name == "notwal.db":
            sqlite_utils.Database(db_path).vacuum()
        else:
            sqlite_utils.Database(db_path).enable_wal()
        db_paths.append(db_path)
    return Datasette(db_paths)


def mock_metadata_and_count(httpx_mock):
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


@pytest.mark.asyncio
async def test_import_shows_preview(datasette, httpx_mock):
    mock_metadata_and_count(httpx_mock)
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
    assert "<p><strong>General Building Permits</strong> - 2 rows</p>" in html
    assert ">List of issued building permits from the City of Edmonton<" in html
    assert (
        "<p>Data will be imported into the <strong>data</strong> database.</p>" in html
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("number_of_nowal_databases", (0, 1, 2))
async def test_configuration_error_no_databases(tmpdir, number_of_nowal_databases):
    db_paths = []
    for i in range(number_of_nowal_databases):
        db_path = str(tmpdir / "db{}.db".format(i))
        sqlite_utils.Database(db_path).vacuum()
        db_paths.append(db_path)
    ds = Datasette(db_paths)
    response = await ds.client.get(
        "/-/import-socrata",
        cookies={"ds_actor": ds.sign({"a": {"id": "root"}}, "actor")},
    )
    assert response.status_code == 400
    assert (
        "There are no attached databases which can be written to and are running in WAL mode"
        in response.text
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("immutable", (False, True))
async def test_configuration_error_bad_configured_database(tmpdir, immutable):
    db_path = str(tmpdir / "data.db")
    db_paths = []
    immutables = []
    if immutable:
        immutables = [db_path]
        sqlite_utils.Database(db_path).enable_wal()
    else:
        db_paths = [db_path]
        sqlite_utils.Database(db_path).vacuum()
    ds = Datasette(
        db_paths,
        immutables=immutables,
        metadata={"plugins": {"datasette-socrata": {"database": "data"}}},
    )
    response = await ds.client.get(
        "/-/import-socrata",
        cookies={"ds_actor": ds.sign({"a": {"id": "root"}}, "actor")},
    )
    assert response.status_code == 400
    assert "is not both writable and running in WAL mode" in response.text


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
async def test_import_select_database_if_multiple_options(datasette2):
    response = await datasette2.client.get(
        "/-/import-socrata",
        cookies={"ds_actor": datasette2.sign({"a": {"id": "root"}}, "actor")},
    )
    assert response.status_code == 200
    html = response.text
    assert '<select name="database">' in html
    assert "<option>data2</option>" in html
    assert "<option>data3</option>" in html


@pytest.mark.asyncio
@pytest.mark.parametrize("database", ("data2", "data3"))
async def test_import(datasette2, httpx_mock, database):
    mock_metadata_and_count(httpx_mock)
    httpx_mock.add_response(
        url="https://data.edmonton.ca/api/views/24uj-dj8v/rows.csv",
        text="id,species\r\n1,Dog\r\n2,Chicken",
    )

    # Hit preview page, mainly to get a CSRFtoken
    response = await datasette2.client.get(
        "/-/import-socrata",
        params={
            "url": "https://data.edmonton.ca/Urban-Planning-Economy/General-Building-Permits/24uj-dj8v"
        },
        cookies={"ds_actor": datasette2.sign({"a": {"id": "root"}}, "actor")},
    )
    assert response.status_code == 200
    csrftoken = response.cookies["ds_csrftoken"]

    # Now POST to kick off the import
    import_response = await datasette2.client.post(
        "/-/import-socrata",
        data={
            "url": "https://data.edmonton.ca/Urban-Planning-Economy/General-Building-Permits/24uj-dj8v",
            "csrftoken": csrftoken,
            "database": database,
        },
        cookies={
            "ds_actor": datasette2.sign({"a": {"id": "root"}}, "actor"),
            "ds_csrftoken": csrftoken,
        },
    )
    assert import_response.status_code == 302
    assert import_response.headers["location"] == "/{}/socrata_24uj_dj8v".format(
        database or "data2"
    )
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
    db = sqlite_utils.Database(datasette2.get_database(database).connect())
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
async def test_import_if_config_specifies_database(datasette2, httpx_mock):
    mock_metadata_and_count(httpx_mock)
    datasette2._metadata_local = {
        "plugins": {"datasette-socrata": {"database": "data3"}}
    }
    # Hit preview page, mainly to get a CSRFtoken
    response = await datasette2.client.get(
        "/-/import-socrata",
        cookies={"ds_actor": datasette2.sign({"a": {"id": "root"}}, "actor")},
    )
    assert response.status_code == 200
    csrftoken = response.cookies["ds_csrftoken"]
    assert (
        "<p>Data will be imported into the <strong>data3</strong> database.</p>"
        in response.text
    )

    # Now POST to kick off the import
    import_response = await datasette2.client.post(
        "/-/import-socrata",
        data={
            "url": "https://data.edmonton.ca/Urban-Planning-Economy/General-Building-Permits/24uj-dj8v",
            "csrftoken": csrftoken,
        },
        cookies={
            "ds_actor": datasette2.sign({"a": {"id": "root"}}, "actor"),
            "ds_csrftoken": csrftoken,
        },
    )
    assert import_response.status_code == 302


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


@pytest.mark.asyncio
@pytest.mark.parametrize("auth", [True, False])
async def test_menu(auth):
    ds = Datasette(memory=True)
    cookies = {}
    if auth:
        cookies = {"ds_actor": ds.sign({"a": {"id": "root"}}, "actor")}
    response = await ds.client.get("/", cookies=cookies)
    assert response.status_code == 200
    if auth:
        assert "/-/import-socrata" in response.text
    else:
        assert "/-/import-socrata" not in response.text
