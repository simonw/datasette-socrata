from datasette import hookimpl, Response, Forbidden
from datasette_low_disk_space_hook import space_is_running_low
import asyncio
import datetime
import httpx
import sqlite_utils
from sqlite_utils.utils import TypeTracker, maximize_csv_field_size_limit
import urllib.parse
import re
import json
import io
import csv
import textwrap

maximize_csv_field_size_limit()

is_valid_id = re.compile(r"^\w{4}\-\w{4}$").match


class DatasetError(Exception):
    pass


class ParseError(DatasetError):
    pass


def parse_url(url):
    # Given a URL like this:
    # https://data.edmonton.ca/Urban-Planning-Economy/General-Building-Permits/24uj-dj8v
    # either returns (domain, id) or raises a ParseError
    bits = urllib.parse.urlparse(url)
    if not bits.netloc:
        raise ParseError("Missing domain")
    domain = bits.netloc
    potential_id = bits.path.split("/")[-1]
    if not is_valid_id(potential_id):
        raise ParseError("Last element of path was not a valid ID")
    return domain, potential_id


class MetadataError(DatasetError):
    pass


class DiskSpaceLow(Exception):
    pass


async def parse_url_fetch_metadata(url):
    domain, id = parse_url(url)
    return domain, id, await fetch_metadata(domain, id)


async def fetch_metadata(domain, id):
    metadata_url = "https://{}/api/views/{}.json".format(domain, id)
    try:
        async with httpx.AsyncClient() as client:
            metadata_response = await client.get(metadata_url)
            if metadata_response.status_code != 200:
                raise MetadataError("Dataset not found")
            else:
                return metadata_response.json()
    except httpx.HTTPError as e:
        raise MetadataError("HTTP error fetching metadata for dataset: {}".format(e))


async def get_row_count(domain, id):
    # Fetch the row count too - we ignore errors and keep row_count at None
    async with httpx.AsyncClient() as client:
        count_url = "https://{}/resource/{}.json?$select=count(*)".format(domain, id)
        count_response = await client.get(count_url)
        if count_response.status_code == 200:
            count_data = count_response.json()
            if (
                isinstance(count_data, list)
                and len(count_data) == 1
                and list(count_data[0].keys())[0].startswith("count")
            ):
                return int(list(count_data[0].values())[0])
    return None


async def import_socrata(request, datasette):
    if not await datasette.permission_allowed(
        request.actor, "import-socrata", default=False
    ):
        raise Forbidden("Permission denied for import-socrata")

    async def _error(message, status=400):
        return Response.html(
            await datasette.render_template(
                "datasette_socrata_error.html",
                {
                    "error": message,
                },
                request=request,
            ),
            status=status,
        )

    # Config can be used to restrict to a named database
    config = datasette.plugin_config("datasette-socrata") or {}
    configured_database = config.get("database")

    supported_databases = [
        db
        for db in datasette.databases.values()
        if db.is_mutable
        and db.name != "_internal"
        and await db.execute_write_fn(
            lambda conn: sqlite_utils.Database(conn).journal_mode == "wal"
        )
    ]
    if not supported_databases and not configured_database:
        return await _error(
            "There are no attached databases which can be written to and are running in WAL mode."
        )

    if configured_database:
        if configured_database not in [db.name for db in supported_databases]:
            return await _error(
                "Configured database '{}' is not both writable and running in WAL mode.".format(
                    configured_database
                )
            )
        supported_databases = [
            db for db in supported_databases if db.name == configured_database
        ]

    if request.method != "POST":
        url = request.args.get("url") or ""
        error = None
        metadata = None
        row_count = None
        if url:
            try:
                domain, id, metadata = await parse_url_fetch_metadata(url)
            except DatasetError as e:
                error = str(e)
            else:
                row_count = await get_row_count(domain, id)
        return Response.html(
            await datasette.render_template(
                "datasette_socrata.html",
                {
                    "error": error,
                    "url": url,
                    "fetched_metadata": metadata,
                    "row_count": row_count,
                    "databases": [db.name for db in supported_databases],
                },
                request=request,
            )
        )

    vars = await request.post_vars()
    url = vars.get("url") or ""
    error = None
    try:
        domain, id, metadata = await parse_url_fetch_metadata(url)
    except DatasetError as e:
        error = str(e)
    else:
        row_count = None
        if not error:
            row_count = await get_row_count(domain, id)

    if error:
        return Response.html(
            await datasette.render_template(
                "datasette_socrata.html", {"error": error}, request=request
            )
        )

    # Which database?
    if len(supported_databases) == 1:
        database = supported_databases[0]
    else:
        database_name = vars.get("database")
        filtered = [db for db in supported_databases if db.name == database_name]
        if not filtered:
            return await _error("You need to pick a database.")
        database = filtered[0]

    # Ensure table exists
    await database.execute_write(
        textwrap.dedent(
            """
        create table if not exists socrata_imports (
            id text primary key,
            name text,
            url text,
            metadata text,
            row_count integer,
            row_progress integer,
            import_started text,
            import_complete text
        );
        """
        )
    )

    # Run the actual import here
    # First, write the metadata to the `socrata_imports` table:
    await database.execute_write_fn(
        lambda conn: sqlite_utils.Database(conn)["socrata_imports"].insert(
            {
                "id": metadata["id"],
                "name": metadata["name"],
                "url": url,
                "metadata": json.dumps(metadata),
                "row_count": row_count,
                "row_progress": 0,
                "import_started": datetime.datetime.utcnow().isoformat() + "Z",
                "error": None,
            },
            replace=True,
            alter=True,
        )
    )
    await refresh_in_memory_socrata_metadata(datasette)

    # Now start the import, in a task which runs after this request has returned
    table_name = "socrata_" + id.replace("-", "_")

    # If the table exists already, delete it
    await database.execute_write_fn(
        lambda conn: sqlite_utils.Database(conn)[table_name].drop(ignore=True)
    )

    async def run_the_import_catch_errors():
        try:
            await run_the_import()
        except Exception as error:
            await database.execute_write_fn(
                lambda conn: sqlite_utils.Database(conn)["socrata_imports"].update(
                    id,
                    {"error": str(error)},
                )
            )

    async def run_the_import():
        csv_url = "https://{}/api/views/{}/rows.csv".format(domain, id)

        async def write_batch(rows):
            if await space_is_running_low(datasette):
                raise DiskSpaceLow("Disk space is running low")

            def _write(conn):
                db = sqlite_utils.Database(conn)
                with db.conn:
                    db[table_name].insert_all(rows)
                    db.execute(
                        "update socrata_imports set row_progress = row_progress + ? where id = ?",
                        (len(rows), id),
                    )

            return await database.execute_write_fn(_write, block=True)

        async with httpx.AsyncClient() as client:
            async with client.stream("GET", csv_url) as response:
                reader = AsyncDictReader(response.aiter_lines())
                tracker = TypeTracker()
                batch = []
                async for row in reader:
                    batch.append(row)
                    if len(batch) >= 100:
                        # Write this batch to disk
                        await write_batch(list(tracker.wrap(batch)))
                        batch = []
                if batch:
                    await write_batch(list(tracker.wrap(batch)))

                # Convert columns to detected types
                await database.execute_write_fn(
                    lambda conn: sqlite_utils.Database(conn)[table_name].transform(
                        types=tracker.types
                    )
                )

                # Record import completion time
                await database.execute_write_fn(
                    lambda conn: sqlite_utils.Database(conn)["socrata_imports"].update(
                        id,
                        {
                            "import_complete": datetime.datetime.utcnow().isoformat()
                            + "Z"
                        },
                    )
                )

    asyncio.ensure_future(run_the_import_catch_errors())

    # Wait for up to 1 second for the table to exist, then redirect to it
    i = 0
    while i < 10:
        if (
            await database.execute(
                "select 1 from sqlite_master where tbl_name = ?", [table_name]
            )
        ).first():
            return Response.redirect(
                datasette.urls.table(database=database.name, table=table_name)
            )
        i += 1
        await asyncio.sleep(0.1)
    # Set a message about the table being created and redirect to database page
    datasette.add_message(request, "Import has started", datasette.INFO)
    return Response.redirect(datasette.urls.database(database=database.name))


class AsyncDictReader:
    def __init__(self, async_line_iterator):
        self.async_line_iterator = async_line_iterator
        self.buffer = io.StringIO()
        self.reader = csv.DictReader(self.buffer)
        self.line_num = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.line_num == 0:
            header = await self.async_line_iterator.__anext__()
            self.buffer.write(header)

        line = await self.async_line_iterator.__anext__()

        if not line:
            raise StopAsyncIteration

        self.buffer.write(line)
        self.buffer.seek(0)

        try:
            result = next(self.reader)
        except StopIteration as e:
            raise StopAsyncIteration from e

        self.buffer.seek(0)
        self.buffer.truncate(0)
        self.line_num = self.reader.line_num

        return result


@hookimpl
def register_routes():
    return [(r"^/-/import-socrata$", import_socrata)]


@hookimpl
def permission_allowed(actor, action):
    if action == "import-socrata" and actor and actor.get("id") == "root":
        return True


@hookimpl
def menu_links(datasette, actor):
    async def inner():
        if await datasette.permission_allowed(actor, "import-socrata", default=False):
            return [
                {
                    "href": datasette.urls.path("/-/import-socrata"),
                    "label": "Import from Socrata",
                },
            ]

    return inner


PROGRESS_JS = """
const SOCRATA_PROGRESS_CSS = `
.datasette-socrata-progress {
    position: relative;
}
.datasette-socrata-progress progress {
    -webkit-appearance: none;
    appearance: none;
    border: none;
    width: 100%;
    height: 2em;
    margin-top: 1em;
    margin-bottom: 1em;
}
.datasette-socrata-progress div {
    position: absolute;
    top: 0px;
    left: 10px;
    line-height: 2em;
    color: #fff;
    text-shadow: 1px 1px 3px #2f2c2c;
    font-size: 14px;
}
.datasette-socrata-progress progress::-webkit-progress-bar {
    background-color: #ddd;
}
.datasette-socrata-progress progress::-webkit-progress-value {
    background-color: #124d77;
}
`;
(function() {
    const style = document.createElement("style");
    style.innerHTML = SOCRATA_PROGRESS_CSS;
    const pollUrl = !POLL_URL!;
    document.head.appendChild(style);
    const wrapper = document.createElement('div');
    wrapper.setAttribute('class', 'datasette-socrata-progress');
    const progress = document.createElement('progress');
    wrapper.appendChild(progress);
    const text = document.createElement('div');
    wrapper.appendChild(text);
    progress.setAttribute('value', 0);
    progress.setAttribute('max', 100);
    progress.style.display = 'none';
    progress.innerHTML = 'Importing from Socrata...';
    const table = document.querySelector('table.rows-and-columns');
    table.parentNode.insertBefore(wrapper, table);
    // Only show message about completion if we have polled a different
    // value at least once.
    let hasPolled = false;
    /* Start polling */
    function pollNext() {
        fetch(pollUrl).then(r => r.json()).then(rows => {
            if (rows.length) {
                const row = rows[0];
                if (row.error) {
                    alert(`Error: ${row.error}`);
                    progress.style.display = 'none';
                    return;
                }
                if (row.row_count > row.row_progress) {
                    progress.style.display = 'block';
                    text.innerText = `${row.row_progress.toLocaleString()} / ${row.row_count.toLocaleString()}`;
                    progress.setAttribute('value', row.row_progress);
                    progress.setAttribute('max', row.row_count);
                    setTimeout(pollNext, 2000);
                    hasPolled = true;
                } else {
                    if (hasPolled) {
                        text.innerText = `${row.row_progress.toLocaleString()} rows loaded - refresh the page to see them`;
                    } else {
                        progress.style.display = 'none';
                    }
                }
            }
        });
    }
    pollNext();
})();
"""


@hookimpl
def extra_body_script(view_name, table, database, datasette):
    if not table:
        return
    dataset_id = table.replace("socrata_", "").replace("_", "-")
    if (
        view_name == "table"
        and table.startswith("socrata_")
        and is_valid_id(dataset_id)
    ):
        return PROGRESS_JS.replace(
            "!POLL_URL!",
            json.dumps(
                "{}.json?id={}&_shape=array&_col=row_count&_col=row_progress&_col=error".format(
                    datasette.urls.table(database=database, table="socrata_imports"),
                    dataset_id,
                )
            ),
        )


async def refresh_in_memory_socrata_metadata(datasette):
    datasette._socrata_metadata = {}
    databases = {}
    for database_name, db in datasette.databases.items():
        tables = {}
        if "socrata_imports" in await db.table_names():
            table_metadata = {
                row["id"]: dict(row, metadata=json.loads(row["metadata"]))
                for row in await db.execute("select * from socrata_imports")
            }
        else:
            continue
        for table_id, info in table_metadata.items():
            table_metadata = {
                "title": info["name"],
                "source": info["metadata"].get("attribution")
                or info["url"].split("//")[1].split("/")[0],
                "source_url": info["url"],
            }
            description = info["metadata"].get("description")
            if description:
                table_metadata["description"] = description
            column_descriptions = {
                c["name"]: c.get("description")
                for c in (info.get("metadata") or {}).get("columns") or []
                if c.get("description")
            }
            if column_descriptions:
                table_metadata["columns"] = column_descriptions
            if info["metadata"].get("license"):
                license_name = info["metadata"]["license"].get("name")
                license_url = info["metadata"]["license"].get("termsLink")
                if license_name and license_url:
                    table_metadata["license"] = license_name
                    table_metadata["license_url"] = license_url
            tables["socrata_{}".format(table_id.replace("-", "_"))] = table_metadata
        databases[database_name] = {
            "tables": tables,
        }
    datasette._socrata_metadata = {"databases": databases}


# Populate datasette._socrata_metadata on startup
@hookimpl
def startup(datasette):
    async def inner():
        await refresh_in_memory_socrata_metadata(datasette)

    return inner


@hookimpl
def get_metadata(datasette):
    return getattr(datasette, "_socrata_metadata", None)
