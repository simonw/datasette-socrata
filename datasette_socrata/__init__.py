from datasette import hookimpl, Response
import asyncio
import datetime
import httpx
import sqlite_utils
from sqlite_utils.utils import TypeTracker
import urllib.parse
import re
import json
import io
import csv
import textwrap


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


async def socrata(request, datasette):
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

    # Run the actual import here
    # First, write the metadata to the `socrata_imports` table:
    database = datasette.get_database("data")  # TODO: Configure database
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
            },
            replace=True,
        )
    )

    # Now start the import, in a task which runs after this request has returned
    table_name = "socrata_" + id.replace("-", "_")

    # If the table exists already, delete it
    await database.execute_write_fn(
        lambda conn: sqlite_utils.Database(conn)[table_name].drop(ignore=True)
    )

    async def run_the_import():
        csv_url = "https://{}/api/views/{}/rows.csv".format(domain, id)

        async def write_batch(rows):
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

    asyncio.ensure_future(run_the_import())

    # Wait for 1 second for the table to be created, then redirect to it
    await asyncio.sleep(1.0)
    return Response.redirect("/data/{}".format(table_name))


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
    return [(r"^/-/import-socrata$", socrata)]


@hookimpl
def startup(datasette):
    async def inner():
        db = datasette.get_database("data")
        await db.execute_write_fn(lambda conn: sqlite_utils.Database(conn).enable_wal())
        await db.execute_write(
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

    return inner
