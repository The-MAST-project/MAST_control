import io
import logging
import re
import socket
import zipfile
from collections import OrderedDict
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from MAST_common.canonical import CanonicalResponse
from MAST_common.config import Config
from MAST_common.const import Const
from MAST_common.mast_logging import init_log
from MAST_common.proxy import ProxyContext

logger = logging.getLogger("mast.control.data_server")
init_log(logger)


class DataServer:
    """
    Singleton service for serving data under /Storage/mast-share.
    All endpoint paths use base_path = Const.BASE_DATA_PATH.
    """

    storage_root = "/Storage/mast-share/MAST/"

    _instance = None
    _initialized = False

    @property
    def name(self) -> str:
        return socket.gethostname()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(DataServer, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._initialized = True

    def autofocus(self, unit_name: str, request: Request) -> CanonicalResponse:
        """
        Returns a tree hierarchy of autofocus sessions and files for the given unit.
        Sorted by date (descending) then by session sequence (descending).
        """
        root = Path(f"{self.storage_root}/{unit_name}")
        # Use OrderedDict to ensure the JSON response preserves newest-first ordering
        result: OrderedDict[str, OrderedDict[str, dict]] = OrderedDict()

        # Helpers to produce sortable keys
        def _date_key(name: str):
            # Accept only folder names of the exact form YYYY-MM-DD.
            # If the name matches, parse and return (0, timestamp) so these sort first.
            # Otherwise return (1, name) so non-matching names sort after valid dates.
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", name):
                try:
                    dt = datetime.strptime(name, "%Y-%m-%d")
                    return (0, dt.timestamp())
                except Exception:
                    # If parsing somehow fails, fall back to treating as non-date
                    return (1, name)
            return (1, name)

        def _seq_key(name: str):
            # Accept only folder names that are exactly four zero-padded digits (e.g. '0001', '0123', '1234').
            # If it matches, return numeric key so sessions sort numerically; otherwise treat as non-sequence.
            if re.fullmatch(r"\d{4}", name):
                try:
                    return (0, int(name))
                except Exception:
                    return (1, name)
            return (1, name)

        def autofocus_storage_url(file_name: str) -> str:
            path = Path(file_name)
            return f"http://mast-wis-control:8008/{unit_name}/{path.relative_to(root)}"

        if not root.exists():
            return CanonicalResponse(errors=[f"Unit data not found at '{root}'"])

        # Traverse the directory tree
        # Sort dates then sessions; dict insertion order preserves desired ordering in the result.
        date_dirs = sorted(
            (d for d in root.iterdir() if d.is_dir()),
            key=lambda d: _date_key(d.name),
            reverse=True,
        )
        sites = Config().get_sites()
        site = next((s for s in sites if unit_name in s.deployed_units), None)
        if not site:
            logger.warning(f"Unit '{unit_name}' not found in any configured site")
            return CanonicalResponse(
                errors=[
                    f"Unit '{unit_name}' not found as deployed in any of the configured sites"
                ]
            )
        controller_host = site.controller_host
        controller_ipaddr = socket.gethostbyname(controller_host)
        proxy = ProxyContext.from_request(request)

        for date_dir in date_dirs:
            autofocus_dir = date_dir / "Autofocus"
            if not autofocus_dir.exists():
                continue
            session_dirs = sorted(
                (s for s in autofocus_dir.iterdir() if s.is_dir()),
                key=lambda s: _seq_key(s.name),
                reverse=True,
            )
            for session_dir in session_dirs:
                session_number = session_dir.name
                fits_files = [f.name for f in session_dir.glob("*.fz")]
                if not fits_files:
                    fits_files = [f.name for f in session_dir.glob("*.fits")]

                folder_url = proxy.rewrite(
                    f"http://{controller_ipaddr}:8008/{unit_name}/{date_dir.name}/Autofocus/{session_number}",
                    base="/mast-share/",
                )

                fits_files = (
                    [f"{folder_url}/{f}" for f in fits_files] if fits_files else []
                )
                vcurve_files = [
                    f"{folder_url}/{f.name}" for f in session_dir.glob("vcurve.png")
                ] or None
                status_files = [
                    f"{folder_url}/{f.name}" for f in session_dir.glob("status.json")
                ] or None
                if fits_files or vcurve_files or status_files:
                    result.setdefault(date_dir.name, OrderedDict())
                    result[date_dir.name][session_number] = {
                        "fits": fits_files,
                        "vcurve": vcurve_files,
                        "status": status_files,
                    }
        # logger.debug(f"autofocus: {unit_name=}, status={status}")

        return CanonicalResponse(value=result)

    def get_date_session(self, unit_name: str, date: str, session: str):
        """
        Returns the list of files for the given unit, date, and session.
        Previously returned a CanonicalResponse listing filenames.
        Now returns a ZIP archive (StreamingResponse) containing the .fits and vcurve.png files.
        """
        root = Path(f"{self.storage_root}/{unit_name}/{date}/Autofocus/{session}")

        if not root.exists():
            return CanonicalResponse(errors=[f"Session data not found at '{root}'"])

        fits_paths = list(root.glob("*.fz"))
        if not fits_paths:
            fits_paths = list(root.glob("*.fits"))
        png_paths = list(root.glob("vcurve.png"))
        all_paths = fits_paths + png_paths

        if not all_paths:
            return CanonicalResponse(
                errors=[f"No fits or vcurve files found at '{root}'"]
            )

        # Build in-memory ZIP and stream it back
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for p in all_paths:
                try:
                    zf.write(p, arcname=p.name)
                except Exception:
                    # skip files we can't read
                    continue
        buf.seek(0)

        archive_name = f"{unit_name}_{date}_{session}.zip"
        headers = {"Content-Disposition": f'attachment; filename="{archive_name}"'}
        return StreamingResponse(buf, media_type="application/zip", headers=headers)

    @property
    def api_router(self) -> APIRouter:
        base_path = Const.BASE_DATA_PATH
        router = APIRouter()

        logger.info(f"Registering DataServer API routes under '{base_path}'")
        tag = "Data Server"

        router.add_api_route(
            base_path + "/autofocus/{unit_name}",
            tags=[tag],
            endpoint=self.autofocus,
            methods=["GET"],
            response_model=CanonicalResponse,
        )

        # router.add_api_route(
        #     base_path + "/autofocus/{unit_name}/{date}/{session}",
        #     tags=[tag],
        #     endpoint=self.get_date_session,
        #     methods=["GET"],
        #     response_class=StreamingResponse,
        # )

        return router
