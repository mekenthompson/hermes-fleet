"""Fail-closed Linear screenshot and short recording attachments."""
from __future__ import annotations

import re
import zlib
from collections.abc import Callable
from dataclasses import dataclass

_ALLOWED_TYPES = {
    "image/png": ({".png"}, "image"),
    "image/jpeg": ({".jpg", ".jpeg"}, "image"),
    "image/webp": ({".webp"}, "image"),
    "video/mp4": ({".mp4"}, "video"),
    "video/webm": ({".webm"}, "video"),
}
_MAX_BYTES = {"image": 10 * 1024 * 1024, "video": 25 * 1024 * 1024}
_SECRET_NAME = re.compile(
    r"(token|secret|password|credential|apikey|api[_.-]*key|private[_.-]*key|"
    r"ssh[_.-]*key|bearer|cookie|session|env(?:ironment)?[_.-]*key)",
    re.I,
)
_SAFE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,78}$")
_SAFE_ALT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 ._-]{0,78}$")
_FILE_UPLOAD = """mutation LinearFileUpload($contentType: String!, $filename: String!, $size: Int!, $makePublic: Boolean) {
  fileUpload(contentType: $contentType, filename: $filename, size: $size, makePublic: $makePublic) {
    success
    uploadFile {
      uploadUrl
      assetUrl
      headers { key value }
      contentType
      filename
      size
    }
  }
}"""
_ATTACHMENT_CREATE = """mutation LinearAttachmentCreate($input: AttachmentCreateInput!) {
  attachmentCreate(input: $input) { success }
}"""
_COMMENT_CREATE = """mutation LinearIssueComment($input: CommentCreateInput!) {
  commentCreate(input: $input) { success }
}"""


class MediaRejected(ValueError):
    """Refuse media that is not a bounded screenshot or short recording."""


@dataclass(frozen=True)
class MediaSpec:
    filename: str
    content_type: str
    size: int
    kind: str


@dataclass(frozen=True)
class UploadedAsset:
    asset_url: str
    filename: str
    content_type: str
    size: int


def admit_media(filename: object, content_type: object, size: object) -> MediaSpec:
    if not isinstance(filename, str) or not filename:
        raise MediaRejected("media filename is required")
    if ".." in filename or filename.startswith("/") or "\\" in filename:
        raise MediaRejected("media filename is not allowed")
    base = filename.rsplit("/", 1)[-1]
    compact = re.sub(r"[._-]+", "", base)
    if _SECRET_NAME.search(base) or _SECRET_NAME.search(compact) or not _SAFE_NAME.match(base):
        raise MediaRejected("media filename is not allowed")
    if not isinstance(content_type, str) or content_type not in _ALLOWED_TYPES:
        raise MediaRejected("media type is not allowed")
    exts, kind = _ALLOWED_TYPES[content_type]
    suffix = ""
    for ext in sorted(exts, key=len, reverse=True):
        if base.endswith(ext):
            suffix = ext
            break
    if suffix not in exts:
        raise MediaRejected("media type is not allowed")
    if type(size) is not int or size < 1 or size > _MAX_BYTES[kind]:
        raise MediaRejected("media size is not allowed")
    return MediaSpec(filename=base, content_type=content_type, size=size, kind=kind)


_PNG_MAGIC = b"\x89PNG\r\n\x1a\n"
_WEBM_EBML = b"\x1a\x45\xdf\xa3"
_WEBM_DOCTYPE = b"\x42\x82"
_WEBM_SEGMENT = b"\x18\x53\x80\x67"
_WEBM_INFO = b"\x15\x49\xa9\x66"
_JPEG_SOF = frozenset({0xC0, 0xC1, 0xC2, 0xC3, 0xC5, 0xC6, 0xC7, 0xC9, 0xCA, 0xCB, 0xCD, 0xCE, 0xCF})
_PNG_CHANNELS = {0: 1, 2: 3, 3: 1, 4: 2, 6: 4}
_PNG_DEPTHS = {
    0: frozenset({1, 2, 4, 8, 16}),
    2: frozenset({8, 16}),
    3: frozenset({1, 2, 4, 8}),
    4: frozenset({8, 16}),
    6: frozenset({8, 16}),
}
_MAX_DECODED = 40 * 1024 * 1024
_MP4_BRANDS = {
    b"avc1", b"dash", b"iso2", b"iso4", b"iso5", b"iso6",
    b"isom", b"M4A ", b"M4V ", b"mp41", b"mp42", b"mp71", b"msdh",
}
_MP4_TOP = {b"ftyp", b"moov", b"mdat", b"free", b"skip"}
_WEBP_CHUNKS = {
    b"VP8 ", b"VP8L", b"VP8X", b"ALPH", b"ANIM", b"ANMF",
    b"ICCP", b"EXIF", b"XMP ",
}


def _refuse_payload() -> None:
    raise MediaRejected("media payload is not allowed")


def _inflate_png(idat: bytes, expected: int) -> bytes:
    if expected < 1 or expected > _MAX_DECODED:
        _refuse_payload()
    try:
        decoder = zlib.decompressobj()
        raw = decoder.decompress(idat, expected + 1)
    except zlib.error:
        _refuse_payload()
        raise AssertionError
    if len(raw) != expected or decoder.unconsumed_tail or decoder.unused_data or not decoder.eof:
        _refuse_payload()
    return raw


def _admit_png_pixels(width: int, height: int, bit_depth: int, color_type: int, idat: bytes) -> None:
    row_bytes = (width * _PNG_CHANNELS[color_type] * bit_depth + 7) // 8
    expected = height * (1 + row_bytes)
    raw = _inflate_png(idat, expected)
    step = 1 + row_bytes
    for row in range(height):
        if raw[row * step] > 4:
            _refuse_payload()


def _admit_png(data: bytes) -> None:
    if not data.startswith(_PNG_MAGIC) or len(data) < 33:
        _refuse_payload()
    index = 8
    saw_ihdr = False
    saw_plte = False
    idat_started = False
    idat_finished = False
    width = height = bit_depth = color_type = 0
    idat = bytearray()
    while index + 12 <= len(data):
        length = int.from_bytes(data[index:index + 4], "big")
        chunk_type = data[index + 4:index + 8]
        index += 8
        if index + length + 4 > len(data):
            _refuse_payload()
        chunk = data[index:index + length]
        crc = int.from_bytes(data[index + length:index + length + 4], "big")
        if (zlib.crc32(chunk_type + chunk) & 0xFFFFFFFF) != crc:
            _refuse_payload()
        index += length + 4
        if any(byte < 65 or byte > 122 or 90 < byte < 97 for byte in chunk_type):
            _refuse_payload()
        if not saw_ihdr:
            if chunk_type != b"IHDR" or length != 13:
                _refuse_payload()
            width = int.from_bytes(chunk[0:4], "big")
            height = int.from_bytes(chunk[4:8], "big")
            bit_depth = chunk[8]
            color_type = chunk[9]
            if width < 1 or height < 1 or width > 10000 or height > 10000:
                _refuse_payload()
            if color_type not in _PNG_DEPTHS or bit_depth not in _PNG_DEPTHS[color_type]:
                _refuse_payload()
            if chunk[10] != 0 or chunk[11] != 0 or chunk[12] != 0:
                _refuse_payload()
            saw_ihdr = True
            continue
        if chunk_type == b"IDAT":
            if idat_finished or length < 1:
                _refuse_payload()
            idat.extend(chunk)
            idat_started = True
        elif chunk_type == b"IEND":
            if length != 0 or not idat or index != len(data):
                _refuse_payload()
            if color_type == 3 and not saw_plte:
                _refuse_payload()
            _admit_png_pixels(width, height, bit_depth, color_type, bytes(idat))
            return
        elif chunk_type == b"PLTE":
            if idat_started or saw_plte or length < 3 or length % 3:
                _refuse_payload()
            saw_plte = True
        else:
            if chunk_type[0] < 97:
                _refuse_payload()
            if idat_started:
                idat_finished = True
    _refuse_payload()


def _admit_jpeg(data: bytes) -> None:
    if len(data) < 4 or data[0] != 0xFF or data[1] != 0xD8:
        _refuse_payload()
    index = 2
    saw_sof = False
    saw_dht = False
    saw_sos = False
    while index < len(data):
        if data[index] != 0xFF:
            _refuse_payload()
        while index < len(data) and data[index] == 0xFF:
            index += 1
        if index >= len(data):
            _refuse_payload()
        marker = data[index]
        index += 1
        if marker == 0xD9:
            if not saw_sof or not saw_dht or not saw_sos or index != len(data):
                _refuse_payload()
            return
        if marker in (0xD8, 0x01) or 0xD0 <= marker <= 0xD7:
            _refuse_payload()
        if index + 2 > len(data):
            _refuse_payload()
        seglen = int.from_bytes(data[index:index + 2], "big")
        if seglen < 2 or index + seglen > len(data):
            _refuse_payload()
        payload = data[index + 2:index + seglen]
        if marker in _JPEG_SOF:
            if len(payload) < 6:
                _refuse_payload()
            height = int.from_bytes(payload[1:3], "big")
            width = int.from_bytes(payload[3:5], "big")
            if width < 1 or height < 1 or width > 10000 or height > 10000:
                _refuse_payload()
            saw_sof = True
        elif marker == 0xC4:
            if len(payload) < 17:
                _refuse_payload()
            saw_dht = True
        if marker == 0xDA:
            if not saw_sof or not saw_dht:
                _refuse_payload()
            index += seglen
            entropy_at = index
            saw_sos = True
            while index < len(data) - 1:
                if data[index] == 0xFF:
                    nxt = data[index + 1]
                    if nxt == 0x00 or 0xD0 <= nxt <= 0xD7:
                        index += 2
                        continue
                    break
                index += 1
            if index <= entropy_at:
                _refuse_payload()
            continue
        index += seglen
    _refuse_payload()


def _lsb_bits(data: bytes, start: int, count: int) -> int:
    value = 0
    bit = start
    for shift in range(count):
        byte_at = bit // 8
        if byte_at >= len(data):
            _refuse_payload()
        value |= ((data[byte_at] >> (bit % 8)) & 1) << shift
        bit += 1
    return value


def _admit_vp8(chunk: bytes) -> None:
    if len(chunk) < 10 or chunk[3:6] != b"\x9d\x01\x2a":
        _refuse_payload()
    width = int.from_bytes(chunk[6:8], "little") & 0x3FFF
    height = int.from_bytes(chunk[8:10], "little") & 0x3FFF
    if width < 1 or height < 1 or width > 10000 or height > 10000:
        _refuse_payload()


def _admit_vp8l(chunk: bytes) -> None:
    if len(chunk) < 5 or chunk[0] != 0x2F:
        _refuse_payload()
    width = _lsb_bits(chunk, 8, 14) + 1
    height = _lsb_bits(chunk, 22, 14) + 1
    version = _lsb_bits(chunk, 37, 3)
    if version != 0 or width > 10000 or height > 10000:
        _refuse_payload()


def _admit_webp(data: bytes) -> None:
    if len(data) < 20 or data[:4] != b"RIFF" or data[8:12] != b"WEBP":
        _refuse_payload()
    if int.from_bytes(data[4:8], "little") != len(data) - 8:
        _refuse_payload()
    index = 12
    saw_bitstream = False
    first = True
    while index + 8 <= len(data):
        fourcc = data[index:index + 4]
        size = int.from_bytes(data[index + 4:index + 8], "little")
        index += 8
        if index + size > len(data):
            _refuse_payload()
        chunk = data[index:index + size]
        index += size
        if size % 2:
            if index >= len(data) or data[index] != 0:
                _refuse_payload()
            index += 1
        if fourcc not in _WEBP_CHUNKS:
            _refuse_payload()
        if first:
            if fourcc not in (b"VP8 ", b"VP8L", b"VP8X"):
                _refuse_payload()
            first = False
        if fourcc == b"VP8 ":
            _admit_vp8(chunk)
            saw_bitstream = True
        elif fourcc == b"VP8L":
            _admit_vp8l(chunk)
            saw_bitstream = True
        elif fourcc == b"VP8X" and size < 10:
            _refuse_payload()
    if index != len(data) or first or not saw_bitstream:
        _refuse_payload()


def _iter_boxes(data: bytes):
    index = 0
    while index + 8 <= len(data):
        size = int.from_bytes(data[index:index + 4], "big")
        kind = data[index + 4:index + 8]
        header = 8
        if size == 1:
            if index + 16 > len(data):
                _refuse_payload()
            size = int.from_bytes(data[index + 8:index + 16], "big")
            header = 16
        elif size == 0:
            size = len(data) - index
        if size < header or index + size > len(data):
            _refuse_payload()
        if any(byte < 32 or byte > 126 for byte in kind):
            _refuse_payload()
        yield kind, data[index + header:index + size]
        index += size
    if index != len(data):
        _refuse_payload()


def _admit_mp4(data: bytes) -> None:
    if len(data) < 16:
        _refuse_payload()
    first = True
    saw_mvhd = False
    for kind, payload in _iter_boxes(data):
        if first:
            if kind != b"ftyp" or len(payload) < 8 or payload[:4] not in _MP4_BRANDS:
                _refuse_payload()
            first = False
            continue
        if kind not in _MP4_TOP or kind == b"ftyp":
            _refuse_payload()
        if kind == b"moov":
            for child, child_payload in _iter_boxes(payload):
                if child == b"mvhd" and len(child_payload) >= 24:
                    saw_mvhd = True
    if first or not saw_mvhd:
        _refuse_payload()


def _ebml_id(data: bytes, index: int) -> tuple[bytes, int]:
    if index >= len(data):
        _refuse_payload()
    lead = data[index]
    if lead & 0x80:
        width = 1
    elif lead & 0x40:
        width = 2
    elif lead & 0x20:
        width = 3
    elif lead & 0x10:
        width = 4
    else:
        _refuse_payload()
        raise AssertionError
    if index + width > len(data):
        _refuse_payload()
    return data[index:index + width], index + width


def _ebml_vint(data: bytes, index: int) -> tuple[int, int, bool]:
    if index >= len(data) or data[index] == 0:
        _refuse_payload()
    lead = data[index]
    width = 1
    mask = 0x80
    while width <= 8 and (lead & mask) == 0:
        width += 1
        mask >>= 1
    if width > 8 or index + width > len(data):
        _refuse_payload()
    unknown = width == 1 and lead == 0xFF
    value = lead & (mask - 1)
    for byte in data[index + 1:index + width]:
        value = (value << 8) | byte
    return value, index + width, unknown


def _admit_webm(data: bytes) -> None:
    if not data.startswith(_WEBM_EBML):
        _refuse_payload()
    _eid, index = _ebml_id(data, 0)
    size, index, unknown = _ebml_vint(data, index)
    if unknown or index + size > len(data):
        _refuse_payload()
    header = data[index:index + size]
    end_header = index + size
    cursor = 0
    saw_webm = False
    while cursor < len(header):
        child_id, cursor = _ebml_id(header, cursor)
        child_size, cursor, child_unknown = _ebml_vint(header, cursor)
        if child_unknown or cursor + child_size > len(header):
            _refuse_payload()
        if child_id == _WEBM_DOCTYPE:
            if header[cursor:cursor + child_size] != b"webm":
                _refuse_payload()
            saw_webm = True
        cursor += child_size
    if not saw_webm:
        _refuse_payload()
    index = end_header
    segment_id, index = _ebml_id(data, index)
    if segment_id != _WEBM_SEGMENT:
        _refuse_payload()
    segment_size, index, segment_unknown = _ebml_vint(data, index)
    if segment_unknown or index + segment_size != len(data):
        _refuse_payload()
    segment = data[index:index + segment_size]
    cursor = 0
    saw_info = False
    while cursor < len(segment):
        child_id, cursor = _ebml_id(segment, cursor)
        child_size, cursor, child_unknown = _ebml_vint(segment, cursor)
        if child_unknown or cursor + child_size > len(segment):
            _refuse_payload()
        if child_id == _WEBM_INFO and child_size >= 1:
            saw_info = True
        cursor += child_size
    if not saw_info:
        _refuse_payload()


def admit_payload(content_type: object, payload: object) -> None:
    if not isinstance(content_type, str) or content_type not in _ALLOWED_TYPES:
        raise MediaRejected("media type is not allowed")
    if not isinstance(payload, (bytes, bytearray)):
        raise MediaRejected("media payload is required")
    data = bytes(payload)
    if content_type == "image/png":
        _admit_png(data)
        return
    if content_type == "image/jpeg":
        _admit_jpeg(data)
        return
    if content_type == "image/webp":
        _admit_webp(data)
        return
    if content_type == "video/webm":
        _admit_webm(data)
        return
    if content_type == "video/mp4":
        _admit_mp4(data)
        return
    raise MediaRejected("media type is not allowed")


def comment_markdown(asset_url: object, alt: object) -> str:
    if not isinstance(asset_url, str) or not asset_url.startswith("https://"):
        raise MediaRejected("media asset URL is not allowed")
    if any(char in asset_url for char in (" ", ")", "\n", "\r", "\t")):
        raise MediaRejected("media asset URL is not allowed")
    label = alt if isinstance(alt, str) and _SAFE_ALT.match(alt) else "attachment"
    if _SECRET_NAME.search(label):
        label = "attachment"
    return f"![{label}]({asset_url})"


def _mapping(value: object) -> dict[str, object] | None:
    return value if isinstance(value, dict) else None


class LinearMediaClient:
    def __init__(
        self,
        graphql: Callable[[str, dict[str, object]], dict[str, object]],
        put: Callable[[str, dict[str, str], bytes], None],
    ) -> None:
        self._graphql = graphql
        self._put = put

    def upload(self, filename: str, content_type: str, payload: bytes) -> UploadedAsset:
        if not isinstance(payload, (bytes, bytearray)):
            raise MediaRejected("media payload is required")
        data = bytes(payload)
        spec = admit_media(filename, content_type, len(data))
        admit_payload(spec.content_type, data)
        result = self._graphql(_FILE_UPLOAD, {
            "contentType": spec.content_type,
            "filename": spec.filename,
            "size": spec.size,
            "makePublic": False,
        })
        data_field = _mapping(result.get("data"))
        upload = _mapping(data_field.get("fileUpload") if data_field else None)
        file_info = _mapping(upload.get("uploadFile") if upload else None)
        if upload is None or upload.get("success") is not True or file_info is None:
            raise RuntimeError("Linear file upload was not accepted")
        upload_url = file_info.get("uploadUrl")
        asset_url = file_info.get("assetUrl")
        if not isinstance(upload_url, str) or not upload_url.startswith("https://"):
            raise RuntimeError("Linear file upload was not accepted")
        if not isinstance(asset_url, str) or not asset_url.startswith("https://"):
            raise RuntimeError("Linear file upload was not accepted")
        if (
            file_info.get("contentType") != spec.content_type
            or file_info.get("filename") != spec.filename
            or file_info.get("size") != spec.size
        ):
            raise RuntimeError("Linear file upload was not accepted")
        headers = {"Content-Type": spec.content_type, "Cache-Control": "public, max-age=31536000"}
        raw_headers = file_info.get("headers", [])
        if raw_headers is None:
            raw_headers = []
        if not isinstance(raw_headers, list):
            raise RuntimeError("Linear file upload was not accepted")
        for item in raw_headers:
            row = _mapping(item)
            if row is None:
                raise RuntimeError("Linear file upload was not accepted")
            key = row.get("key")
            value = row.get("value")
            if not isinstance(key, str) or not key or not isinstance(value, str):
                raise RuntimeError("Linear file upload was not accepted")
            if key.lower() == "content-type":
                if value != spec.content_type:
                    raise RuntimeError("Linear file upload was not accepted")
                continue
            headers[key] = value
        self._put(upload_url, headers, data)
        return UploadedAsset(
            asset_url=asset_url,
            filename=spec.filename,
            content_type=spec.content_type,
            size=spec.size,
        )

    def attach_issue(self, issue_id: str, asset: UploadedAsset, *, title: str | None = None) -> None:
        if not issue_id:
            raise ValueError("Linear issue id is required")
        label = title if isinstance(title, str) and title and _SAFE_ALT.match(title) else asset.filename
        if _SECRET_NAME.search(label):
            label = asset.filename
        result = self._graphql(_ATTACHMENT_CREATE, {
            "input": {"issueId": issue_id, "url": asset.asset_url, "title": label},
        })
        data_field = _mapping(result.get("data"))
        created = _mapping(data_field.get("attachmentCreate") if data_field else None)
        if created is None or created.get("success") is not True:
            raise RuntimeError("Linear rejected issue attachment")

    def comment_with_media(
        self,
        issue_id: str,
        body: str,
        asset: UploadedAsset,
        *,
        alt: str | None = None,
    ) -> None:
        if not issue_id:
            raise ValueError("Linear issue id is required")
        if not isinstance(body, str):
            raise MediaRejected("comment body is required")
        embed = comment_markdown(asset.asset_url, alt or asset.filename)
        text = f"{body.rstrip()}\n\n{embed}" if body.strip() else embed
        result = self._graphql(_COMMENT_CREATE, {"input": {"issueId": issue_id, "body": text}})
        data_field = _mapping(result.get("data"))
        created = _mapping(data_field.get("commentCreate") if data_field else None)
        if created is None or created.get("success") is not True:
            raise RuntimeError("Linear rejected issue comment")
