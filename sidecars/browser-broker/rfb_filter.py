"""Bounded RFB/WebSocket parser for Observe-mode noVNC connections.

The broker is a WebSocket intermediary, so it must inspect WebSocket messages before
classifying the enclosed RFB client stream.  Observe permits only negotiation and
rendering configuration/update requests; all input and unknown extensions close the
connection rather than being guessed at.
"""
from __future__ import annotations

import os
import struct

_MAX_MESSAGE = 1024 * 1024
_MAX_ENCODINGS = 4096
_MAX_FENCE_PAYLOAD = 64


class ProtocolError(ValueError):
    pass


def client_binary_frame(payload: bytes) -> bytes:
    """Encode a masked client-to-server binary frame for an upstream WebSocket."""
    if len(payload) > _MAX_MESSAGE:
        raise ProtocolError("RFB message exceeds bound")
    size = len(payload)
    if size < 126:
        head = bytes((0x82, 0x80 | size))
    elif size <= 0xffff:
        head = bytes((0x82, 0xfe)) + struct.pack(">H", size)
    else:
        head = bytes((0x82, 0xff)) + struct.pack(">Q", size)
    mask = os.urandom(4)
    return head + mask + bytes(value ^ mask[index % 4] for index, value in enumerate(payload))


class WebSocketClientFrames:
    """Decode one WebSocket direction and yield complete binary messages."""
    def __init__(self, *, masked: bool = True) -> None:
        self._masked = masked
        self._buffer = bytearray()
        self._fragment = bytearray()
        self._fragmented = False

    @property
    def transition_safe(self) -> bool:
        """True only at a complete WebSocket message boundary."""
        return not self._buffer and not self._fragment and not self._fragmented

    def feed(self, data: bytes) -> list[tuple[str, bytes]]:
        self._buffer.extend(data)
        if len(self._buffer) > _MAX_MESSAGE + 14:
            raise ProtocolError("WebSocket frame exceeds bound")
        result: list[tuple[str, bytes]] = []
        while True:
            if len(self._buffer) < 2:
                return result
            first, second = self._buffer[0], self._buffer[1]
            fin, opcode, masked = bool(first & 0x80), first & 0x0f, bool(second & 0x80)
            if first & 0x70 or masked != self._masked:
                raise ProtocolError("invalid WebSocket frame direction")
            length, pos = second & 0x7f, 2
            if length == 126:
                if len(self._buffer) < pos + 2: return result
                length = struct.unpack(">H", self._buffer[pos:pos + 2])[0]; pos += 2
            elif length == 127:
                if len(self._buffer) < pos + 8: return result
                length = struct.unpack(">Q", self._buffer[pos:pos + 8])[0]; pos += 8
                if length >> 63: raise ProtocolError("invalid WebSocket length")
            mask_length = 4 if masked else 0
            if length > _MAX_MESSAGE or len(self._buffer) < pos + mask_length + length:
                if length > _MAX_MESSAGE: raise ProtocolError("WebSocket frame exceeds bound")
                return result
            mask = self._buffer[pos:pos + 4] if masked else b''
            pos += 4 if masked else 0
            end = pos + length
            raw = bytes(self._buffer[:end])
            payload = bytes(value ^ mask[index % 4] for index, value in enumerate(self._buffer[pos:end])) if masked else bytes(self._buffer[pos:end])
            del self._buffer[:end]
            if opcode >= 8:
                if not fin or length > 125: raise ProtocolError("invalid WebSocket control frame")
                if opcode not in (8, 9, 10):
                    raise ProtocolError("reserved WebSocket control frame")
                # Control frames are transport housekeeping, not RFB input. Keep
                # their exact wire representation so the upstream sees a masked
                # client frame and the browser can complete Close/Ping/Pong.
                result.append(("control", raw))
                continue
            if opcode == 2:
                if self._fragmented: raise ProtocolError("new message during fragment")
                self._fragment.extend(payload); self._fragmented = not fin
            elif opcode == 0:
                if not self._fragmented: raise ProtocolError("unexpected continuation")
                self._fragment.extend(payload); self._fragmented = not fin
            else:
                raise ProtocolError("text or reserved WebSocket data is forbidden")
            if len(self._fragment) > _MAX_MESSAGE: raise ProtocolError("WebSocket message exceeds bound")
            if fin:
                result.append(("binary", bytes(self._fragment)))
                self._fragment.clear()


class ObserveRfb:
    """Track a standard RFB 3.8 handshake and admit display-only client records."""
    def __init__(self) -> None:
        self.server_stage = "version"
        self.client_stage = "version"
        self.server = bytearray()
        self.client = bytearray()
        self.security_types = b""
        # A partial raw record already reached the upstream during takeover.  Its
        # suffix must never become an Observe record; the proxy closes that socket.
        self.transition_safe = True
        self._takeover_partial = False

    @property
    def takeover_transition_safe(self) -> bool:
        """A mode switch may only happen between complete RFB records."""
        return (self.transition_safe and not self.client and not self._takeover_partial
                and self.server_stage == "normal" and not self.server)

    def server_bytes(self, data: bytes) -> None:
        self.server.extend(data)
        if len(self.server) > _MAX_MESSAGE: raise ProtocolError("RFB server handshake exceeds bound")
        while True:
            if self.server_stage == "version":
                if len(self.server) < 12: return
                if bytes(self.server[:12]) != b"RFB 003.008\n": raise ProtocolError("unexpected RFB version")
                del self.server[:12]; self.server_stage = "security-types"; self.client_stage = "version"
            elif self.server_stage == "security-types":
                if len(self.server) < 1: return
                count = self.server[0]
                if count == 0: raise ProtocolError("RFB server rejected connection")
                if len(self.server) < 1 + count: return
                self.security_types = bytes(self.server[1:1 + count])
                if 1 not in self.security_types:
                    raise ProtocolError("Observe requires RFB no-auth security")
                del self.server[:1 + count]; self.server_stage = "security-result"; self.client_stage = "security-choice"
            elif self.server_stage == "security-result":
                if len(self.server) < 4: return
                if bytes(self.server[:4]) != b"\0\0\0\0": raise ProtocolError("RFB security failed")
                del self.server[:4]; self.server_stage = "server-init"; self.client_stage = "client-init"
            elif self.server_stage == "server-init":
                if len(self.server) < 24: return
                name_length = struct.unpack(">I", self.server[20:24])[0]
                if name_length > _MAX_MESSAGE or len(self.server) < 24 + name_length: return
                del self.server[:24 + name_length]; self.server_stage = "normal"; self.client_stage = "normal"
            else:
                # framebuffer updates and other server messages do not affect client permissions.
                self.server.clear(); return

    def client_message(self, payload: bytes) -> bytes:
        # During takeover the raw prefix was already sent upstream.  Do not replay
        # it with the suffix after a mode change: that could complete a keyboard or
        # pointer record.  The caller closes this contaminated connection.
        if self.client_stage == "normal" and self._takeover_partial:
            self.client.clear()
            self._takeover_partial = False
            self.transition_safe = False
            return b""
        prospective = bytearray(self.client); prospective.extend(payload)
        if len(prospective) > _MAX_MESSAGE: raise ProtocolError("RFB client stream exceeds bound")
        pos = 0
        while True:
            needed = self._record_length(prospective, pos)
            if needed is None: break
            if len(prospective) - pos < needed: break
            self._validate_record(bytes(prospective[pos:pos + needed]))
            pos += needed
        # Do not release a partial record: a following fragment could turn it into input.
        self.client = prospective[pos:]
        return bytes(prospective[:pos])

    def track_client_bytes(self, payload: bytes) -> None:
        """Track complete raw takeover records so an Observe switch has a boundary.

        Takeover remains byte-transparent: these records are not policy-validated.
        Only an incomplete record is retained, so the first Observe payload can be
        discarded and the contaminated socket closed rather than completing input.
        """
        prospective = bytearray(self.client)
        prospective.extend(payload)
        if len(prospective) > _MAX_MESSAGE:
            raise ProtocolError("RFB client stream exceeds bound")
        pos = 0
        while self.client_stage != "normal":
            needed = self._record_length(prospective, pos)
            if needed is None or len(prospective) - pos < needed:
                self.client = prospective[pos:]
                return
            self._validate_record(bytes(prospective[pos:pos + needed]))
            pos += needed
        while True:
            needed = self._takeover_record_length(prospective, pos)
            if needed is None or len(prospective) - pos < needed:
                self.client = prospective[pos:]
                self._takeover_partial = bool(self.client)
                return
            pos += needed

    def _takeover_record_length(self, data: bytearray, pos: int) -> int | None:
        """Frame raw normal-mode records without changing takeover permissions."""
        available = len(data) - pos
        if available < 1:
            return None
        kind = data[pos]
        if kind == 0:
            return 20
        if kind == 2:
            if available < 4:
                return None
            count = struct.unpack(">H", data[pos + 2:pos + 4])[0]
            if count > _MAX_ENCODINGS:
                raise ProtocolError("too many RFB encodings")
            return 4 + 4 * count
        if kind in (3, 150):
            return 10
        if kind == 248:
            if available < 9:
                return None
            payload_length = data[pos + 8]
            if payload_length > _MAX_FENCE_PAYLOAD:
                raise ProtocolError("RFB fence payload exceeds bound")
            return 9 + payload_length
        if kind == 4:
            return 8
        if kind == 5:
            return 6
        if kind == 6:
            if available < 8:
                return None
            length = struct.unpack(">I", data[pos + 4:pos + 8])[0]
            if length > _MAX_MESSAGE:
                raise ProtocolError("RFB clipboard exceeds bound")
            return 8 + length
        raise ProtocolError("untrackable RFB takeover extension")

    def _record_length(self, data: bytearray, pos: int) -> int | None:
        available = len(data) - pos
        if self.client_stage == "version": return 12 if available >= 12 else None
        if self.client_stage in ("security-choice", "client-init"): return 1 if available >= 1 else None
        if self.client_stage != "normal": raise ProtocolError("client spoke before RFB server handshake")
        if available < 1: return None
        kind = data[pos]
        if kind == 0: return 20 if available >= 20 else None
        if kind == 2:
            if available < 4: return None
            count = struct.unpack(">H", data[pos + 2:pos + 4])[0]
            if count > _MAX_ENCODINGS: raise ProtocolError("too many RFB encodings")
            return 4 + 4 * count if available >= 4 + 4 * count else None
        if kind == 3: return 10 if available >= 10 else None
        if kind == 150: return 10 if available >= 10 else None
        if kind == 248:
            if available < 9: return None
            payload_length = data[pos + 8]
            if payload_length > _MAX_FENCE_PAYLOAD: raise ProtocolError("RFB fence payload exceeds bound")
            return 9 + payload_length if available >= 9 + payload_length else None
        # Known input (4, 5, 6) and every extension fail closed even when fragmented.
        raise ProtocolError("RFB input or unsupported extension in Observe mode")

    def _validate_record(self, record: bytes) -> None:
        if self.client_stage == "version":
            if record != b"RFB 003.008\n": raise ProtocolError("invalid RFB client version")
            self.client_stage = "security-choice"; return
        if self.client_stage == "security-choice":
            if record != b"\x01": raise ProtocolError("Observe requires RFB no-auth security")
            self.client_stage = "client-init"; return
        if self.client_stage == "client-init":
            self.client_stage = "normal"; return
        if record[0] not in (0, 2, 3, 150, 248): raise ProtocolError("RFB input or unsupported extension in Observe mode")
