#!/usr/bin/env python3
"""
Raspberry Pi receiver / primary for the ESP32 Wi-Fi collector mesh.

Roles it plays on the Pi's single BLE adapter, concurrently:
  * CENTRAL  - discovers every ESP (by service UUID), connects, subscribes to
               its DATA notifications, and writes UID-tagged commands.
  * BROADCASTER - advertises an "origin" beacon (manufacturer data FF FF E5 32
               02 00) so the ESPs can measure their distance to the Pi. The Pi
               is node index 0 / the coordinate origin.

Line protocol (newline-terminated text), matching the ESP sketch:
  ESP -> Pi : NET  <node> <bssid> <ch> <rssi> <ssid...>
              RSSI <node> <bssid> <rssi> <t_ms> <age_ms>     (or ... NA)
              POS  <node> pi=<m> n=<k> peers=<id:m:k,...>
              ACK  <uid>
  Pi -> ESP : <uid> SUB   <bssid>
              <uid> UNSUB <bssid>

Console commands (type while it runs): help, nets, nodes, sub <bssid>,
  unsub <bssid>, pos, quit.
"""

import asyncio
import time
import uuid

import numpy as np
from bleak import BleakScanner, BleakClient

from dbus_fast.aio import MessageBus
from dbus_fast.service import ServiceInterface, method, dbus_property
from dbus_fast import Variant, BusType, PropertyAccess

# ---- identity (must match the ESP sketch) ----
SERVICE_UUID = "6b1f0001-9c4e-4a3b-8d21-2f7a9c0e1b55"
DATA_CHAR    = "6b1f0002-9c4e-4a3b-8d21-2f7a9c0e1b55"
CMD_CHAR     = "6b1f0003-9c4e-4a3b-8d21-2f7a9c0e1b55"

# origin beacon payload that goes AFTER the 0xFFFF company id: E5 32 (magic) 02 (role=Pi) 00 (id)
ADAPTER        = "hci0"
BEACON_COMPANY = 0xFFFF
BEACON_DATA    = bytes([0xE5, 0x32, 0x02, 0x00])

CMD_RETRIES = 3           # resend a command this many times until ACKed
CMD_TIMEOUT = 1.5         # seconds between resend attempts

# Target (Wi-Fi AP) RSSI -> distance. This is a SEPARATE calibration from the
# ESPs' BLE ranging: it's the Wi-Fi radio measuring the router, not BLE.
TARGET_RSSI_AT_1M  = -40.0
TARGET_PATH_LOSS_N =   3.0


# =========================================================================
#  Per-node state
# =========================================================================
class Node:
    def __init__(self, address):
        self.address = address
        self.id = None                 # learned from the first line it sends
        self.client = None
        self.rxbuf = ""
        self.networks = {}             # bssid -> (ssid, ch, rssi)
        self.stream = {}               # bssid -> (rssi, t_ms, age_ms)
        self.dist_pi = None            # metres to the Pi
        self.peer_dist = {}            # peer_id -> metres
        self.pending = {}              # uid -> (payload, verb, bssid)

nodes = {}        # address -> Node
FRAME = None      # solved node coordinates: {"Pi": xy, esp_id: xy, ...}
track_tasks = {}  # bssid -> asyncio.Task


# =========================================================================
#  Parsing incoming lines
# =========================================================================
def handle_line(node, line):
    if not line:
        return
    parts = line.split()
    tag = parts[0]

    try:
        if tag == "NET" and len(parts) >= 6:
            node.id = int(parts[1])
            bssid = parts[2]
            ssid = " ".join(parts[5:])         # SSID may contain spaces
            node.networks[bssid] = (ssid, parts[3], int(parts[4]))
            print(f"[NET ] esp{node.id}  {bssid}  ch{parts[3]}  {parts[4]} dBm  {ssid!r}")

        elif tag == "RSSI" and len(parts) >= 3:
            node.id = int(parts[1])
            bssid = parts[2]
            if parts[3] == "NA":
                print(f"[RSSI] esp{node.id}  {bssid}  (not seen yet)")
            else:
                rssi, t_ms, age = int(parts[3]), int(parts[4]), int(parts[5])
                node.stream[bssid] = (rssi, t_ms, age)
                print(f"[RSSI] esp{node.id}  {bssid}  {rssi} dBm  age={age}ms")

        elif tag == "POS" and len(parts) >= 2:
            node.id = int(parts[1])
            for tok in parts[2:]:
                if tok.startswith("pi="):
                    node.dist_pi = float(tok[3:])
                elif tok.startswith("peers="):
                    node.peer_dist = {}
                    body = tok[6:]
                    for entry in body.split(","):
                        if not entry:
                            continue
                        pid, dist, _n = entry.split(":")
                        node.peer_dist[int(pid)] = float(dist)
            print(f"[POS ] esp{node.id}  d(Pi)={node.dist_pi} m  peers={node.peer_dist}")

        elif tag == "ACK" and len(parts) >= 2:
            node.pending.pop(parts[1], None)

    except (ValueError, IndexError):
        print(f"[warn] could not parse: {line!r}")


def make_handler(node):
    def handler(_sender, data):
        node.rxbuf += data.decode(errors="ignore")
        while "\n" in node.rxbuf:
            line, node.rxbuf = node.rxbuf.split("\n", 1)
            handle_line(node, line.strip())
    return handler


# =========================================================================
#  Sending UID-tagged commands (with resend-until-ACK)
# =========================================================================
async def send_cmd(node, verb, bssid):
    if not node.client or not node.client.is_connected:
        return
    cmd_uid = uuid.uuid4().hex[:8]        # unique; ESP remembers the last 16
    payload = f"{cmd_uid} {verb} {bssid}"
    node.pending[cmd_uid] = (payload, verb, bssid)
    for _ in range(CMD_RETRIES):
        try:
            await node.client.write_gatt_char(CMD_CHAR, payload.encode(), response=True)
        except Exception as e:
            print(f"[warn] write to esp{node.id} failed: {e}")
            return
        await asyncio.sleep(CMD_TIMEOUT)
        if cmd_uid not in node.pending:   # ACKed -> the ESP acted on it exactly once
            return
    # If it was never ACKed the ESP still dedups on the UID, so no double-action.


async def broadcast_cmd(verb, bssid):
    await asyncio.gather(*(send_cmd(n, verb, bssid)
                           for n in list(nodes.values())
                           if n.client and n.client.is_connected))


# =========================================================================
#  Discovery + connection management
# =========================================================================
async def connect_and_listen(node):
    try:
        async with BleakClient(node.address) as client:
            node.client = client
            await client.start_notify(DATA_CHAR, make_handler(node))
            print(f"[conn] connected {node.address}")
            while client.is_connected:
                await asyncio.sleep(1)
    except Exception as e:
        print(f"[conn] {node.address} dropped: {e}")
    finally:
        node.client = None
        nodes.pop(node.address, None)     # allow rediscovery
        print(f"[conn] released {node.address}")


async def scanner_loop():
    while True:
        try:
            found = await BleakScanner.discover(timeout=4.0, return_adv=True)
        except Exception as e:
            print(f"[scan] error: {e}")
            await asyncio.sleep(2)
            continue
        for address, (dev, adv) in found.items():
            svc = [u.lower() for u in (adv.service_uuids or [])]
            is_ours = SERVICE_UUID.lower() in svc or (dev.name or "").startswith("ESP-")
            if is_ours and address not in nodes:
                n = Node(address)
                nodes[address] = n
                asyncio.create_task(connect_and_listen(n))
        await asyncio.sleep(1)


# =========================================================================
#  Origin beacon (BlueZ LEAdvertisement1 over D-Bus)
# =========================================================================
class OriginBeacon(ServiceInterface):
    def __init__(self, path):
        super().__init__("org.bluez.LEAdvertisement1")
        self.path = path

    @method()
    def Release(self):
        pass

    @dbus_property(access=PropertyAccess.READ)
    def Type(self) -> "s":
        return "broadcast"

    @dbus_property(access=PropertyAccess.READ)
    def ManufacturerData(self) -> "a{qv}":
        return {BEACON_COMPANY: Variant("ay", BEACON_DATA)}

    @dbus_property(access=PropertyAccess.READ)
    def LocalName(self) -> "s":
        return "RPi-Origin"


async def start_beacon():
    bus = await MessageBus(bus_type=BusType.SYSTEM).connect()
    beacon = OriginBeacon("/org/bluez/origin/adv0")
    bus.export(beacon.path, beacon)

    intro = await bus.introspect("org.bluez", f"/org/bluez/{ADAPTER}")
    obj = bus.get_proxy_object("org.bluez", f"/org/bluez/{ADAPTER}", intro)
    mgr = obj.get_interface("org.bluez.LEAdvertisingManager1")
    await mgr.call_register_advertisement(beacon.path, {})
    print("[beacon] origin advertisement registered")
    return bus, mgr, beacon.path


# =========================================================================
#  Relative-position solve (weighted SMACOF, Pi anchored at origin)
# =========================================================================
def _smacof_step(D, W, X):
    m = len(X)
    Dx = np.linalg.norm(X[:, None, :] - X[None, :, :], axis=2)
    Dx[Dx == 0] = 1e-9
    B = -W * D / Dx
    np.fill_diagonal(B, 0)
    np.fill_diagonal(B, -B.sum(axis=1))
    V = -W.copy()
    np.fill_diagonal(V, 0)
    np.fill_diagonal(V, -V.sum(axis=1))
    return np.linalg.pinv(V) @ (B @ X)


def solve_positions():
    """Solve node coordinates (Pi at origin) and freeze them into FRAME."""
    global FRAME
    ids = sorted({n.id for n in nodes.values() if n.id is not None})
    if not ids:
        print("[pos] no node data yet")
        return None
    labels = ["Pi"] + [f"esp{i}" for i in ids]
    slot = {iid: k + 1 for k, iid in enumerate(ids)}     # Pi is slot 0
    m = len(labels)
    D = np.zeros((m, m))
    W = np.zeros((m, m))

    for n in nodes.values():
        if n.id is None:
            continue
        a = slot[n.id]
        if n.dist_pi is not None:
            D[0, a] = D[a, 0] = n.dist_pi
            W[0, a] = W[a, 0] = 1
        for pid, dist in n.peer_dist.items():
            if pid in slot:
                b = slot[pid]
                if W[a, b] > 0:                           # both directions -> average
                    D[a, b] = D[b, a] = (D[a, b] + dist) / 2
                else:
                    D[a, b] = D[b, a] = dist
                    W[a, b] = W[b, a] = 1

    if W.sum() == 0:
        print("[pos] no distances ready yet")
        return None

    X = np.random.rand(m, 2)
    X[0] = [0, 0]
    for _ in range(400):
        X = _smacof_step(D, W, X)
        X = X - X[0]                                      # keep Pi at the origin

    FRAME = {"Pi": X[0]}
    for iid in ids:
        FRAME[iid] = X[slot[iid]]

    print("[pos] relative coordinates (metres, Pi at origin; rotation arbitrary):")
    for lab, (x, y) in zip(labels, X):
        print(f"       {lab:6s}  x={x:7.2f}  y={y:7.2f}")
    return FRAME


# ---- locating a target device from the nodes' streamed RSSI ----
def rssi_to_distance_target(rssi):
    return 10.0 ** ((TARGET_RSSI_AT_1M - rssi) / (10.0 * TARGET_PATH_LOSS_N))


def trilaterate(points, dists):
    """Least-squares position from node positions + measured distances (2D)."""
    pts = np.array(points, dtype=float)
    r = np.array(dists, dtype=float)
    ref = int(np.argmin(r))                 # strongest signal = most reliable anchor
    p0, r0 = pts[ref], r[ref]
    A, b = [], []
    for i in range(len(pts)):
        if i == ref:
            continue
        A.append(2.0 * (pts[i] - p0))
        b.append((r0 ** 2 - r[i] ** 2) + (pts[i] @ pts[i] - p0 @ p0))
    sol, *_ = np.linalg.lstsq(np.array(A), np.array(b), rcond=None)
    return sol


async def track_loop(bssid):
    await broadcast_cmd("SUB", bssid)
    print(f"[track] subscribed all nodes to {bssid}; waiting for RSSI...")
    while True:
        await asyncio.sleep(1.0)
        if FRAME is None:
            print("[track] no node frame yet - run 'pos' first")
            continue
        pts, ds, used = [], [], []
        for n in nodes.values():
            if n.id is None or n.id not in FRAME:
                continue
            rec = n.stream.get(bssid)
            if not rec:
                continue
            pts.append(FRAME[n.id])
            ds.append(rssi_to_distance_target(rec[0]))
            used.append(n.id)
        if len(pts) < 3:
            print(f"[track] {bssid}: {len(pts)} node reading(s) - need >=3 for a 2D fix")
            continue
        est = trilaterate(pts, ds)
        print(f"[track] {bssid}  target ~ x={est[0]:7.2f}  y={est[1]:7.2f} m   (esp{used})")


# =========================================================================
#  Console
# =========================================================================
HELP = ("commands: help | nets | nodes | sub <bssid> | unsub <bssid> | "
        "pos | track <bssid> | untrack <bssid> | quit")

async def console():
    loop = asyncio.get_event_loop()
    print(HELP)
    while True:
        line = (await loop.run_in_executor(None, input)).strip()
        if not line:
            continue
        cmd, *rest = line.split()
        if cmd == "help":
            print(HELP)
        elif cmd == "nets":
            seen = {}
            for n in nodes.values():
                for b, (ssid, ch, rssi) in n.networks.items():
                    seen.setdefault(b, ssid)
            for b, ssid in sorted(seen.items()):
                print(f"  {b}  {ssid!r}")
            print(f"  ({len(seen)} unique BSSIDs)")
        elif cmd == "nodes":
            for n in nodes.values():
                state = "up" if (n.client and n.client.is_connected) else "down"
                print(f"  esp{n.id}  {n.address}  {state}  d(Pi)={n.dist_pi}")
        elif cmd == "sub" and rest:
            await broadcast_cmd("SUB", rest[0])
            print(f"  asked all nodes to stream {rest[0]}")
        elif cmd == "unsub" and rest:
            await broadcast_cmd("UNSUB", rest[0])
            print(f"  asked all nodes to stop {rest[0]}")
        elif cmd == "pos":
            solve_positions()
        elif cmd == "track" and rest:
            bssid = rest[0]
            if bssid in track_tasks:
                print(f"  already tracking {bssid}")
            else:
                if FRAME is None:
                    solve_positions()      # need a node map before we can place the target
                track_tasks[bssid] = asyncio.create_task(track_loop(bssid))
                print(f"  tracking {bssid} (type 'untrack {bssid}' to stop)")
        elif cmd == "untrack" and rest:
            t = track_tasks.pop(rest[0], None)
            if t:
                t.cancel()
            await broadcast_cmd("UNSUB", rest[0])
            print(f"  stopped tracking {rest[0]}")
        elif cmd == "quit":
            raise KeyboardInterrupt
        else:
            print(HELP)


# =========================================================================
#  main
# =========================================================================
async def main():
    bus = mgr = adv_path = None
    try:
        bus, mgr, adv_path = await start_beacon()
    except Exception as e:
        print(f"[beacon] could not register advertisement: {e}")
        print("[beacon] ESPs won't be able to range to the Pi. See notes about running with sudo.")

    tasks = [asyncio.create_task(scanner_loop()), asyncio.create_task(console())]
    try:
        await asyncio.gather(*tasks)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        if mgr and adv_path:
            try:
                await mgr.call_unregister_advertisement(adv_path)
            except Exception:
                pass
        for t in tasks:
            t.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nbye")
