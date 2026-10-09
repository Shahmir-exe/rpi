#!/usr/bin/env python3
"""
Raspberry Pi receiver / primary for the ESP32 Wi-Fi collector mesh — GUI version.

A Tkinter window shows:
  * a map/grid with the Pi at the origin and every ESP plotted (once solved),
  * a side list of discovered Wi-Fi devices you can select and track,
  * a live marker + distance rings for the device being tracked.

BLE (bleak) + the origin beacon (dbus-fast) run in a background asyncio thread;
the GUI runs in the main thread and talks to it via run_coroutine_threadsafe.

Line protocol (matches the ESP sketch):
  ESP -> Pi : NET / RSSI / POS / ACK
  Pi -> ESP : <uid> SUB <bssid> | <uid> UNSUB <bssid>
"""

import asyncio
import threading
import uuid
from math import hypot

import numpy as np
import tkinter as tk
from tkinter import ttk

from bleak import BleakScanner, BleakClient
from dbus_fast.aio import MessageBus
from dbus_fast.service import ServiceInterface, method, dbus_property
from dbus_fast import Variant, BusType, PropertyAccess

# ---- identity (must match the ESP sketch) ----
SERVICE_UUID = "6b1f0001-9c4e-4a3b-8d21-2f7a9c0e1b55"
DATA_CHAR    = "6b1f0002-9c4e-4a3b-8d21-2f7a9c0e1b55"
CMD_CHAR     = "6b1f0003-9c4e-4a3b-8d21-2f7a9c0e1b55"

ADAPTER        = "hci0"
BEACON_COMPANY = 0xFFFF
BEACON_DATA    = bytes([0xE5, 0x32, 0x02, 0x00])     # magic + role(Pi) + id(0)

CMD_RETRIES = 3
CMD_TIMEOUT = 1.5

# Target (Wi-Fi AP) RSSI -> distance. SEPARATE calibration from the BLE ranging.
TARGET_RSSI_AT_1M  = -40.0
TARGET_PATH_LOSS_N =   3.0

# ---- shared state (written by async thread, read by GUI thread) ----
nodes = {}          # address -> Node
FRAME = None        # {"Pi": xy, esp_id: xy, ...}
track_tasks = {}    # bssid -> asyncio.Task
TARGET_EST = {}     # bssid -> {"x","y","ok","readings":[(id,dist),...]}
LOOP = None         # the background asyncio event loop
_beacon_ref = None  # keep the beacon's D-Bus objects alive


# =========================================================================
#  Per-node state + parsing
# =========================================================================
class Node:
    def __init__(self, address):
        self.address = address
        self.id = None
        self.client = None
        self.rxbuf = ""
        self.networks = {}     # bssid -> (ssid, ch, rssi)
        self.stream = {}       # bssid -> (rssi, t_ms, age_ms)
        self.dist_pi = None
        self.peer_dist = {}
        self.pending = {}


def handle_line(node, line):
    if not line:
        return
    p = line.split()
    tag = p[0]
    try:
        if tag == "NET" and len(p) >= 6:
            node.id = int(p[1])
            node.networks[p[2]] = (" ".join(p[5:]), p[3], int(p[4]))
        elif tag == "RSSI" and len(p) >= 3:
            node.id = int(p[1])
            if p[3] != "NA":
                node.stream[p[2]] = (int(p[3]), int(p[4]), int(p[5]))
        elif tag == "POS" and len(p) >= 2:
            node.id = int(p[1])
            for tok in p[2:]:
                if tok.startswith("pi="):
                    node.dist_pi = float(tok[3:])
                elif tok.startswith("peers="):
                    node.peer_dist = {}
                    for entry in tok[6:].split(","):
                        if entry:
                            pid, dist, _n = entry.split(":")
                            node.peer_dist[int(pid)] = float(dist)
        elif tag == "ACK" and len(p) >= 2:
            node.pending.pop(p[1], None)
    except (ValueError, IndexError):
        pass


def make_handler(node):
    def handler(_sender, data):
        node.rxbuf += data.decode(errors="ignore")
        while "\n" in node.rxbuf:
            line, node.rxbuf = node.rxbuf.split("\n", 1)
            handle_line(node, line.strip())
    return handler


# =========================================================================
#  Commands
# =========================================================================
async def send_cmd(node, verb, bssid):
    if not node.client or not node.client.is_connected:
        return
    cmd_uid = uuid.uuid4().hex[:8]
    payload = f"{cmd_uid} {verb} {bssid}"
    node.pending[cmd_uid] = payload
    for _ in range(CMD_RETRIES):
        try:
            await node.client.write_gatt_char(CMD_CHAR, payload.encode(), response=True)
        except Exception:
            return
        await asyncio.sleep(CMD_TIMEOUT)
        if cmd_uid not in node.pending:
            return


async def broadcast_cmd(verb, bssid):
    await asyncio.gather(*(send_cmd(n, verb, bssid)
                           for n in list(nodes.values())
                           if n.client and n.client.is_connected))


async def start_track(bssid):
    if bssid in track_tasks:
        return
    track_tasks[bssid] = asyncio.create_task(track_loop(bssid))


async def stop_track(bssid):
    t = track_tasks.pop(bssid, None)
    if t:
        t.cancel()
    await broadcast_cmd("UNSUB", bssid)
    TARGET_EST.pop(bssid, None)


# =========================================================================
#  Discovery + connection
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
        nodes.pop(node.address, None)


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
            if (SERVICE_UUID.lower() in svc or (dev.name or "").startswith("ESP-")) \
                    and address not in nodes:
                n = Node(address)
                nodes[address] = n
                asyncio.create_task(connect_and_listen(n))
        await asyncio.sleep(1)


# =========================================================================
#  Origin beacon
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


async def beacon_task():
    global _beacon_ref
    try:
        bus = await MessageBus(bus_type=BusType.SYSTEM).connect()
        beacon = OriginBeacon("/org/bluez/origin/adv0")
        bus.export(beacon.path, beacon)
        intro = await bus.introspect("org.bluez", f"/org/bluez/{ADAPTER}")
        obj = bus.get_proxy_object("org.bluez", f"/org/bluez/{ADAPTER}", intro)
        mgr = obj.get_interface("org.bluez.LEAdvertisingManager1")
        await mgr.call_register_advertisement(beacon.path, {})
        _beacon_ref = (bus, mgr, beacon.path)
        print("[beacon] origin advertisement registered")
    except Exception as e:
        print(f"[beacon] could not register advertisement: {e}")
        print("[beacon] ESPs can't range to the Pi. Try running with sudo.")


# =========================================================================
#  Position solve + target trilateration
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
    global FRAME
    ids = sorted({n.id for n in nodes.values() if n.id is not None})
    if not ids:
        return None
    slot = {iid: k + 1 for k, iid in enumerate(ids)}
    m = len(ids) + 1
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
                if W[a, b] > 0:
                    D[a, b] = D[b, a] = (D[a, b] + dist) / 2
                else:
                    D[a, b] = D[b, a] = dist
                    W[a, b] = W[b, a] = 1
    if W.sum() == 0:
        return None
    X = np.random.rand(m, 2)
    X[0] = [0, 0]
    for _ in range(400):
        X = _smacof_step(D, W, X)
        X = X - X[0]
    FRAME = {"Pi": X[0]}
    for iid in ids:
        FRAME[iid] = X[slot[iid]]
    return FRAME


def rssi_to_distance_target(rssi):
    return 10.0 ** ((TARGET_RSSI_AT_1M - rssi) / (10.0 * TARGET_PATH_LOSS_N))


def trilaterate(points, dists):
    pts = np.array(points, dtype=float)
    r = np.array(dists, dtype=float)
    ref = int(np.argmin(r))
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
    while True:
        await asyncio.sleep(1.0)
        if FRAME is None:
            continue
        pts, ds, readings = [], [], []
        for n in nodes.values():
            if n.id is None or n.id not in FRAME:
                continue
            rec = n.stream.get(bssid)
            if not rec:
                continue
            dist = rssi_to_distance_target(rec[0])
            pts.append(FRAME[n.id])
            ds.append(dist)
            readings.append((n.id, dist))
        if len(pts) < 3:
            TARGET_EST[bssid] = {"x": None, "y": None, "ok": False, "readings": readings}
            continue
        est = trilaterate(pts, ds)
        TARGET_EST[bssid] = {"x": float(est[0]), "y": float(est[1]),
                             "ok": True, "readings": readings}


# =========================================================================
#  Background asyncio thread
# =========================================================================
def start_async_thread():
    def runner():
        global LOOP
        LOOP = asyncio.new_event_loop()
        asyncio.set_event_loop(LOOP)
        LOOP.create_task(scanner_loop())
        LOOP.create_task(beacon_task())
        LOOP.run_forever()
    threading.Thread(target=runner, daemon=True).start()


# =========================================================================
#  GUI
# =========================================================================
class App:
    def __init__(self, root):
        self.root = root
        root.title("ESP Wi-Fi locator")
        root.geometry("1000x680")

        left = ttk.Frame(root, padding=6)
        left.pack(side=tk.LEFT, fill=tk.Y)
        ttk.Label(left, text="Discovered devices").pack(anchor="w")
        self.listbox = tk.Listbox(left, width=44, height=28, activestyle="dotbox")
        self.listbox.pack(fill=tk.Y, expand=True)
        self.bssids = []        # parallel to listbox rows

        btns = ttk.Frame(left)
        btns.pack(fill=tk.X, pady=4)
        ttk.Button(btns, text="Track", command=self.on_track).pack(side=tk.LEFT, expand=True, fill=tk.X)
        ttk.Button(btns, text="Stop", command=self.on_stop).pack(side=tk.LEFT, expand=True, fill=tk.X)
        ttk.Button(left, text="Solve node positions", command=self.on_solve).pack(fill=tk.X)
        self.status = ttk.Label(left, text="", wraplength=300, justify="left", foreground="#444")
        self.status.pack(anchor="w", pady=6)

        self.canvas = tk.Canvas(root, bg="#0d0d10", highlightthickness=0)
        self.canvas.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True)

        self.tracking = None
        self.refresh()

    # ---- actions ----
    def on_solve(self):
        solve_positions()

    def on_track(self):
        sel = self.listbox.curselection()
        if not sel:
            return
        bssid = self.bssids[sel[0]]
        if FRAME is None:
            solve_positions()
        self.tracking = bssid
        if LOOP:
            asyncio.run_coroutine_threadsafe(start_track(bssid), LOOP)

    def on_stop(self):
        if self.tracking and LOOP:
            asyncio.run_coroutine_threadsafe(stop_track(self.tracking), LOOP)
        self.tracking = None

    # ---- live update ----
    def refresh(self):
        self.update_list()
        self.draw()
        self.root.after(400, self.refresh)

    def update_list(self):
        seen = {}
        for n in nodes.values():
            for b, (ssid, ch, rssi) in n.networks.items():
                seen.setdefault(b, ssid)
        new = sorted(seen.items(), key=lambda kv: (kv[1] or "~", kv[0]))
        new_bssids = [b for b, _ in new]
        if new_bssids == self.bssids:
            return                                  # nothing changed
        keep = self.bssids[self.listbox.curselection()[0]] if self.listbox.curselection() else None
        self.listbox.delete(0, tk.END)
        self.bssids = new_bssids
        for b, ssid in new:
            label = f"{ssid or '<hidden>'}   {b}"
            self.listbox.insert(tk.END, label)
        if keep in self.bssids:
            i = self.bssids.index(keep)
            self.listbox.selection_set(i)

    def world_to_screen(self, x, y, cx, cy, scale):
        return cx + x * scale, cy - y * scale

    def draw(self):
        c = self.canvas
        c.delete("all")
        w, h = c.winfo_width(), c.winfo_height()
        if w < 50 or h < 50:
            return
        cx, cy = w / 2, h / 2

        # collect everything that needs to fit on screen
        pts = [(0.0, 0.0)]
        if FRAME:
            for k, v in FRAME.items():
                pts.append((float(v[0]), float(v[1])))
        est = TARGET_EST.get(self.tracking) if self.tracking else None
        if est and est["ok"]:
            pts.append((est["x"], est["y"]))
        extent = max((hypot(x, y) for x, y in pts), default=2.0)
        extent = max(extent, 2.0)
        scale = (min(w, h) / 2 - 50) / extent

        # grid every 1 m
        step_px = scale
        gx = cx % step_px
        while gx < w:
            c.create_line(gx, 0, gx, h, fill="#1c1c22")
            gx += step_px
        gy = cy % step_px
        while gy < h:
            c.create_line(0, gy, w, gy, fill="#1c1c22")
            gy += step_px
        # axes
        c.create_line(0, cy, w, cy, fill="#33333c")
        c.create_line(cx, 0, cx, h, fill="#33333c")

        # distance rings + ESP markers come from the live data
        if est:
            for nid, dist in est["readings"]:
                if FRAME and nid in FRAME:
                    px, py = self.world_to_screen(FRAME[nid][0], FRAME[nid][1], cx, cy, scale)
                    rad = dist * scale
                    c.create_oval(px - rad, py - rad, px + rad, py + rad,
                                  outline="#8a5a2b", dash=(3, 3))

        # Pi at origin
        self._marker(cx, cy, "#4ec36b", "Pi")

        # ESP nodes
        if FRAME:
            for k, v in FRAME.items():
                if k == "Pi":
                    continue
                px, py = self.world_to_screen(v[0], v[1], cx, cy, scale)
                self._marker(px, py, "#45b7d6", f"esp{k}")

        # target
        if est and est["ok"]:
            tx, ty = self.world_to_screen(est["x"], est["y"], cx, cy, scale)
            c.create_line(tx - 10, ty, tx + 10, ty, fill="#e8603c", width=2)
            c.create_line(tx, ty - 10, tx, ty + 10, fill="#e8603c", width=2)
            c.create_text(tx + 12, ty - 10, text="target", fill="#e8603c", anchor="w")

        self._update_status(est)

    def _marker(self, x, y, color, label):
        r = 7
        self.canvas.create_oval(x - r, y - r, x + r, y + r, fill=color, outline="")
        self.canvas.create_text(x, y - 14, text=label, fill=color)

    def _update_status(self, est):
        up = sum(1 for n in nodes.values() if n.client and n.client.is_connected)
        lines = [f"nodes connected: {up}",
                 f"positions solved: {'yes' if FRAME else 'no'}"]
        if self.tracking:
            lines.append(f"tracking: {self.tracking}")
            if est and est["ok"]:
                lines.append(f"target: x={est['x']:.2f}  y={est['y']:.2f} m")
            elif est:
                lines.append(f"target: {len(est['readings'])} reading(s) - need >=3")
            else:
                lines.append("target: waiting for RSSI...")
        self.status.config(text="\n".join(lines))


def main():
    start_async_thread()
    root = tk.Tk()
    App(root)
    root.mainloop()


if __name__ == "__main__":
    main()
