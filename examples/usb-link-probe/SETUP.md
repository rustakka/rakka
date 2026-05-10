# Per-platform setup guide

Everything you need on each side of the cable before
`usb-link-probe` will see a device. The README focuses on running
the demo; this file focuses on the host-OS plumbing.

If you've already used a USB serial device on a given OS — `screen
/dev/ttyACM0`, PuTTY against COM3, `screen /dev/cu.usbmodem*` — you
can skip the corresponding section. This guide assumes a fresh
machine where USB serial has never been touched.

## Hardware shapes you can use

The probe is hardware-agnostic — anything that exposes a byte pipe
between the two machines works. The two common shapes:

* **CDC-ACM USB serial.** A device that enumerates as a USB CDC-ACM
  composite device on both sides. Examples: a Raspberry Pi Zero / Pi
  4 / BeagleBone configured in USB-gadget mode, an Arduino-class MCU
  running pass-through firmware, or any custom embedded board with
  CDC firmware. One side appears as `/dev/ttyACM0` (Linux),
  `/dev/cu.usbmodemXXXX` (macOS), or `COMx` (Windows). The other
  side talks back through whatever device path *its* OS chose.
* **Two USB-to-serial dongles + a null-modem cable.** Plug a
  USB-to-serial adapter (FTDI, CH340, CP210x) into each machine, then
  connect them with a null-modem RS-232 cable (TX→RX, RX→TX, GND→GND).
  Slower than CDC-ACM (typically 115200 baud) but works between any
  two non-gadget machines.

If you're choosing fresh, **CDC-ACM is faster and needs no driver on
Linux/macOS, and Windows 10+ has the driver built in.** Reach for
USB-to-serial dongles only when one side can't act as a CDC-ACM
endpoint.

---

## Linux

### Required packages

The runtime needs nothing beyond the Rust toolchain — the kernel's
`cdc_acm` module is loaded automatically when the device appears, and
`usbserial`/`ftdi_sio`/`ch341`/`cp210x` ship in every modern distro.

For builds:

```
# Debian / Ubuntu
sudo apt install -y build-essential pkg-config libudev-dev

# Fedora
sudo dnf install -y gcc pkgconfig systemd-devel
```

`libudev-dev` is what `tokio-serial`'s `available_ports()` uses for
device enumeration — without it, `usb-link-probe list-devices` returns
an empty list even when devices are present.

### User permissions

Serial devices on Linux are owned by `root:dialout` (or `root:uucp`
on some distros). Add yourself to that group once and log out:

```
sudo usermod -aG dialout $USER
# log out and log back in (group membership only takes effect on a fresh login)
groups | tr ' ' '\n' | grep dialout    # confirm
```

Without this, `usb-link-probe listen --device /dev/ttyACM0` errors
with `Permission denied (os error 13)`.

### Finding the device

```
ls /dev/ttyACM* /dev/ttyUSB* /dev/ttyGS* 2>/dev/null
dmesg | tail -20      # check what was just plugged in
```

`ttyACM*` = USB CDC-ACM device, `ttyUSB*` = USB-to-serial dongle,
`ttyGS*` = local USB-gadget endpoint (only present if *this* Linux
box is configured as a gadget).

### USB-gadget setup (only if this Linux box is the gadget side)

If the Linux side is a single-board computer pretending to be a USB
device — i.e. you're plugging the SBC's USB-OTG port into a host PC —
you need ConfigFS gadget configuration. Quickstart for a Raspberry Pi
Zero / Zero 2 W:

```
# /boot/config.txt
dtoverlay=dwc2
# /boot/cmdline.txt — append (single line):
modules-load=dwc2
```

Reboot, then load a CDC-ACM gadget:

```
sudo modprobe libcomposite
sudo /usr/bin/env bash <<'EOF'
cd /sys/kernel/config/usb_gadget
mkdir -p atomr && cd atomr
echo 0x1d6b > idVendor; echo 0x0104 > idProduct
echo 0x0100 > bcdDevice; echo 0x0200 > bcdUSB
mkdir -p strings/0x409
echo "deadbeef" > strings/0x409/serialnumber
echo "atomr"    > strings/0x409/manufacturer
echo "usb-link" > strings/0x409/product
mkdir -p configs/c.1/strings/0x409
echo "ACM" > configs/c.1/strings/0x409/configuration
mkdir -p functions/acm.usb0
ln -s functions/acm.usb0 configs/c.1/
ls /sys/class/udc | head -n1 > UDC
EOF
```

Once attached you'll see `/dev/ttyGS0` on the gadget side and
`/dev/ttyACM0` (or similar) on the host side.

### Stable device names (optional)

`/dev/ttyACM0` can become `/dev/ttyACM1` after a reboot. To pin a
device to a stable name, add a udev rule keyed on the USB
vendor:product ID. Find them with `lsusb`:

```
lsusb
# Bus 001 Device 005: ID 2341:0043 Arduino SA Uno R3
```

Then create `/etc/udev/rules.d/99-atomr-link.rules`:

```
SUBSYSTEM=="tty", ATTRS{idVendor}=="2341", ATTRS{idProduct}=="0043", SYMLINK+="atomr-link"
```

`sudo udevadm control --reload && sudo udevadm trigger`. Now use
`--device /dev/atomr-link`.

### Troubleshooting

| Symptom | Likely cause / fix |
|---|---|
| `list-devices` returns empty | `libudev-dev` missing at build time. Install it and `cargo clean && cargo build -p example-usb-link-probe`. |
| `Permission denied (os error 13)` | Not in `dialout` group, or the system uses `uucp` (Arch). Check `ls -l /dev/ttyACM0` for the owning group. |
| `Device or resource busy (os error 16)` | Something else has the port open. `sudo lsof /dev/ttyACM0` to find the holder. Common culprits: ModemManager, gpsd, screen sessions left running. |
| ModemManager keeps grabbing the device on plug-in | `sudo systemctl mask ModemManager.service` if you don't need it for anything. |
| Device disappears every few seconds and reconnects | USB autosuspend. `echo on | sudo tee /sys/bus/usb/devices/<id>/power/control`. |

---

## macOS

### Required packages

Nothing — Apple ships drivers for CDC-ACM, FTDI, CP210x, and most
mainstream USB-to-serial chipsets. CH340 is the notable exception
(see below).

### Finding the device

```
ls /dev/cu.*
ls /dev/tty.*
```

Use the **`/dev/cu.*` name**, not `/dev/tty.*`. They point at the
same hardware, but `cu.*` doesn't block on DCD assertion; opening
`/dev/tty.usbmodemXXXX` will hang forever waiting for the modem
control lines to come up. This is the most common macOS-specific
gotcha and `tokio-serial` doesn't paper over it.

```
usb-link-probe listen --device /dev/cu.usbmodem14101
```

The exact suffix is generated from the device's USB serial number
and changes per device, not per plug-in event — once you know the
name for *your* dongle, it's stable.

### Apple Silicon

Native `aarch64-apple-darwin` builds work the same as Intel. The
universal2 build target shipped by `cargo build --release` runs on
both. No special flags.

### CH340 / CH341 driver

Apple does *not* bundle the CH340 driver. If your USB-to-serial
dongle uses one (the cheapest dongles do), grab the latest signed
driver from WCH (the chipset vendor) — they distribute a `.pkg`
installer. After install, the device shows up at
`/dev/cu.wchusbserialXXXX`.

### Permissions

macOS doesn't gate serial access by group; any user can open
`/dev/cu.*`. The OS *does* prompt for a USB-device permission grant
the first time a process opens a new device — if `usb-link-probe`
silently exits the very first time you run it, check System Settings
→ *Privacy & Security* and grant the permission, then retry.

### Troubleshooting

| Symptom | Likely cause / fix |
|---|---|
| `usb-link-probe list-devices` returns empty | Device truly not enumerated. `system_profiler SPUSBDataType | grep -A4 'Serial Number'` to check the OS sees the USB device at all. |
| Open hangs forever | You used `/dev/tty.*` instead of `/dev/cu.*`. Switch and retry. |
| `Operation not permitted (os error 1)` | First-run privacy prompt was dismissed. System Settings → Privacy & Security → enable the entry for `usb-link-probe`. |
| Device disappears when laptop sleeps | Expected. macOS power-cycles USB on lid close; the demo's reconnect policy will pick it up automatically when the link comes back. |

---

## Windows

### Drivers

Windows 10 and 11 ship a generic CDC-ACM driver (`usbser.sys`) that
binds automatically to most USB CDC devices. For USB-to-serial
dongles you may need a vendor-specific driver:

| Chipset | Driver source | How to confirm |
|---|---|---|
| FTDI (FT232, FT2232) | Bundled with Windows 10+ | Device Manager → *Ports (COM & LPT)* shows `USB Serial Port (COMx)` |
| CP210x (Silicon Labs) | Windows Update auto-installs; manual fallback at silabs.com | Shows `Silicon Labs CP210x USB to UART Bridge (COMx)` |
| CH340 / CH341 (WCH) | Auto-installs on Win 11; for Win 10 grab from wch.cn | Shows `USB-SERIAL CH340 (COMx)` |
| Microchip MCP2200 / MCP2221 | Microchip site | Shows the chip name + COMx |

If after plugging the device in you see *Other devices → "USB Serial
Device"* with a yellow triangle in Device Manager, the driver isn't
bound — install the chipset driver above.

### Finding the COM port

Three equivalent ways:

```
# cmd
mode

# PowerShell
[System.IO.Ports.SerialPort]::GetPortNames()

# usb-link-probe
usb-link-probe.exe list-devices
```

Or open *Device Manager → Ports (COM & LPT)* — the device shows up
there with its assigned `COMx`. Plug / unplug the cable to identify
which entry is yours: it appears or disappears.

The COM number is **per-device, sticky**: once Windows assigns
`COM3` to a particular dongle's serial number, it'll keep using
`COM3` for that dongle across reboots. A second dongle of the same
chipset gets a different number.

### High-numbered COM ports

If your dongle gets `COM10` or higher, every CLI tool needs the
extended path `\\.\COMxx` instead of `COMxx`. The probe handles both,
but if you ever paste a path into another tool and get "file not
found," that's why.

### Permissions

You do **not** need to run as administrator for normal serial access.
Some corporate-managed Windows installs lock COM port access behind
group policy; if `usb-link-probe.exe connect --device COM3` errors
with `Access is denied. (os error 5)` and no other process is
holding the port, that's almost always group policy or third-party
endpoint security software (CrowdStrike, Carbon Black, etc.).

### PowerShell vs cmd

Both work identically for invoking the binary. The address argument
contains `:` and `@` which neither shell special-cases, so no
escaping is needed:

```
usb-link-probe.exe connect --device COM3 --peer akka.serial://A@/dev/ttyACM0:0
```

If your terminal swallows the `/`, wrap the address in quotes:

```
usb-link-probe.exe connect --device COM3 --peer "akka.serial://A@/dev/ttyACM0:0"
```

### Windows Defender / SmartScreen

The first time you run a freshly-built `usb-link-probe.exe`,
Defender SmartScreen may prompt about an unrecognized publisher.
Click *More info → Run anyway*. There's no code-signing setup for
example binaries.

### Troubleshooting

| Symptom | Likely cause / fix |
|---|---|
| `list-devices` shows the port but `connect` errors `Access is denied. (os error 5)` | Another process owns the port. Close PuTTY / TeraTerm / Arduino IDE serial monitor. Also check Device Manager isn't showing the device with a yellow triangle (driver problem). |
| `list-devices` returns empty | No driver bound. Open Device Manager and look for *Other devices → USB Serial Device* with a warning. Install the chipset driver from the table above. |
| `The system cannot find the file specified. (os error 2)` | COM number ≥ 10 used without the `\\.\COMxx` prefix in another tool. The probe's `--device COM10` form is fine. |
| Device disappears after a few minutes of idle | Selective USB suspend. Power Options → Change plan settings → Change advanced power settings → USB settings → USB selective suspend setting → *Disabled*. |
| Antivirus quarantines the binary on build | Add the workspace `target/` directory to the AV exclusion list. |

---

## Quick verification before running the demo

Once setup is done on both sides, sanity-check the link with a
non-actor tool first to isolate hardware/driver issues:

* **Linux ↔ Linux:** `picocom -b 115200 /dev/ttyACM0` on each side,
  type characters, see them appear on the other.
* **macOS:** `screen /dev/cu.usbmodem14101 115200`. Exit with
  `Ctrl-A k`.
* **Windows:** PuTTY → *Serial* → enter COM port + 115200 baud.

If raw characters cross the link, the hardware is good and the demo
will work. If they don't, fix the hardware/driver issue first — the
probe can't paper over an unconfigured cable.
