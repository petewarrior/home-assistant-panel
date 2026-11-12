
### `Home Automation Demo`

A fictional Home Automation User Interface.

| `.slint` Design | Rust Source | C++ Source | Node Source | Online wasm Preview | Open in SlintPad |
| --- | --- | --- | --- | --- | --- |
| [`ui.slint`](./ui/demo.slint) | [`main.rs`](./rust/main.rs) |  | [`main.js`](./node/main.js) | [Online simulation](https://slint.dev/snapshots/master/demos/home-automation/) | [Preview in Online Code Editor](https://slint.dev/snapshots/master/editor?load_url=https://raw.githubusercontent.com/slint-ui/slint/master/demos/home-automation/ui/demo.slint) |

![Screenshot of the Home Automation Demo](https://github.com/user-attachments/assets/607e07a5-2e79-4045-9fe4-3da2493ba187 "Home Automation Demo")

## Home Assistant WebSocket Integration

Set `HA_BASE_URL` and `HA_TOKEN` in `rust/.env` to connect a native build to Home Assistant.
Use the base URL, such as `http://homeassistant.local:8123`, and a Home Assistant long-lived access token.
Keep `rust/.env` out of version control.

Home Assistant areas appear as pages in the existing room navigation.
Devices without an assigned area appear on the Unassigned page.
The pages use the demo's Lamp component for lights.
Tap a lamp image to toggle it, or use its slider to change brightness.
Tap a supported entity to request an on/off state change.
Browser builds don't connect to Home Assistant.

## Building and running on iOS

This demo can be cross-compiled to iOS to run on iPhones, iPads, and the respective simulators.

### Prerequisites

 - A computer running macOS.
 - An up-to-date installation of [Xcode](https://developer.apple.com/xcode/).
 - [Xcodegen](https://github.com/yonaskolb/XcodeGen?tab=readme-ov-file#installing)
 - [Rust](https://rustup.rs). Add the target and simulator toolchains using `rustup target add aarch64-apple-ios` and `rustup target add aarch64-apple-ios-sim`

### Building

1. Run `xcodegen -s ios-project.yml` to generate an XCode project file (`.xcodeproj`).
2. Open XCode and open the generated `.xcodeproj` in it.
3. Run, deploy, and debug the demo from within Xcode.
