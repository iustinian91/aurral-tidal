# Aurral Tidal plugin

A download source plugin for [Aurral](https://docs.aurral.org) that fetches tracks and albums from your own Tidal account. Files are tagged and ready for your library, with cover art and lyrics embedded.

> Aurral plugins are in beta, so the plugin API can change between Aurral releases.

## Features

- Hi-Res FLAC (up to 24-bit/192 kHz), FLAC (16-bit/44.1 kHz) or AAC 320
- Track and album downloads, plus upgrades to a better quality
- Tags, cover art and lyrics written into each file
- Falls back to the best quality a track has when it lacks the one you chose
- Caches repeated requests and downloads stream segments in parallel

## Install

In Aurral, open **Settings > Plugins** and paste this repo's link:

```
https://github.com/iustinian91/aurral-tidal
```

Or copy `index.js` and `manifest.json` into a `tidal` folder inside the `plugins` folder of your Aurral data folder.

## Set up

1. Press **Test**, open the link it shows and approve the login.
2. Press **Test** again to finish connecting.

The plugin logs in with a built-in Tidal client (the same one tiddl uses), so you don't need a client ID. The built-in client tops out at `LOSSLESS`: for Hi-Res, enter your own **Client ID** (a **Client secret** is only needed if your client has one) before pressing **Test**. If that client ID logs in through the browser, paste the address of the page you land on into **Login URL**, save, and press **Test** again.

## Settings

| Setting | Values | Default |
|---|---|---|
| Follow Aurral's quality profile | on / off. On: the plugin asks Tidal for the qualities your Aurral quality profile enables, in its order, and judges upgrades the way Aurral does | off |
| Quality | `HI_RES_LOSSLESS`, `LOSSLESS` or `HIGH`. With the profile setting on, used only until Aurral's profile has been read once (after that, a failed read keeps the last profile) | `LOSSLESS` |
| Country code | Two letters, like `US` | Your Tidal account's country |
| Cover size | `80`, `160`, `320`, `640` or `1280` pixels | `1280` |
| Don't embed cover art | on / off | off |
| Don't embed lyrics | on / off | off |

Quality and cover size are typed in, because Aurral settings have no dropdown type yet.

## Notes

- You need an active Tidal subscription. Hi-Res depends on your account and client ID (the built-in client doesn't get it); if Tidal doesn't deliver it, results are capped at `LOSSLESS`.
- Tidal can revoke the built-in client at any time. If logins start failing, enter your own **Client ID**.
- The plugin connects to `api.tidal.com`, `auth.tidal.com`, `login.tidal.com` and `resources.tidal.com`.
- The plugin asks for API access, which gives it an admin key to Aurral. It uses the key only to read your quality profile from Aurral's settings (when **Follow Aurral's quality profile** is on). That answer also holds other services' keys and passwords; the plugin keeps only the quality profile. **Test** shows which quality source is in use.
