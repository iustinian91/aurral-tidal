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

1. Enter a **Client ID** and **Client secret** for a Tidal API client (for example the one tiddl or tidal-dl-ng uses), then save.
2. Press **Test** and sign in to Tidal as prompted.
3. If your client ID logs in through the browser, paste the address of the page you land on into **Login URL**, save, and press **Test** again.

## Settings

| Setting | Values | Default |
|---|---|---|
| Quality | `HI_RES_LOSSLESS`, `LOSSLESS` or `HIGH` | `LOSSLESS` |
| Country code | Two letters, like `US` | Your Tidal account's country |
| Cover size | `80`, `160`, `320`, `640` or `1280` pixels | `1280` |
| Don't embed cover art | on / off | off |
| Don't embed lyrics | on / off | off |

Quality and cover size are typed in, because Aurral settings have no dropdown type yet.

## Notes

- You need an active Tidal subscription. Hi-Res depends on your account and client ID; if Tidal doesn't deliver it, results are capped at `LOSSLESS`.
- The plugin connects to `api.tidal.com`, `auth.tidal.com`, `login.tidal.com` and `resources.tidal.com`.
