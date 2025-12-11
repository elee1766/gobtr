# gobtr

ai coded btrfs web gui that makes pretty dashboards and pictures from your filesystem

tbh, i mostly made this to sort of see how good the ai is at writing solidjs. but i thought i may as well do some fun stuff with btrfs so...

### danger

this thing is very ai

its also does a billion ioctl calls. and could maybe overwhelm your disk

there are also probably bugs.

anyways use it if you want to it does look sort of cool

### features

see an overview of your filesystems

a sampling file disk space usage thing that is basically copied from https://github.com/CyberShadow/btdu

see your subvolumes and also visualize btrbk snapshots for subvolumes that you snapshot

see the status of your most recent scrub and balance and also schedule scrub and balances

visualize the layout of your filesystem, both the fragmentation and slack of extents

thanks to github.com/dennwc/btrfs and github.com/ncruces/go-sqlite3 i could keep things cgo free

