# Nice Micro's music-autoplay
MPD-based music player for local media with recommendation engine based on listening habits

It is a work in progress, mostly for my personal use. If you are interested in joining the development, feel free to fork!

If you would like to try this out, have ideas, comments, etc., feel free to open an issue on the issue tracker or find me
on Mastodon. The current link is in my profile.

I have no idea how to prepare Python packages, and I have not taken the time to do any install script. If you can make one,
feel free to contribute, or signal your demand via an issue.

## Installation
Clone the repository. You might want to work in a Python virtual environment. This code is developed on Arch Linux with
the native packages, so on Arch you will not need a virtual environment to run it. You will need to install the
dependencies manually, either from your Linux distribution's package manager, or whatever package manager (pip, conda)
you use in your virtual environment.

### Dependencies
The development / maintenance of the code follwos the Arch Linux package updates, so using the latest stabel version
of the packages below are recommended.

From the standard library: `argparse`, `os`, `tkinter`, `typing`, `datetime`, `xml`, `threading`, `queue`, `time`,
`multiprocessing`, `random`

- `python-mpd2`
- `pandas`
- `numpy`
- `RPi` (for using the GPIO port to handle a simple IR remote, experimental)

### MPD (music player daemon)
Install MPD. Set up MPD so it can find (and play) your music database. The location of the music files does not matter
as long as they can be found by MPD.

### Add your song list database
Currently, the software won't run if a few database files are missing. It has also not been tested with empty database
files in any meaningful way. Find a way to export your listening history in a CSV file named `data_songlist.csv`,
with the following four column names in the header:
```
Artist,Album,Title,Time added
```
This should contain a list of songs played in reverse order, with the `Time added` column being in UTC times. Currently,
this database expects entries for `Artist`, `Album` and `Title` to have commas (`,`) and double quote signs (`"`) removed.
It should work if those are still included in the list, but this is kind of a vestige of how I first exported my play
history of a different software.

You also need to have a CSV file that contains some song statistics. It can be empty, as the software will update it
dynamically from the information in the `data_songlist.csv`. Create a file called `data_songs.csv`, and insert the
following header (no new line after this!):
```
song_id,Artist,Album,Title,Played,Played last,Added first,Group,artist_l,album_l,title_l
```

A third CSV file will contain the recently played songs in order (this is mostly used for debugging purposes, as most
of the information in this file is redundant with `data_songlist.csv`. Create a file named `data_playlist.csv`, and add
the following header (no new line after this!):
```
Artist,Album,Title,Time added,Place,Trial,Last,Method
```
Copy the second line of `data_songlist.csv` to the second line of `data_playlist.csv` and add three commas (`,,,`)

## Run the application
Add executable privileges to the `autoplay.py` and you can start it up using `./autoplay.py` from the command line.
