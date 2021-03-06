# -*- coding: utf-8 -*-
"""
Created on Tue May  7 21:56:39 2019

@author: nicemicro
"""

import datetime
import random as rnd
import pickle as pc
import multiprocessing as mp
import pandas as pd
import numpy as np
import numpy.typing as npt
from typing import Union, Optional


def load_partial(
    songlist: pd.DataFrame, artists: pd.DataFrame, albums: pd.DataFrame, filename: str
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Loads the newest scrobbles and adds it to the current songlist.
    """
    new_songs: pd.DataFrame = pd.read_csv(
        filename,
        delimiter=",",
        header=None,
        names=["Artist", "Album", "Title", "Scrobble time"],
        parse_dates=["Scrobble time"],
    )
    new_songs = new_songs[(new_songs["Scrobble time"]).notnull()]
    oldest = new_songs["Scrobble time"].min()
    songlist = (
        pd.concat([new_songs, songlist[(songlist["Scrobble time"] < oldest)]])
        .sort_values(by=["Scrobble time"], ascending=[False])
        .reset_index(drop=True)
    )
    songlist_lowercase = songlist.copy()
    songlist_lowercase["artist_low"] = songlist_lowercase["Artist"].str.lower()
    all_artists = songlist_lowercase.groupby("artist_low").agg({"Artist": "max"})
    all_artists = all_artists.reset_index(drop=False)
    old_artists = artists.copy()
    old_artists["artist_low"] = old_artists["Artist"].str.lower()
    artists = pd.merge(old_artists, all_artists, how="outer", on="artist_low")
    artists.columns = ["Artist2", "artist_low", "Artist"]
    albums = (
        songlist.drop_duplicates(subset=["Artist", "Album"])
        .drop("Title", 1)
        .drop("Scrobble time", 1)
    )
    # TODO
    # albums = (Need to do something to conserve album order)
    return songlist, artists[["Artist"]], albums


def load_songlist(
    filename: str = "songlist.csv",
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Loads all the songs from the properly formatted csv file.
    Returns:
        songlist - all the songs with available scrobble information
        artists - the unique artists
        albums - the unique artist + album combinations
    """
    songlist: pd.DataFrame = pd.read_csv(
        filename, delimiter=",", parse_dates=["Scrobble time"]
    )
    print("List loaded")
    artists = pd.DataFrame(songlist[:]["Artist"].unique())
    artists.columns = ["Artist"]
    print("Artists aquired")
    albums = (
        songlist.drop_duplicates(subset=["Artist", "Album"])
        .drop("Title", 1)
        .drop("Scrobble time", 1)
    )
    print("Albums aquired")
    return songlist, artists, albums


def find_similar(
    songlist: pd.DataFrame,
    artist: str,
    title: str,
    album: Union[str, float] = "",
    timeframe: int = 30 * 60,
    points: Optional[list[int]] = None,
) -> pd.DataFrame:
    """
    Finds the songs in 'songlists' played after the song from 'artist' titled
    'title', for the time 'timeframe' (defaults to 30 minutes). Points is a
    list with the points for the songs right after the searched song. Default
    is [10,5,2,1,1] which means that the song played after searched song gets
    10 points, the 2nd song played after the searched song gets 5, the third
    gets 2, etc.
    """
    if points is None:
        points = [10, 5, 2, 1, 1]
    artist = artist.lower()
    title = title.lower()
    if album == "" or pd.isnull(album):
        selected_songs = songlist[
            (songlist["Artist"].str.lower() == artist)
            & (songlist["Title"].str.lower() == title)
        ]
    else:
        album = str(album)
        album = album.lower()
        selected_songs = songlist[
            (songlist["Artist"].str.lower() == artist)
            & ((songlist["Album"].str.lower() == album)
                | (songlist["Album"] == "")
                | (songlist["Album"].isna()))
            & (songlist["Title"].str.lower() == title)
        ]
    #        if selected_songs.empty:
    #            print("Check album title: ", album)
    #            selected_songs = songlist[(songlist['Artist'].str.lower() == artist) &
    #                                      (songlist['Title'].str.lower() == title)]
    if timeframe > 0:
        result = [
            songlist[
                (songlist["Scrobble time"] > time)
                & (
                    songlist["Scrobble time"]
                    < time + datetime.timedelta(seconds=timeframe)
                )
            ]
            .sort_values(by=["Scrobble time"])
            .reset_index(drop=True)
            for time in selected_songs["Scrobble time"]
        ]
    elif timeframe < 0:
        result = [
            songlist[
                (songlist["Scrobble time"] < time)
                & (
                    songlist["Scrobble time"]
                    > time + datetime.timedelta(seconds=timeframe)
                )
            ]
            .sort_values(by=["Scrobble time"])
            .reset_index(drop=True)
            for time in selected_songs["Scrobble time"]
        ]
    if not result:
        return pd.DataFrame()
    for i in range(len(result)):
        same_song = result[i][
            (result[i]["Artist"].str.lower() == artist)
            & (result[i]["Title"].str.lower() == title)
        ].index.values
        if same_song.size > 0:
            result[i] = result[i][0 : same_song[0]]
        result[i]["Point"] = 0
        el_num = len(result[i].index)
        result[i]["Point"] = [
            points[j] if j < len(points) else 0 for j in range(el_num)
        ]
    result_conc = pd.concat(result, sort=False).reset_index(drop=True)
    result_conc.loc[result_conc[(result_conc["Album"].isnull())].index, "Album"] = ""
    result_sum = (
        result_conc.groupby(["Artist", "Album", "Title"])
        .agg({"Scrobble time": ["max"], "Point": ["sum"]})
        .reset_index()
        .set_axis(
            ["Artist", "Album", "Title", "Played last", "Point"], axis=1, inplace=False
        )
    )
    result_sum = result_sum[(result_sum["Point"] > 0)].sort_values(
        by=["Point", "Played last"], ascending=[False, False]
    )
    return result_sum.reset_index(drop=True)


def find_similar_artist(
    songlist: pd.DataFrame,
    artist: str,
    timeframe: int = 30 * 60,
    points: Optional[list[int]] = None,
) -> pd.DataFrame:
    """
    Finds the artists in 'songlists' played after any song from 'artist'for the
    time 'timeframe' (defaults to 30 minutes). Points is a list with the points
    for the artists right after the searched artist. Default is [10,5,2,1,1]
    which means that the artist played after searched artist gets 10 points,
    the 2nd song played after the current song gets 5, the third gets 2, etc.
    """
    if points is None:
        points = [10, 5, 2, 1, 1]
    artist = artist.lower()
    selected_artist: pd.DataFrame = songlist[(songlist["Artist"].str.lower() == artist)]
    result: pd.DataFrame = [
        songlist[
            (songlist["Scrobble time"] > time)
            & (songlist["Scrobble time"] < time + datetime.timedelta(seconds=timeframe))
        ]
        .sort_values(by=["Scrobble time"])
        .reset_index(drop=True)
        for time in selected_artist["Scrobble time"]
    ]
    for i in range(len(result)):
        same_artist = result[i][
            (result[i]["Artist"].str.lower() == artist)
        ].index.values
        if same_artist.size > 0:
            result[i] = result[i][0 : same_artist[0]]
        result[i]["Point"] = 0
        el_num = len(result[i].index)
        result[i]["Point"] = [
            points[j] if j < len(points) else 0 for j in range(el_num)
        ]
    result_conc: pd.DataFrame = pd.concat(result, sort=False).reset_index(drop=True)
    result_conc.at[result_conc[(result_conc["Album"].isnull())].index, "Album"] = ""
    result_sum = (
        result_conc.groupby(["Artist"])
        .agg({"Scrobble time": ["max"], "Point": ["sum"]})
        .reset_index()
    )
    result_sum = result_sum.set_axis(
        ["Artist", "Played last", "Point"], axis=1, inplace=False
    )
    result_sum = result_sum[(result_sum["Point"] > 0)].sort_values(
        by=["Point", "Played last"], ascending=[False, False]
    )
    return result_sum.reset_index(drop=True)


#%%
def choose_song(
    similars: pd.DataFrame, playlist: pd.DataFrame, **kwargs
) -> tuple[int, int]:
    """Choses a song from the similar list given."""
    same_artist: int = 1
    try_artist: int = 15
    try_old_song: int = 5
    perc_inc: float = 2
    base_percent: float = 30
    for key, value in kwargs.items():
        if key == "same_artist":
            same_artist = max(1, value)
        elif key == "try_artist":
            try_artist = max(1, value)
        elif key == "try_old_song":
            try_old_song = max(1, value)
        elif key == "perc_inc":
            perc_inc = value
        elif key == "base_percent":
            base_percent = min(100, max(1, value))
    if similars.empty:
        return -1, -1
    point_sum: float = similars["Point"].sum()
    trial_num: int = 0
    ok_artist = False  # is there more artists in the last rows
    ok_song = False  # is this the first time this song is on the list
    percent: float = base_percent
    while ((trial_num < try_artist) and (not ok_artist)) or (
        (trial_num < try_artist + try_old_song) and (not ok_song)
    ):
        point_target = rnd.randint(0, int(point_sum * percent / 100))
        song_place = -1
        point = 0
        while point <= point_target:
            song_place += 1
            point += similars["Point"].values[song_place]
        next_artist = similars["Artist"].values[song_place]
        next_title = similars["Title"].values[song_place]
        ok_song = (
            sum(
                (playlist["Artist"].str.lower() == next_artist.lower())
                & (playlist["Title"].str.lower() == next_title.lower())
            )
            == 0
        )
        ok_artist = (
            sum(
                playlist[max(len(playlist) - same_artist, 0) : len(playlist)][
                    "Artist"
                ].str.lower()
                == next_artist.lower()
            )
        ) < same_artist
        trial_num += 1
        percent += perc_inc
    return song_place, trial_num


def generate_list(
    playlist: pd.DataFrame,
    songlist: pd.DataFrame,
    length: int = 5,
    artist: str = "",
    title: str = "",
    album: Union[str, float] = "",
    **kwargs,
) -> pd.DataFrame:
    """
    Continues a playlist supplied by the playlist parameter based on the data
    in songlist argument, adds the number of songs that are defined by length,
    if artist, title and album are provided then that song will be added first,
    if not, then the last song will be used as the basis of starting the
    generation.
    **kwargs that are accepted:
        same_artist: the number of songs by the same artist allowed to follow
            each other. Default value is 1
        try_artist: how many times should we try again if the random result
            gives the same artist but we shouldn't use the same artist anymore.
            Default value is 10
        try_old_song: how many times should we try again if the random result
            gives a song that is already on the list. Default value is 5.
        base_percent: the percentage of points from what the algorith should
            chose from. It is calculated by adding up the points for all songs
            and the random generator only choses the from top songs until the
            sum of the points of the top songs reaches the precentage of the
            sum point of all songs. Default value is 30.
        perc_inc: the increase of the accepted percentage after each retry
            based on either artist or the song already existing on the list
            default value is 3.
        timeframe: see find_similar()
        points: see find_similar()
    """
    base_percent: float = 30
    timeframe: int = 30 * 60
    points: list[int] = [10, 5, 2, 1, 1]
    for key, value in kwargs.items():
        if key == "base_percent":
            base_percent = min(100, max(1, value))
        elif key == "timeframe":
            timeframe = value
        elif key == "points":
            points = value
    if artist != "" and title != "":
        if album == "":
            album = np.NaN
        new_line = pd.DataFrame(
            [[artist, album, title, datetime.datetime.utcnow()]],
            columns=["Artist", "Album", "Title", "Date added"],
        )
        playlist = pd.concat([playlist, new_line], ignore_index=True, sort=False)
    for _ in range(length):
        if not playlist.empty:
            last = len(playlist) - 1
            artist = playlist["Artist"].values[last]
            title = playlist["Title"].values[last]
            if playlist["Album"].isnull().values[last]:
                album = ""
            else:
                album = playlist["Album"].values[last]
        if artist == "" or title == "":
            if artist == "":
                all_songs = (
                    songlist.groupby(["Artist", "Album", "Title"])["Scrobble time"]
                    .count()
                    .reset_index()
                    .sort_values(by=["Scrobble time"], ascending=[False])
                    .reset_index(drop=True)
                )
            else:
                artist_songs = songlist[(songlist["Artist"].str.lower() == artist)]
                all_songs = (
                    artist_songs.groupby(["Artist", "Album", "Title"])["Scrobble time"]
                    .count()
                    .reset_index()
                    .sort_values(by=["Scrobble time"], ascending=[False])
                    .reset_index(drop=True)
                )
            point_sum = all_songs["Scrobble time"].sum()
            point_target = rnd.randint(0, int(point_sum * base_percent / 100))
            song_place = -1
            point = 0
            while point <= point_target:
                song_place += 1
                point += all_songs["Scrobble time"].values[song_place]
            playlist = pd.concat(
                [
                    playlist,
                    pd.DataFrame(
                        [[datetime.datetime.utcnow()]], columns=["Date added"]
                    ).join(
                        all_songs[song_place : song_place + 1][
                            ["Artist", "Album", "Title"]
                        ].reset_index(drop=True)
                    ),
                ],
                sort=False,
            )
        else:
            similars = find_similar(songlist, artist, title, album, timeframe, points)
            if similars.empty:
                print("Expanding the playlist failed after:")
                print("    Artist: ", artist)
                print("    Album: ", album)
                print("    Title: ", title)
                return playlist
            song_place, trial_num = choose_song(similars, playlist, *kwargs)
            playlist = pd.concat(
                [
                    playlist,
                    similars[song_place : song_place + 1][["Artist", "Album", "Title"]]
                    .reset_index(drop=True)
                    .join(
                        pd.DataFrame(
                            [[song_place + 1, trial_num, datetime.datetime.utcnow()]],
                            columns=["Place", "Trial", "Date added"],
                        ),
                        sort=False,
                    ),
                ],
                sort=False,
            ).reset_index(drop=True)
    return playlist


#%%
def latest_songs(
    playlist: pd.DataFrame, timeframe: int = 30 * 60, points: Optional[list[int]] = None
) -> pd.DataFrame:
    """Returns a list of the latest shongs from the playlist."""
    if points is None:
        points = [10, 5, 2, 1, 1]
    points = pd.DataFrame(points, columns=["Multiplier"])
    if len(playlist.index) == 0:
        return pd.DataFrame([])
    time = playlist.at[playlist.index[-1], "Date added"] - datetime.timedelta(
        seconds=timeframe
    )
    listend = playlist[(playlist["Date added"] > time)]
    listend = listend.reset_index().sort_values("index", ascending=False)
    result = pd.concat([listend.reset_index(drop=True), points], axis=1)[
        ["Artist", "Album", "Title", "Multiplier"]
    ]
    return result[(result["Multiplier"] > 0) & (result["Artist"].notna())]


def cumul_similar(
    songlist: pd.DataFrame,
    playlist: pd.DataFrame,
    timeframe: int = 30 * 60,
    points: Optional[npt.NDArray[np.int32]] = None,
    show_all: bool = False
):
    """Calculates the similarity points based on the cumulative points from
    multuiple base songs."""
    if points is None:
        points = np.array([[10, 0, 0], [8, 2, 0], [5, 3, 1]], dtype=np.int32)
    latests = latest_songs(playlist, timeframe, list(points.sum(axis=1)))
    songqueue: mp.Queue = mp.Queue()
    processes: dict[int, mp.Process] = {}
    for song in latests.index:
        processes[song] = mp.Process(
            target=similars_parallel,
            args=(
                songqueue,
                song,
                songlist,
                latests.at[song, "Artist"],
                latests.at[song, "Title"],
                latests.at[song, "Album"],
                timeframe,
                points,
            ),
        )
        processes[song].start()
    collected: list[int] = []
    all_similars: dict[int, pd.DataFrame] = {}
    while len(collected) < len(latests.index):
        msg: dict[str, Union[int, pd.DataFrame]] = songqueue.get()
        similars = msg["res"]
        song = msg["index"]
        assert isinstance(similars, pd.DataFrame)
        assert isinstance(song, int)
        collected.append(song)
        if not similars.index.empty:
            similars[f"P{song}"] = similars["Point"] / similars["Point"].sum() * 100
            similars[f"O{song}"] = similars.index
            all_similars[song] = similars
        else:
            all_similars[song] = pd.DataFrame(
                [["", "", "", 0, 0]], columns=["Artist", "Album", "Title", f"P{song}", f"O{song}"]
            )
    for proc in processes.values():
        proc.join()
    result = all_similars[0][["Artist", "Album", "Title", "P0", "O0"]]
    for song, similars in all_similars.items():
        if song == 0:
            continue
        result = pd.merge(
            result,
            similars[["Artist", "Album", "Title", f"P{song}", f"O{song}"]],
            how="outer",
            on=["Artist", "Album", "Title"],
        )
    for i in latests.index:
        result.loc[result[f"P{i}"].isnull(), f"P{i}"] = 0
    result["Sum"] = 0
    for i in latests.index:
        for j in latests.index:
            if points[i, j] == 0:
                continue
            result[f"PC{i}-{j}"] = (
                np.sqrt(result[f"P{i}"] * result[f"P{j}"]) * points[i, j]
            )
            result["Sum"] = result["Sum"] + result[f"PC{i}-{j}"]
    result = result.sort_values(["Sum"], ascending=False).reset_index(drop=True)
    result["Point"] = result["Sum"] / result["Sum"].sum() * 100
    result = result[(result["Point"] > 0)]
    if not show_all:
        result = result[["Artist", "Album", "Title", "Point"]]
    return result


#%%
def playlist_from_songs(
    songlist: pd.DataFrame, playlist: pd.DataFrame, datetime: pd.Timestamp
) -> pd.DataFrame:
    """
    Gets the songs from the songlist that have been scrobbled after the last
    entry in playlist, and adds it to the end of playlist.
    """
    # playlist2 = e.pd.concat([playlist, songlist[(songlist['Scrobble time'] > e.pd.Timestamp(e.datetime.datetime(year=2019, month=11, day=13, hour=22)))].sort_values(by=['Scrobble time'], ascending=[True]).rename(columns={'Scrobble time': 'Date added'})], sort=False)
    merge = (
        songlist[(songlist["Scrobble time"] > pd.Timestamp(datetime))]
        .sort_values(by=["Scrobble time"], ascending=[True])
        .rename(columns={"Scrobble time": "Date added"})
    )
    return pd.concat([playlist, merge], sort=False).reset_index(drop=True)


def similars_parallel(
    queue: mp.Queue,
    index: int,
    songlist: pd.DataFrame,
    artist: str,
    title: str,
    album: str = "",
    timeframe: int = 30 * 60,
    points: Optional[list[int]] = None,
) -> None:
    """Looks for similar songs using parallel computations"""
    if points is None:
        points = [10, 5, 2, 1, 1]
    result = find_similar(
        songlist, artist, title, album, timeframe, points=[10, 5, 2, 1, 1]
    )
    queue.put({"index": index, "res": result})


def song_relations(
    songlist: pd.DataFrame,
    playlist: pd.DataFrame,
    first: int = 0,
    timeframe: int = 30 * 60,
    points: Optional[list[int]] = None,
):
    """
    Goes through all the entries in playlist where the 'Place' value is NaT and
    tries to figure out what is the position of the song in similar songs for
    the song in the previous entry.
    """
    if points is None:
        points = [10, 5, 2, 1, 1]
    playlist = playlist.copy()
    tocheck = playlist[(playlist["Place"]).isnull()].index
    tocheck = tocheck[(tocheck > first)]
    if len(tocheck) == 0:
        return playlist
    songqueue: mp.Queue = mp.Queue()
    processes = {}
    for song in tocheck:
        processes[song] = mp.Process(
            target=similars_parallel,
            args=(
                songqueue,
                song,
                songlist,
                playlist.at[song - 1, "Artist"],
                playlist.at[song - 1, "Title"],
                playlist.at[song - 1, "Album"],
                timeframe,
                points,
            ),
        )
        processes[song].start()
    collected: list[int] = []
    while True:
        msg: dict[str, Union[int, pd.DataFrame]] = songqueue.get()
        similars = msg["res"]
        song = msg["index"]
        assert isinstance(similars, pd.DataFrame)
        assert isinstance(song, int)
        collected.append(song)
        if not similars.index.empty:
            if pd.isnull(playlist.at[song, "Album"]):
                similars = similars[
                    (
                        similars["Artist"].str.lower()
                        == playlist.at[song, "Artist"].lower()
                    )
                    & (
                        similars["Title"].str.lower()
                        == playlist.at[song, "Title"].lower()
                    )
                ]
            else:
                similars = similars[
                    (
                        similars["Artist"].str.lower()
                        == playlist.at[song, "Artist"].lower()
                    )
                    & (
                        similars["Album"].str.lower()
                        == playlist.at[song, "Album"].lower()
                    )
                    & (
                        similars["Title"].str.lower()
                        == playlist.at[song, "Title"].lower()
                    )
                ]
            if not similars.index.empty:
                playlist.at[song, "Place"] = similars.index[0] + 1
        if len(collected) == len(tocheck):
            break
    for proc in processes:
        processes[proc].join()
    return playlist


def delete_song(playlist: pd.DataFrame, first: int, last: int = 0) -> pd.DataFrame:
    "Deletes a song or a batch of songs based on position."
    first = max(first, 0)
    first = min(first, playlist.index.max() + 1)
    if last == 0:
        last = first
    else:
        last = max(first, last)
    playlist = pd.concat([playlist[0:first], playlist[last + 1 :]])
    if last + 1 in playlist.index:
        playlist.at[last + 1, "Place"] = np.NaN
    return playlist.reset_index(drop=True)


def move_song(playlist: pd.DataFrame, song: int, place: int = 0) -> pd.DataFrame:
    "Moves a songs to an other place."
    song = max(song, 0)
    song = min(song, playlist.index.max())
    place = max(place, -1)
    place = min(place, playlist.index.max())
    playlist.at[song, "Place"] = np.NaN
    if song + 1 in playlist.index:
        playlist.at[song + 1, "Place"] = np.NaN
    if place + 1 in playlist.index:
        playlist.at[place + 1, "Place"] = np.NaN
    if place < song:
        playlist = pd.concat(
            [
                playlist[0 : place + 1],
                playlist[song : song + 1],
                playlist[place + 1 : song],
                playlist[song + 1 :],
            ]
        )
    else:
        playlist = pd.concat(
            [
                playlist[0:song],
                playlist[song + 1 : place + 1],
                playlist[song : song + 1],
                playlist[place + 1 :],
            ]
        )
    return playlist.reset_index(drop=True)


#%%
def find_not_played(
    songlist: pd.DataFrame,
    playlist: pd.DataFrame,
    artist: str = "",
    album: str = "",
    **kwargs,
) -> pd.DataFrame:
    """
    Finds the songs of the artist that aren't in the playlist.

    Optional arguments:
    sort_by: defines how the output should be sorted.
        "plays": it will be ordered based on the how many times the song has
            been played (descending) (default if no sort_by value is defined).
        "rarely": ordered by how many times the song has been playerd
            (ascending)
        "recently p" or "old p": it will be ordered based on the last time the
            song has been played in descending or ascending order respectively.
        "new add": the songs added most recently to the library (based on
            first time played) will appear top.
    min_play: the number how many times a song has to have been played to
        appear
    per_album: whether a song that is on multiple albums should appear as
        separate entries per album (True) or not (False)
    """
    sort_by = "plays"
    min_play = 0
    max_play = 0
    per_album = False
    for key, value in kwargs.items():
        if key == "sort_by":
            sort_by = value.lower()
        if key == "min_play":
            min_play = max(int(value), 0)
        if key == "max_play":
            max_play = max(int(value), 0)
        if key == "per_album":
            per_album = value
    artist = artist.lower()
    album = album.lower()
    songlist = songlist.copy()
    songlist.loc[(songlist["Album"].isnull()), ["Album"]] = ""
    songlist["Artist_low"] = songlist["Artist"].str.lower()
    songlist["Title_low"] = songlist["Title"].str.lower()
    songlist["Album_low"] = songlist["Album"].str.lower()
    if artist == "":
        if per_album:
            artist_songs = songlist.groupby(
                ["Artist_low", "Album_low", "Title_low"]
            ).agg(
                {
                    "Scrobble time": ["count", "max", "min"],
                    "Artist": ["max"],
                    "Album": ["max"],
                    "Title": ["max"],
                }
            )
            artist_songs = artist_songs.reset_index(drop=False).set_axis(
                [
                    "Artist_low",
                    "Album_low",
                    "Title_low",
                    "Played",
                    "Played last",
                    "Added first",
                    "Artist",
                    "Album",
                    "Title",
                ],
                axis=1,
                inplace=False,
            )
        else:
            artist_songs = songlist.groupby(["Artist_low", "Title_low"]).agg(
                {
                    "Scrobble time": ["count", "max", "min"],
                    "Artist": ["max"],
                    "Album": ["max"],
                    "Title": ["max"],
                }
            )
            artist_songs = artist_songs.reset_index(drop=False).set_axis(
                [
                    "Artist_low",
                    "Title_low",
                    "Played",
                    "Played last",
                    "Added first",
                    "Artist",
                    "Album",
                    "Title",
                ],
                axis=1,
                inplace=False,
            )
    else:
        if album == "":
            artist_songs = songlist[(songlist["Artist_low"] == artist)]
            if per_album:
                artist_songs = artist_songs.groupby(["Title_low", "Album_low"]).agg(
                    {
                        "Scrobble time": ["count", "max", "min"],
                        "Artist": ["max"],
                        "Album": ["max"],
                        "Title": ["max"],
                    }
                )
                artist_songs = artist_songs.reset_index(drop=False).set_axis(
                    [
                        "Title_low",
                        "Album_low",
                        "Played",
                        "Played last",
                        "Added first",
                        "Artist",
                        "Album",
                        "Title",
                    ],
                    axis=1,
                    inplace=False,
                )
            else:
                artist_songs = artist_songs.groupby(["Title_low"]).agg(
                    {
                        "Scrobble time": ["count", "max", "min"],
                        "Artist": ["max"],
                        "Album": ["max"],
                        "Title": ["max"],
                    }
                )
                artist_songs = artist_songs.reset_index(drop=False).set_axis(
                    [
                        "Title_low",
                        "Played",
                        "Played last",
                        "Added first",
                        "Artist",
                        "Album",
                        "Title",
                    ],
                    axis=1,
                    inplace=False,
                )
        else:
            artist_songs = (
                songlist[
                    (songlist["Artist_low"] == artist)
                    & (songlist["Album_low"] == album)
                ]
                .groupby(["Title_low"])
                .agg(
                    {
                        "Scrobble time": ["count", "max", "min"],
                        "Artist": ["max"],
                        "Album": ["max"],
                        "Title": ["max"],
                    }
                )
            )
            artist_songs = artist_songs.reset_index(drop=False).set_axis(
                [
                    "Title_low",
                    "Played",
                    "Played last",
                    "Added first",
                    "Artist",
                    "Album",
                    "Title",
                ],
                axis=1,
                inplace=False,
            )
    if artist_songs.empty:
        print("Error")
        return artist_songs
    songs_played = playlist[["Artist", "Title"]].copy()
    songs_played["Check"] = True
    songs_played["Artist_low"] = songs_played["Artist"].str.lower()
    songs_played["Title_low"] = songs_played["Title"].str.lower()
    songs_played = songs_played[["Check", "Artist_low", "Title_low"]]
    artist_songs["Artist_low"] = artist_songs["Artist"].str.lower()
    artist_songs["Title_low"] = artist_songs["Title"].str.lower()
    merged = pd.merge(
        artist_songs, songs_played, how="left", on=["Artist_low", "Title_low"]
    )
    merged = merged[(merged["Check"].isnull()) & (merged["Played"] >= min_play)][
        ["Artist", "Album", "Title", "Played", "Added first", "Played last"]
    ]
    if max_play > 0:
        merged = merged[(merged["Played"] <= max_play)]
    if sort_by == "recent p":
        merged = merged.sort_values(by="Played last", ascending=False)
    elif sort_by == "old p":
        merged = merged.sort_values(by="Played last", ascending=True)
    elif sort_by == "new add":
        merged = merged.sort_values(by="Added first", ascending=False)
    elif sort_by == "rarely":
        merged = merged.sort_values(by=["Played", "Played last"], ascending=True)
    else:
        merged = merged.sort_values(by=["Played", "Played last"], ascending=False)
    return merged.reset_index(drop=True)


#%%
def find_old_song(
    songlist: pd.DataFrame,
    playlist: pd.DataFrame,
    date: pd.Timestamp,
    sort_by_last: bool = False,
) -> pd.DataFrame:
    """
    Finds the songs that haven't been scrobbled since the specified date and
    aren't in the playlist. If sort_by_last is set True, the output is sorted
    to show the songs played the latest. If False, it will be ordered based on
    the number the song has been played.
    """
    old_songs = (
        songlist[["Artist", "Title", "Scrobble time"]]
        .groupby(["Artist", "Title"])
        .agg({"Scrobble time": ["count", "max"]})
        .reset_index(drop=False)
        .set_axis(["Artist", "Title", "Played", "Played last"], axis=1, inplace=False)
    )
    old_songs = old_songs[(old_songs["Played last"] < date)]
    songs_played = playlist[["Artist", "Title"]].copy()
    songs_played["Check"] = True
    songs_played["Artist_low"] = songs_played["Artist"].str.lower()
    songs_played["Title_low"] = songs_played["Title"].str.lower()
    old_songs["Artist_low"] = old_songs["Artist"].str.lower()
    old_songs["Title_low"] = old_songs["Title"].str.lower()
    merged = pd.merge(
        old_songs, songs_played, how="left", on=["Artist_low", "Title_low"]
    )
    merged = merged[(merged["Check"].isnull())][
        ["Artist", "Title", "Played", "Played last"]
    ]
    if sort_by_last:
        merged = merged.sort_values(by="Played last", ascending=False)
    else:
        merged = merged.sort_values(by="Played", ascending=False)
    return merged.reset_index(drop=True)


#%%
def save_data(
    songlist: pd.DataFrame,
    artists: pd.DataFrame,
    albums: pd.DataFrame,
    playlist: pd.DataFrame,
    filename: str = "data",
) -> None:
    """
    Saves the songlist and the playlist in a pickle file.
    """
    with open(filename + ".pckl", "wb") as output_file:
        pc.dump((songlist, artists, albums, playlist), output_file)
    songlist.to_csv(filename + "_songlist.csv", index=False, header=False)
    artists.to_csv(filename + "_artists.csv", index=False, header=False)
    albums.to_csv(filename + "_albums.csv", index=False, header=False)
    playlist.to_csv(filename + "_playlist.csv", index=False, header=False)


def load_data(
    filename: str = "data",
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Loads the songlist and the playlist from a pickle and extracts artists
    and albums.
    """
    with open(filename + ".pckl", "rb") as input_file:
        (songlist, artists, albums, playlist) = pc.load(input_file)
    # artists = pd.DataFrame(songlist[:]['Artist'].unique())
    # artists.columns = ['Artist']
    # albums = songlist.drop_duplicates(subset=['Artist', 'Album']
    #                                  ).drop('Title', 1).drop('Scrobble time',
    #                                                          1)
    return songlist, artists, albums, playlist


def load_csv(
    filename: str = "data",
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Loads the data from CSV files."""
    songlist = pd.read_csv(
        filename + "_songlist.csv",
        sep=",",
        names=["Artist", "Album", "Title", "Scrobble time"],
        parse_dates=["Scrobble time"],
    )
    artists = pd.read_csv(filename + "_artists.csv", sep=",", names=["Artist"])
    albums = pd.read_csv(filename + "_albums.csv", sep=",", names=["Artist", "Albums"])
    playlist = pd.read_csv(
        filename + "_playlist.csv",
        sep=",",
        names=["Artist", "Album", "Title", "Date added", "Place", "Trial"],
        parse_dates=["Date added"],
        dtype={"Place": "Int32", "Trial": "Int32"},
    )
    return songlist, artists, albums, playlist
