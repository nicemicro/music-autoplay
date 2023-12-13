#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May  7 21:56:39 2019

@author: nicemicro
"""

import datetime
import multiprocessing as mp
import pickle as pc
import random as rnd
from typing import Optional, Union

import numpy as np
import numpy.typing as npt
import pandas as pd


class Cache():
    def __init__(self):
        self.cache = pd.DataFrame()

    def save_to_cache(
        self,
        artist: str,
        album: str,
        title: str,
        similars_list: pd.DataFrame,
    ) -> None:
        """Gets a cache dataframe and saves the similars list to it."""
        if self.is_in_cache(artist, album, title):
            # TODO: this should delete the current line in the cache and inser the new one
            return
        #similars_list = similars_list[["Artist", "Album", "Title", "Point", "Last"]]
        line = (
            similars_list.sort_values(by=["Artist", "Album", "Title"]).
            set_index(["Artist", "Album", "Title"]).
            transpose()
        )
        index = pd.MultiIndex.from_tuples([
            (artist, album, title, "Point"),
            (artist, album, title, "Last"),
            (artist, album, title, "Place")],
            names=["Artist", "Album", "Title", ""])
        line.index = index
        self.cache = pd.concat([self.cache, line]).sort_index()

    def is_in_cache(
        self,
        artist: str,
        album: str,
        title: str
    ) -> bool:
        """Checks whether a certain song has been saved to the cache."""
        if len(self.cache) == 0:
            return False
        songs = self.cache.reset_index()[["Artist", "Album", "Title"]]
        length = len(
            songs[
                (songs["Artist"] == artist) &
                (songs["Album"] == album) &
                (songs["Title"] == title)
            ]
        )
        return length > 0

    def return_from_cache(self, artist: str, album: str, title: str) -> pd.DataFrame:
        """Returns the list of similar songs from the cache for a specific song."""
        result = (
            self.cache.loc[(artist, album, title), :].
            transpose().
            sort_values(by=["Place"]).
            reset_index()
        )
        return result[(result["Place"].notna())]

def load_partial(
    songlist: pd.DataFrame,
    artists: pd.DataFrame,
    albums: pd.DataFrame,
    filename: str
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Loads the newest scrobbles and adds it to the current songlist.
    """
    new_songs: pd.DataFrame = pd.read_csv(
        filename,
        delimiter=",",
        header=None,
        names=["Artist", "Album", "Title", "Time added"],
        parse_dates=["Time added"],
    )
    new_songs = new_songs[(new_songs["Time added"]).notnull()]
    oldest = new_songs["Time added"].min()
    songlist = (
        pd.concat([new_songs, songlist[(songlist["Time added"] < oldest)]])
        .sort_values(by=["Time added"], ascending=[False])
        .reset_index(drop=True)
    )
    songlist2 = songlist.copy(deep=False)
    songlist2["artist_l"] = songlist2["Artist"].str.lower()
    all_artists = songlist2.groupby("artist_l").agg({"Artist": "max"})
    all_artists = all_artists.reset_index(drop=False)
    old_artists = artists.copy()
    old_artists["artist_l"] = old_artists["Artist"].str.lower()
    artists = pd.merge(old_artists, all_artists, how="outer", on="artist_l")
    artists.columns = ["Artist2", "artist_l", "Artist"]
    albums = (
        songlist2.drop_duplicates(subset=["Artist", "Album"])
        .drop("Title", 1)
        .drop("Time added", 1)
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
        filename, delimiter=",", parse_dates=["Time added"]
    )
    print("List loaded")
    artists = pd.DataFrame(songlist[:]["Artist"].unique())
    artists.columns = ["Artist"]
    print("Artists aquired")
    albums = (
        songlist.drop_duplicates(subset=["Artist", "Album"])
        .drop("Title", 1)
        .drop("Time added", 1)
    )
    print("Albums aquired")
    return songlist, artists, albums

def make_indexlist(songlist: pd.DataFrame, songs: pd.DataFrame) -> pd.DataFrame:
    songlist = songlist.copy(deep=False)
    songlist["artist_l"] = songlist["Artist"].str.lower()
    songlist["title_l"] = songlist["Title"].str.lower()
    songlist["album_l"] = songlist["Album"].fillna("").str.lower()
    indexlist: pd.Dataframe = (
        pd.merge(
            songlist,
            songs,
            on=["artist_l", "album_l", "title_l"],
            how="left"
        )
        .sort_values("Time added", ascending=True)
        .reset_index(drop=True)
        .rename(columns={
            "Artist_x": "Artist",
            "Album_x": "Album",
            "Title_x": "Title"})
    )[["song_id", "Artist", "Album", "Title", "Time added",
       "artist_l", "album_l", "title_l"]]
    # Dealing with the tracks that have no album in the songlist
    no_match = indexlist[(indexlist["song_id"].isna())]
    indexlist = indexlist[(indexlist["song_id"].notna())]
    bestalbum = (
        songs[["song_id", "artist_l", "album_l", "title_l", "Played"]]
        .loc[
            songs.groupby(["artist_l", "title_l"])["Played"]
            .idxmax()
        ]
    )
    no_match = (
        pd.merge(no_match, bestalbum, on=["artist_l", "title_l"], how="left")
        .rename(columns={"song_id_y": "song_id", "album_l_y": "album_l"})
    )[["song_id", "Artist", "Album", "Title", "Time added",
       "artist_l", "album_l", "title_l"]]
    indexlist = (
        pd.concat([indexlist, no_match])
        .sort_values("Time added")[["song_id", "Scrobble time"]]
    )
    indexlist = indexlist.reset_index(drop=True)
    return indexlist


def summarize_similars(
    songlist: Optional[pd.DataFrame] = None,
    songs: Optional[pd.DataFrame] = None,
    indexlist: Optional[pd.DataFrame] = None,
    points: Optional[list[int]] = None,
    timeframe: int = 30,
) -> pd.DataFrame:
    """
    Creates a list for all songs played after each other, scoring and summarizing
    the "closeness" of the two songs.
    """
    if points is None:
        points = [10, 5, 2, 1, 1]
    if songs is None or indexlist is None:
        if songlist is None:
            raise ValueError("Either songlist or songs+indexlist is needed")
        songs = summarize_songlist(songlist)
        indexlist = make_indexlist(songlist, songs)
    result: list[pd.DataFrame] = []
    shifted: pd.DataFrame
    for index in range(len(points)):
        shifted = (pd.concat(
            [
                indexlist,
                (
                    indexlist[index+1:].reset_index(drop=True)
                    .set_axis([f"id_after", f"Time_after"], axis=1)
                )
            ],
            axis=1))
        shifted["Timediff"] = shifted["Time_after"] - shifted["Time added"]
        shifted = (
            shifted[(shifted["Timediff"] < pd.Timedelta(minutes=abs(timeframe)))]
        )
        shifted["Point"] = points[index]
        result.append(shifted[[
            "song_id",
            "Time added",
            "id_after",
            "Timediff",
            "Point"
        ]])
    similarities: pd.DataFrame = pd.concat(result).sort_values(
        ["Time added", "Timediff"], ascending=[False, True]
    )
    return similarities.reset_index(drop=True)


def find_similar(
    artist: str,
    title: str,
    album: str = "",
    songlist: Optional[pd.DataFrame] = None,
    songs: Optional[pd.DataFrame] = None,
    indexlist: Optional[pd.DataFrame] = None,
    similarities: Optional[pd.DataFrame] = None,
    timeframe: int = 30,
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
    if (
        songlist is None and
        (songs is None or indexlist is None) and
        (similarities is None or songs is None)
    ):
        raise ValueError(
            "Songlist or songs + indexlist or similarities + songs is needed."
        )
    if timeframe == 0:
        return pd.DataFrame()
    if points is None:
        points = [10, 5, 2, 1, 1]
    artist = artist.lower()
    title = title.lower()
    if pd.isnull(album):
        album == ""
    album = album.lower()
    if similarities is None:
        if songs is None:
            if songlist is None:
                assert False
            songs = summarize_songlist(songlist)
        if indexlist is None:
            if songlist is None:
                assert False
            indexlist = make_indexlist(songlist, songs)
        similarities = summarize_similars(
            songs=songs,
            indexlist=indexlist,
            points=points,
            timeframe=timeframe
        )
    song_matches: pd.DataFrame = (
        (songs["artist_l"] == artist) & (songs["title_l"] == title)
    )
    if album != "":
        song_matches = (song_matches) & (songs["album_l"] == album)
    songindex: list[int] = list(songs[song_matches].index)
    if len(songindex) == 0:
        return pd.DataFrame()
    if timeframe > 0:
        similarities = similarities[similarities["song_id"].isin(songindex)]
    else:
        similarities = similarities[similarities["id_after"].isin(songindex)]
    similarities_group: pd.Grouper = similarities.groupby(["song_id", "id_after"])
    sum_by_index: pd.DataFrame = (
        similarities_group.agg({"Time added": ["max"], "Point": ["sum"]})
        .reset_index()
        .set_axis(["song_id", "id_after", "Played last", "Point"], axis=1)
        .sort_values(by=["Point", "Played last"], ascending=[False, False])
        .reset_index(drop=True)
    )
    sum_by_index["Last"] = sum_by_index.index + 1
    sum_by_index["Place"] = sum_by_index["Last"]
    result_sum: pd.DataFrame
    if timeframe > 0:
        result_sum = pd.merge(
            sum_by_index, songs, left_on="id_after", right_on="song_id"
        )
    else:
        result_sum = pd.merge(sum_by_index, songs, on="song_id")
    return result_sum[["Artist", "Album", "Title", "Point", "Last", "Place"]]


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
            (songlist["Time added"] > time)
            & (songlist["Time added"] < time + datetime.timedelta(seconds=timeframe))
        ]
        .sort_values(by=["Time added"])
        .reset_index(drop=True)
        for time in selected_artist["Time added"]
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
    result_sum = (
        result_conc.groupby(["Artist"])
        .agg({"Time added": ["max"], "Point": ["sum"]})
        .reset_index()
    )
    result_sum = result_sum.set_axis(
        ["Artist", "Played last", "Point"], axis=1
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
    if not remove_played(similars, playlist).empty:
        similars = remove_played(similars, playlist)
    point_sum: float = similars["Point"].sum()
    trial_num: int = 0
    indices = similars.index
    similars = similars.reset_index(drop=True)
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
    return indices[song_place], trial_num

def generate_list(
    playlist: pd.DataFrame,
    length: int = 5,
    artist: str = "",
    title: str = "",
    album: Union[str, float] = "",
    cumul_find: bool = False,
    cache: Optional[Cache] = None,
    songlist: Optional[pd.DataFrame] = None,
    songs: Optional[pd.DataFrame] = None,
    indexlist: Optional[pd.DataFrame] = None,
    similarities: Optional[pd.DataFrame] = None,
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
    if (
        songlist is None and
        (songs is None or indexlist is None) and
        (similarities is None or songs is None)
    ):
        raise ValueError(
            "Songlist or songs + indexlist or similarities + songs is needed."
        )
    if cache is None:
        cache = Cache()
    base_percent: float = 30
    timeframe: int = 30 * 60
    points: list[int] = [10, 5, 2, 1, 1]
    cumul_points: npt.NDArray[np.int32] = np.array([[10, 0, 0], [10, 5, 0], [5, 2, 1]])
    for key, value in kwargs.items():
        if key == "base_percent":
            base_percent = min(100, max(1, value))
        elif key == "timeframe":
            timeframe = value
        elif key == "points":
            if cumul_find:
                cumul_points = value
            else:
                points = value
    if artist != "" and title != "":
        if album == "":
            album = np.NaN
        new_line = pd.DataFrame(
            [[artist, album, title, datetime.datetime.utcnow()]],
            columns=["Artist", "Album", "Title", "Time added"],
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
        album = str(album)
        assert isinstance(album, str)
        if artist == "" or title == "":
            if artist == "":
                all_songs = (
                    songlist.groupby(["Artist", "Album", "Title"])["Time added"]
                    .count()
                    .reset_index()
                    .sort_values(by=["Time added"], ascending=[False])
                    .reset_index(drop=True)
                )
            else:
                artist_songs = songlist[(songlist["Artist"].str.lower() == artist)]
                all_songs = (
                    artist_songs.groupby(["Artist", "Album", "Title"])[
                        "Time added"
                    ]
                    .count()
                    .reset_index()
                    .sort_values(by=["Time added"], ascending=[False])
                    .reset_index(drop=True)
                )
            point_sum = all_songs["Time added"].sum()
            point_target = rnd.randint(0, int(point_sum * base_percent / 100))
            song_place = -1
            point = 0
            while point <= point_target:
                song_place += 1
                point += all_songs["Time added"].values[song_place]
            playlist = pd.concat(
                [
                    playlist,
                    pd.DataFrame(
                        [[datetime.datetime.utcnow()]], columns=["Time added"]
                    ).join(
                        all_songs[song_place : song_place + 1][
                            ["Artist", "Album", "Title"]
                        ].reset_index(drop=True)
                    ),
                ],
                sort=False,
            )
        else:
            if cumul_find:
                similars = cumul_similar(
                    songlist, playlist, timeframe, cumul_points, cache=cache
                )
            else:
                if cache.is_in_cache(artist, album, title):
                    similars = cache.return_from_cache(artist, album, title)
                else:
                    similars = find_similar(
                        artist, title, album,
                        songlist=songlist,
                        songs=songs,
                        indexlist=indexlist,
                        similarities=similarities,
                        timeframe=timeframe,
                        points=points
                    )
                    if not similars.empty:
                        cache.save_to_cache(artist, album, title, similars)
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
                    similars[song_place : song_place + 1][[
                        "Artist", "Album", "Title", "Place", "Last"
                    ]]
                    .reset_index(drop=True)
                    .join(
                        pd.DataFrame(
                            [[trial_num, datetime.datetime.utcnow()]],
                            columns=["Trial", "Time added"],
                        ),
                        sort=False,
                    ),
                ],
                sort=False,
            ).reset_index(drop=True)
    return playlist[[
        "Artist", "Album", "Title", "Time added", "Place", "Last", "Trial"
    ]]


#%%
def latest_songs(
    playlist: pd.DataFrame,
    timeframe: int = 30 * 60,
    points: Optional[list[int]] = None
) -> pd.DataFrame:
    """Returns a list of the latest songs from the playlist."""
    if points is None:
        points = [10, 5, 2, 1, 1]
    points = pd.DataFrame(points, columns=["Multiplier"])
    if len(playlist.index) == 0:
        return pd.DataFrame([])
    time = playlist.at[playlist.index[-1], "Time added"] - datetime.timedelta(
        seconds=timeframe
    )
    listend = playlist[(playlist["Time added"] > time)]
    listend = listend.reset_index().sort_values("index", ascending=False)
    result = pd.concat([listend.reset_index(drop=True), points], axis=1)[
        ["Artist", "Album", "Title", "Multiplier"]
    ]
    return result[(result["Multiplier"] > 0) & (result["Artist"].notna())]


def cumul_similar(
    playlist: pd.DataFrame,
    timeframe: int = 30 * 60,
    points: Optional[npt.NDArray[np.int32]] = None,
    single_points: Optional[list[int]] = None,
    show_all: bool = False,
    songlist: Optional[pd.DataFrame] = None,
    songs: Optional[pd.DataFrame] = None,
    indexlist: Optional[pd.DataFrame] = None,
    similarities: Optional[pd.DataFrame] = None,
    cache: Optional[Cache] = None
):
    """Calculates the similarity points based on the cumulative points from
    multuiple base songs."""
    if (
        songlist is None and
        (songs is None or indexlist is None) and
        (similarities is None or songs is None)
    ):
        raise ValueError(
            "Songlist or songs + indexlist or similarities + songs is needed."
        )
    if points is None:
        points = np.array([[10, 0, 0], [8, 2, 0], [5, 3, 1]], dtype=np.int32)
    if cache is None:
        cache = Cache()
    if single_points is None:
        single_points = [10, 5, 2, 1, 1]
    latests = latest_songs(playlist, timeframe, list(points.sum(axis=1)))
    songqueue: mp.Queue = mp.Queue()
    processes: dict[int, mp.Process] = {}
    cached_similars: dict[int, pd.DataFrame] = {}
    song: int
    for song in latests.index:
        artist = latests.at[song, "Artist"]
        album = latests.at[song, "Album"]
        if album == np.NaN:
            album = ""
        title = latests.at[song, "Title"]
        if cache.is_in_cache(artist, album, title):
            cached_similars[song] = cache.return_from_cache(artist, album, title)
            continue
        processes[song] = mp.Process(
            target=similars_parallel,
            args=(
                songqueue,
                song,
                artist,
                title,
                album,
                songlist,
                songs,
                indexlist,
                similarities,
                timeframe,
                single_points,
            ),
        )
        processes[song].start()
    all_similars: dict[int, pd.DataFrame] = {}
    collected: list[int] = []
    similars: pd.DataFrame
    while len(collected) < len(latests.index):
        if len(cached_similars) > 0:
            song = list(cached_similars.keys())[0]
            similars = cached_similars[song]
            cached_similars.pop(song)
        else:
            msg: dict[str, Union[int, pd.DataFrame]] = songqueue.get()
            similars = msg["res"]
            song = msg["index"]
        assert isinstance(similars, pd.DataFrame)
        assert isinstance(song, int)
        collected.append(song)
        if not similars.index.empty:
            album = latests.at[song, "Album"]
            if pd.isnull(album):
                album = ""
            cache.save_to_cache(
                latests.at[song, "Artist"],
                album,
                latests.at[song, "Title"],
                similars
            )
            similars[f"P{song}"] = similars["Point"] / similars["Point"].sum() * 100
            similars[f"O{song}"] = similars["Place"]
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
    result["Place"] = result.index + 1
    result["Last"] = result["O0"].astype(pd.Int64Dtype())
    if not show_all:
        result = result[["Artist", "Album", "Title", "Point", "Last", "Place"]]
    return result


#%%
def playlist_from_songs(
    songlist: pd.DataFrame, playlist: pd.DataFrame, datetime: pd.Timestamp
) -> pd.DataFrame:
    """
    Gets the songs from the songlist that have been scrobbled after the last
    entry in playlist, and adds it to the end of playlist.
    """
    merge = (
        songlist[(songlist["Time added"] > pd.Timestamp(datetime))]
        .sort_values(by=["Time added"], ascending=[True])
    )
    return pd.concat([playlist, merge], sort=False).reset_index(drop=True)


def similars_parallel(
    queue: mp.Queue,
    index: int,
    artist: str,
    title: str,
    album: str = "",
    songlist: Optional[pd.DataFrame] = None,
    songs: Optional[pd.DataFrame] = None,
    indexlist: Optional[pd.DataFrame] = None,
    similarities: Optional[pd.DataFrame] = None,
    timeframe: int = 30,
    points: Optional[list[int]] = None,
) -> None:
    """Looks for similar songs using parallel computations"""
    if (
        songlist is None and
        (songs is None or indexlist is None) and
        (similarities is None or songs is None)
    ):
        raise ValueError(
            "Songlist or songs + indexlist or similarities + songs is needed."
        )
    result = find_similar(
        artist,
        title,
        album,
        songlist=songlist,
        songs=songs,
        indexlist=indexlist,
        similarities=similarities,
        timeframe=timeframe,
        points=points
    )
    queue.put({"index": index, "res": result})


def song_relations(
    playlist: pd.DataFrame,
    first: int = 0,
    songlist: Optional[pd.DataFrame] = None,
    songs: Optional[pd.DataFrame] = None,
    indexlist: Optional[pd.DataFrame] = None,
    similarities: Optional[pd.DataFrame] = None,
    timeframe: int = 30 * 60,
    points: Optional[list[int]] = None,
    cache: Optional[Cache] = None,
):
    """
    Goes through all the entries in playlist where the 'Place' value is NaT and
    tries to figure out what is the position of the song in similar songs for
    the song in the previous entry.
    """
    if (
        songlist is None and
        (songs is None or indexlist is None) and
        (similarities is None or songs is None)
    ):
        raise ValueError(
            "Songlist or songs + indexlist or similarities + songs is needed."
        )
    if points is None:
        points = [10, 5, 2, 1, 1]
    if cache is None:
        cache = Cache()
    playlist = playlist.copy()
    tocheck = playlist[(playlist["Last"]).isnull()].index
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
                playlist.at[song - 1, "Artist"],
                playlist.at[song - 1, "Title"],
                playlist.at[song - 1, "Album"],
                songlist,
                songs,
                indexlist,
                similarities,
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
            if similars.index.empty:
                playlist.at[song, "Last"] = 0
            else:
                playlist.at[song, "Last"] = similars["Last"].iloc[0]
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
        playlist.at[last + 1, "Last"] = np.NaN
    return playlist.reset_index(drop=True)


def move_song(playlist: pd.DataFrame, song: int, place: int = 0) -> pd.DataFrame:
    "Moves a songs to an other place."
    song = max(song, 0)
    song = min(song, playlist.index.max())
    place = max(place, -1)
    place = min(place, playlist.index.max())
    playlist.at[song, "Place"] = np.NaN
    playlist.at[song, "Last"] = np.NaN
    if song + 1 in playlist.index:
        playlist.at[song + 1, "Place"] = np.NaN
        playlist.at[song + 1, "Last"] = np.NaN
    if place + 1 in playlist.index:
        playlist.at[place + 1, "Place"] = np.NaN
        playlist.at[place + 1, "Last"] = np.NaN
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

def remove_played(
    list_to_handle: pd.DataFrame,
    playlist: pd.DataFrame,
) -> pd.DataFrame:
    songs_played = playlist[["Artist", "Title"]].copy()
    list_cpy = list_to_handle.reset_index(drop=False)
    songs_played["Check"] = True
    songs_played["artist_l"] = songs_played["Artist"].str.lower()
    songs_played["title_l"] = songs_played["Title"].str.lower()
    songs_played = songs_played[["Check", "artist_l", "title_l"]]
    list_cpy["artist_l"] = list_cpy["Artist"].str.lower()
    list_cpy["title_l"] = list_cpy["Title"].str.lower()
    merged = pd.merge(
        list_cpy, songs_played, how="left", on=["artist_l", "title_l"]
    )
    merged = merged[(merged["Check"].isnull())]
    merged = merged.set_index("index")
    return merged[list_to_handle.columns]


def summarize_songlist(songlist: pd.DataFrame) -> pd.DataFrame:
    result: pd.DataFrame
    songlist = songlist.copy(deep=False)
    songlist["artist_l"] = songlist["Artist"].str.lower()
    songlist["title_l"] = songlist["Title"].str.lower()
    songlist["album_l"] = songlist["Album"].fillna("").str.lower()
    artist_songs = songlist.groupby(
        ["artist_l", "album_l", "title_l"]
    ).agg(
        {
            "Time added": ["count", "max", "min"],
            "Artist": ["max"],
            "Album": ["max"],
            "Title": ["max"],
        }
    )
    songs = artist_songs.reset_index(drop=False).set_axis(
        [
            "artist_l",
            "album_l",
            "title_l",
            "Played",
            "Played last",
            "Added first",
            "Artist",
            "Album",
            "Title",
        ],
        axis=1,
    )
    no_album = songs[(songs["Album"]=="")]
    with_album = songs[(songs["Album"]!="")]
    #TODO try to make this a bit more elegant
    bestalbum = (
        with_album.groupby(["artist_l", "title_l"])
        .agg({"Played": ["max"]})
        .reset_index()
        .set_axis(["artist_l", "title_l", "Played"], axis=1)
    )
    bestalbum = pd.merge(
        with_album, bestalbum, on=["artist_l", "title_l", "Played"], how="right"
    )
    no_album = pd.merge(
        bestalbum, no_album, on=["artist_l", "title_l"], how="right"
    )
    no_album["album_l"] = no_album["album_l_x"].fillna("")
    no_album["Played"] = (
        no_album["Played_x"].fillna(0) +
        no_album["Played_y"].fillna(0)
    ).astype(int)
    no_album["Played last"] = no_album[["Played last_x", "Played last_y"]].max(axis=1)
    no_album["Added first"] = no_album[["Added first_x", "Added first_y"]].min(axis=1)
    no_album = no_album.rename(
        columns={"Artist_y": "Artist", "Album_x": "Album", "Title_y": "Title"}
    )[[
        "Artist",
        "Album",
        "Title",
        "Played last",
        "Added first",
        "Played",
        "artist_l",
        "album_l",
        "title_l"
    ]]
    overwrite = pd.merge(
        with_album, no_album[["artist_l", "album_l", "title_l"]], how="inner"
    )
    overwrite[["Over"]] = True
    overwrite = pd.merge(with_album, overwrite, how="left")
    result = pd.concat(
        [overwrite[(overwrite["Over"].isna())], no_album]
    ).sort_values(["Artist", "Album", "Title"]).reset_index(drop=True)
    result = result.reset_index().rename(columns={"index": "song_id"})
    return result[[
        "song_id", "Artist", "Album", "Title", "Played", "Played last",
        "Added first", "artist_l", "album_l", "title_l"
    ]]

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
    songlist = songlist.copy(deep=False)
    songlist["artist_l"] = songlist["Artist"].str.lower()
    songlist["title_l"] = songlist["Title"].str.lower()
    songlist["album_l"] = songlist["Album"].fillna("").str.lower()
    if artist == "":
        if per_album:
            artist_songs = songlist.groupby(
                ["artist_l", "album_l", "title_l"]
            ).agg(
                {
                    "Time added": ["count", "max", "min"],
                    "Artist": ["max"],
                    "Album": ["max"],
                    "Title": ["max"],
                }
            )
            artist_songs = artist_songs.reset_index(drop=False).set_axis(
                [
                    "artist_l",
                    "album_l",
                    "title_l",
                    "Played",
                    "Played last",
                    "Added first",
                    "Artist",
                    "Album",
                    "Title",
                ],
                axis=1,
            )
        else:
            artist_songs = songlist.groupby(["artist_l", "title_l"]).agg(
                {
                    "Time added": ["count", "max", "min"],
                    "Artist": ["max"],
                    "Album": ["max"],
                    "Title": ["max"],
                }
            )
            artist_songs = artist_songs.reset_index(drop=False).set_axis(
                [
                    "artist_l",
                    "title_l",
                    "Played",
                    "Played last",
                    "Added first",
                    "Artist",
                    "Album",
                    "Title",
                ],
                axis=1,
            )
    else:
        if album == "":
            artist_songs = songlist[(songlist["artist_l"] == artist)]
            if per_album:
                artist_songs = artist_songs.groupby(["title_l", "album_l"]).agg(
                    {
                        "Time added": ["count", "max", "min"],
                        "Artist": ["max"],
                        "Album": ["max"],
                        "Title": ["max"],
                    }
                )
                artist_songs = artist_songs.reset_index(drop=False).set_axis(
                    [
                        "title_l",
                        "album_l",
                        "Played",
                        "Played last",
                        "Added first",
                        "Artist",
                        "Album",
                        "Title",
                    ],
                    axis=1,
                )
            else:
                artist_songs = artist_songs.groupby(["title_l"]).agg(
                    {
                        "Time added": ["count", "max", "min"],
                        "Artist": ["max"],
                        "Album": ["max"],
                        "Title": ["max"],
                    }
                )
                artist_songs = artist_songs.reset_index(drop=False).set_axis(
                    [
                        "title_l",
                        "Played",
                        "Played last",
                        "Added first",
                        "Artist",
                        "Album",
                        "Title",
                    ],
                    axis=1,
                )
        else:
            artist_songs = (
                songlist[
                    (songlist["artist_l"] == artist)
                    & (songlist["album_l"] == album)
                ]
                .groupby(["title_l"])
                .agg(
                    {
                        "Time added": ["count", "max", "min"],
                        "Artist": ["max"],
                        "Album": ["max"],
                        "Title": ["max"],
                    }
                )
            )
            artist_songs = artist_songs.reset_index(drop=False).set_axis(
                [
                    "title_l",
                    "Played",
                    "Played last",
                    "Added first",
                    "Artist",
                    "Album",
                    "Title",
                ],
                axis=1,
            )
    if artist_songs.empty:
        print("Error")
        return artist_songs
    merged = remove_played(artist_songs, playlist)
    merged = merged[(merged["Played"] >= min_play)][
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
        songlist[["Artist", "Title", "Time added"]]
        .groupby(["Artist", "Title"])
        .agg({"Time added": ["count", "max"]})
        .reset_index(drop=False)
        .set_axis(["Artist", "Title", "Played", "Played last"], axis=1)
    )
    old_songs = old_songs[(old_songs["Played last"] < date)]
    merged = remove_played(old_songs, playlist)
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
    songlist["Album"] = songlist["Album"].fillna("")
    playlist["Album"] = playlist["Album"].fillna("")
    # artists = pd.DataFrame(songlist[:]['Artist'].unique())
    # artists.columns = ['Artist']
    # albums = songlist.drop_duplicates(subset=['Artist', 'Album']
    #                                  ).drop('Title', 1).drop('Time added',
    #                                                          1)
    return songlist, artists, albums, playlist


def load_csv(
    filename: str = "data",
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Loads the data from CSV files."""
    songlist = pd.read_csv(
        filename + "_songlist.csv",
        sep=",",
        names=["Artist", "Album", "Title", "Time added"],
        parse_dates=["Time added"],
    )
    artists = pd.read_csv(filename + "_artists.csv", sep=",", names=["Artist"])
    albums = pd.read_csv(filename + "_albums.csv", sep=",", names=["Artist", "Albums"])
    playlist = pd.read_csv(
        filename + "_playlist.csv",
        sep=",",
        names=["Artist", "Album", "Title", "Time added", "Place", "Last", "Trial"],
        parse_dates=["Time added"],
        dtype={
            "Place": pd.Int64Dtype(),
            "Trial": pd.Int64Dtype(),
            "Last": pd.Int64Dtype()
        },
    )
    songlist["Album"] = songlist["Album"].fillna("")
    playlist["Album"] = playlist["Album"].fillna("")
    return songlist, artists, albums, playlist
