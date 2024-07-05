#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May  7 21:56:39 2019

@author: nicemicro
"""

import datetime
import multiprocessing as mp
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
        similars_list = similars_list.copy()[[
            "Artist", "Album", "Title", "Point", "Place"
        ]]
        line = (
            similars_list.sort_values(by=["Artist", "Album", "Title"]).
            set_index(["Artist", "Album", "Title"]).
            transpose()
        )
        index = pd.MultiIndex.from_tuples([
            (artist, album, title, "Point"),
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


def make_indexlist(songlist: pd.DataFrame, songs: pd.DataFrame) -> pd.DataFrame:
    songlist = songlist.copy(deep=False)
    songs = songs.copy(deep=False).reset_index(drop=False)
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
        .sort_values("Time added")[["song_id", "Time added"]]
    )
    indexlist = indexlist.reset_index(drop=True)
    return indexlist


def summarize_similars(
    songs: pd.DataFrame,
    songlist: Optional[pd.DataFrame] = None,
    indexlist: Optional[pd.DataFrame] = None,
    points: Optional[list[int]] = None,
    timeframe: int = 30,
) -> pd.DataFrame:
    """
    Creates a list for all songs played after each other, scoring and summarizing
    the "closeness" of the two songs.
    """
    if songs is None or (songlist is None and indexlist is None):
        raise ValueError("Either songlist or indexlist is needed")
    if points is None:
        points = [10, 5, 2, 1, 1]
    if indexlist is None:
        assert songlist is not None
        indexlist = make_indexlist(songlist, songs)
    result: list[pd.DataFrame] = []
    shifted: pd.DataFrame
    for index in range(len(points)):
        shifted = (pd.concat(
            [
                indexlist,
                (
                    indexlist[index+1:].reset_index(drop=True)
                    .set_axis(["id_after", "Time_after"], axis=1)
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


def get_song_id(
    songs: pd.DataFrame,
    artist: str,
    title: str = "",
    album: str = "",
    group: int = -1
) -> list[int]:
    artist = artist.lower().replace(",", "").replace("\"", "")
    album = album.lower().replace(",", "").replace("\"", "")
    title = title.lower().replace(",", "").replace("\"", "")
    songs = songs.copy().sort_values("Played", ascending=False)
    song_matches: pd.DataFrame = (
        (songs["artist_l"] == artist)
    )
    if title != "":
        song_matches = (song_matches) & (songs["title_l"] == title)
    if album != "":
        song_matches = (song_matches) & (songs["album_l"] == album)
    if group != -1 and group % 100 == 0:
        song_matches = (song_matches) & (songs["Group"] // 100 == group // 100)
    if group != -1 and group % 100 != 0:
        song_matches = (song_matches) & (songs["Group"] == group)
    return list(songs[song_matches].index)


def find_similar_id(
    songs: pd.DataFrame,
    song_id: Union[int, list[int]],
    timeframe: int = 30,
    points: Optional[list[int]] = None,
    indexlist: Optional[pd.DataFrame] = None,
    similarities: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    if (similarities is None and indexlist is None):
        raise ValueError("indexlist or similarities needed.")
    if isinstance(song_id, int):
        song_id = [song_id]
    if len(song_id) == 0 or timeframe == 0:
        result = pd.DataFrame(
            [], columns=["song_id", "Played last", "Point", "Place"]
        ).set_index("song_id")
        result["Played last"] = result["Played last"].astype("datetime64")
        return result
    if points is None:
        points = [10, 5, 2, 1, 1]
    if similarities is None:
        similarities = summarize_similars(
            songs,
            indexlist=indexlist,
            points=points,
            timeframe=timeframe
        )
    similarities_group: pd.Grouper
    if timeframe > 0:
        similarities = similarities[
            (similarities["song_id"].isin(song_id)) &
            ~(similarities["id_after"]).isin(song_id)
        ]
        similarities_group = similarities.groupby(["id_after"])
    else:
        similarities = similarities[
            (similarities["id_after"].isin(song_id)) &
            ~((similarities["song_id"].isin(song_id)))]
        similarities_group = similarities.groupby(["song_id"])
    sum_by_index: pd.DataFrame = (
        similarities_group.agg({"Time added": ["max"], "Point": ["sum"]})
        .reset_index()
        .set_axis(["song_id", "Played last", "Point"], axis=1)
        .sort_values(by=["Point", "Played last"], ascending=[False, False])
        .reset_index(drop=True)
    )
    sum_by_index["Place"] = sum_by_index.index + 1
    return sum_by_index.set_index("song_id")


def find_similar(
    artist: str,
    title: str = "",
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
    If the similarities DataFrame is supplied, the points and timeframe will
    be meaningless, however the sign of the timeframe will determine whether
    we look for songs that are played after (positive timeframe) or before
    (negative timeframe) the selected song.
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
    if pd.isnull(album) or album is None:
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
            songs,
            indexlist=indexlist,
            points=points,
            timeframe=timeframe
        )
    assert(isinstance(songs, pd.DataFrame))
    song_id: list[int] = get_song_id(songs, artist, title, album)
    sum_by_index: pd.DataFrame = find_similar_id(
        songs, song_id, timeframe, points, similarities=similarities)
    result_sum: pd.DataFrame = pd.merge(
        sum_by_index,
        songs.drop(["Played last", "Added first"], axis=1),
        left_index=True,
        right_index=True
    )
    result_sum["Method"] = "Sim"
    return result_sum


def find_similar_artist(
    artist: str,
    songlist: Optional[pd.DataFrame] = None,
    songs: Optional[pd.DataFrame] = None,
    indexlist: Optional[pd.DataFrame] = None,
    similarities: Optional[pd.DataFrame] = None,
    timeframe: int = 30,
    points: Optional[list[int]] = None,
) -> pd.DataFrame:
    """
    Finds the artists in 'songlists' played after any song from 'artist'for the
    time 'timeframe' (defaults to 30 minutes). Points is a list with the points
    for the artists right after the searched artist. Default is [10,5,2,1,1]
    which means that the artist played after searched artist gets 10 points,
    the 2nd song played after the current song gets 5, the third gets 2, etc.
    """
    results: pd.DataFrame = find_similar(
        artist,
        songlist=songlist,
        songs=songs,
        indexlist=indexlist,
        similarities=similarities,
        timeframe=timeframe,
        points=points
    )
    result_sum = (
        results.groupby(["artist_l"])
        .agg({"Played last": ["max"], "Point": ["sum"], "Artist": ["max"]})
        .set_axis(["Played last", "Point", "Artist"], axis=1)
        .sort_values(["Point", "Played last"], ascending=[False, False])
        .reset_index(drop=False)
    )
    return result_sum


def choose_song(
    similars: pd.DataFrame,
    avoid_artist: Union[str, list[str]] = "",
    **kwargs
) -> tuple[int, int]:
    """Choses a song from the similar list given."""
    trials: int = 15
    perc_inc: float = 2
    base_percent: float = 30
    for key, value in kwargs.items():
        if key == "trials":
            trials = max(1, value)
        elif key == "perc_inc":
            perc_inc = value
        elif key == "base_percent":
            base_percent = min(100, max(1, value))
    if similars.empty:
        return -1, -1
    if avoid_artist == "":
        avoid_artist = []
    if isinstance(avoid_artist, str):
        avoid_artist = [avoid_artist]
    avoid: list[str] = [
        artist.lower().replace(",", "").replace("\"", "") for
        artist in
        avoid_artist
    ]
    point_sum: float = similars["Point"].sum()
    trial_num: int = 0
    indices = similars.index
    similars = similars.reset_index(drop=True)
    ok_artist = False  # is there more artists in the last rows
    percent: float = base_percent
    while (trial_num < trials) and (not ok_artist):
        point_target = rnd.randint(0, int(point_sum * percent / 100))
        song_place = -1
        point = 0
        while point <= point_target:
            song_place += 1
            point += similars["Point"].values[song_place]
        next_artist = (
            similars["Artist"].values[song_place].
            lower().replace(",", "").replace("\"", ""))
        ok_artist = next_artist not in avoid
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
    base_percent: float = 30
    timeframe: int = 30 * 60
    points: list[int] = [10, 5, 2, 1, 1]
    cumul_points: npt.NDArray[np.int32] = np.array(
        [[10, 0, 0], [10, 5, 0], [5, 2, 1]]
    )
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
            songs,
            indexlist=indexlist,
            points=points,
            timeframe=timeframe
        )
    assert(isinstance(songs, pd.DataFrame))
    if cache is None:
        cache = Cache()
    if artist != "" and title != "":
        new_line = pd.DataFrame(
            [[artist, album, title, datetime.datetime.utcnow()]],
            columns=["Artist", "Album", "Title", "Time added"],
        )
        playlist = pd.concat([playlist, new_line], ignore_index=True, sort=False)
    for _ in range(length):
        if not playlist.empty:
            artist = playlist["Artist"].values[-1]
            title = playlist["Title"].values[-1]
            if playlist["Album"].isnull().values[-1]:
                album = ""
            else:
                album = playlist["Album"].values[-1]
        album = str(album)
        assert isinstance(album, str)
        if artist == "" or title == "":
            if songs is None:
                songs = summarize_songlist(songlist)
            search_in: pd.DataFrame = (
                songs.sort_values(by="Played", ascending=False)
            )
            if artist != "":
                search_in = (
                    search_in[(search_in["Artist"]) == artist]
                    .reset_index(drop=True)
                )
            if album != "":
                search_in = (
                    search_in[(search_in["Album"]) == album]
                    .reset_index(drop=True)
                )
            if search_in.empty:
                print("nothing matches what you want")
                break
            point_sum = search_in["Played"].sum()
            point_target = rnd.randint(0, int(point_sum * base_percent / 100))
            song_place = -1
            point = 0
            while point <= point_target:
                song_place += 1
                point += search_in["Played"].values[song_place]
            playlist = pd.concat(
                [
                    playlist,
                    pd.DataFrame(
                        [[datetime.datetime.utcnow()]], columns=["Played"]
                    ).join(
                        search_in[song_place : song_place + 1][
                            ["Artist", "Album", "Title"]
                        ].reset_index(drop=True)
                    ),
                ],
                sort=False,
            )
        else:
            if cumul_find:
                similars = cumul_similar(
                    playlist, songs, similarities, timeframe, cumul_points,
                )
            else:
                if cache.is_in_cache(artist, album, title):
                    similars = cache.return_from_cache(artist, album, title)
                else:
                    similars = find_similar(
                        artist, title, album,
                        songs=songs,
                        similarities=similarities,
                        timeframe=timeframe,
                        points=points
                    )
                    if not similars.empty:
                        cache.save_to_cache(artist, album, title, similars)
                similars["Last"] = similars["Place"]
            if remove_played(similars, playlist).empty:
                print("Expanding the playlist failed after:")
                print("    Artist: ", artist)
                print("    Album: ", album)
                print("    Title: ", title)
                return playlist
            song_place, trial_num = choose_song(
                remove_played(similars, playlist), artist, *kwargs
            )
            similars["Trial"] = trial_num
            similars["Time added"] = datetime.datetime.utcnow()
            playlist = pd.concat([
                playlist,
                similars.loc[
                    [song_place],
                    ["Artist", "Album", "Title", "Place", "Last", "Trial",
                        "Time added"]
                ]
            ]).reset_index(drop=True)
    return playlist[[
        "Artist", "Album", "Title", "Time added", "Place", "Last", "Trial"
    ]]


def list_group_count(playlist: pd.DataFrame, songs: pd.DataFrame) -> pd.DataFrame:
    indexlist = make_indexlist(playlist, songs)
    grouplist = pd.merge(
        indexlist, songs["Group"], how="left",
        left_on="song_id", right_index=True
    )
    groups = (
        grouplist.groupby("Group")
        .aggregate({"Time added": "count"})
        .sort_values("Time added", ascending=False)
    )
    return groups


def latest_songs(
    playlist: pd.DataFrame,
    timeframe: int = 30 * 60,
    length: int = 3
) -> pd.DataFrame:
    """Returns a list of the latest songs from the playlist."""
    if len(playlist.index) == 0:
        return pd.DataFrame([], columns=["Artist", "Album", "Title"])
    time = playlist.at[playlist.index[-1], "Time added"] - datetime.timedelta(
        seconds=timeframe
    )
    result = (
        playlist[(playlist["Time added"] > time)][-length:]
        .sort_index(ascending=False)
        .reset_index(drop=True)
    )[["Artist", "Album", "Title"]]
    return result


def cumul_similar(
    playlist: pd.DataFrame,
    songs: pd.DataFrame,
    similarities: pd.DataFrame,
    timeframe: int = 30 * 60,
    points: Optional[npt.NDArray[np.int32]] = None,
    single_points: Optional[list[int]] = None,
    show_all: bool = False,
):
    """Calculates the similarity points based on the cumulative points from
    multuiple base songs."""
    if points is None:
        points = np.array([[10, 0, 0], [8, 2, 0], [5, 3, 1]], dtype=np.int32)
    if single_points is None:
        single_points = [10, 5, 2, 1, 1]
    latests = latest_songs(playlist, timeframe, points.shape[0])
    songqueue: mp.Queue = mp.Queue()
    processes: dict[int, mp.Process] = {}
    song: int
    for song in latests.index:
        artist = latests.at[song, "Artist"]
        album = latests.at[song, "Album"]
        if album == np.nan:
            album = ""
        title = latests.at[song, "Title"]
        processes[song] = mp.Process(
            target=similars_parallel,
            args=(
                songqueue,
                song,
                artist,
                title,
                album,
                songs,
                similarities,
                timeframe,
            ),
        )
        processes[song].start()
    all_similars: dict[int, pd.DataFrame] = {}
    collected: list[int] = []
    similars: pd.DataFrame
    while len(collected) < len(latests.index):
        msg: dict[str, Union[int, pd.DataFrame]] = songqueue.get()
        similars = msg["res"]
        song = msg["index"]
        assert isinstance(similars, pd.DataFrame)
        assert isinstance(song, int)
        collected.append(song)
        if not similars.index.empty:
            if "Method" in similars.columns:
                similars = similars.drop("Method", axis=1)
            album = latests.at[song, "Album"]
            if pd.isnull(album):
                album = ""
            similars[f"P{song}"] = (
                similars["Point"] / np.sqrt(similars["Point"].sum()) * 100
            )
            similars[f"O{song}"] = similars["Place"]
            similars[f"L{song}"] = similars["Played last"]
            all_similars[song] = similars
        else:
            all_similars[song] = pd.DataFrame(
                [],
                columns=["song_id", f"L{song}", f"P{song}", f"O{song}"]
            ).set_index("song_id")
            all_similars[song][f"L{song}"] = (
                all_similars[song][f"L{song}"].astype("datetime64")
            )
    for proc in processes.values():
        proc.join()
    result = (
        all_similars[0].copy(deep=False)[["P0", "O0", "L0"]]
    )
    result["Played last"] = result["L0"]
    for song, similars in all_similars.items():
        if song == 0:
            continue
        result = pd.merge(
            result,
            similars[[f"P{song}", f"O{song}", f"L{song}"]],
            how="outer",
            left_index=True,
            right_index=True
        )
        result["Played last"] = result[["Played last", f"L{song}"]].max(axis=1)
    for i in latests.index:
        #result.loc[result[f"P{i}"].isnull(), f"P{i}"] = 0
        result[f"P{i}"] = result[f"P{i}"].fillna(0)
    result["Sum"] = 0
    for i in latests.index:
        for j in latests.index:
            if points[i, j] == 0:
                continue
            result[f"PC{i}-{j}"] = (
                np.sqrt(result[f"P{i}"] * result[f"P{j}"]) * points[i, j]
            )
            result["Sum"] = result["Sum"] + result[f"PC{i}-{j}"]
    result = result.sort_values(["Sum"], ascending=False).reset_index(drop=False)
    result["Point"] = result["Sum"] / result["Sum"].sum() * 100
    result = result[(result["Point"] > 0)]
    result["Place"] = result.index + 1
    result["Last"] = result["O0"].astype("Int32")
    result["Method"] = "CSim"
    result = result.set_index("song_id")
    if not show_all:
        result = result[["Point", "Last", "Place", "Played last", "Method"]]
    result_merge: pd.DataFrame = pd.merge(
        result,
        songs.drop(["Played last", "Added first"], axis=1),
        left_index=True,
        right_index=True
    )
    return result_merge

def generate_hourly_song(
    indexlist: pd.DataFrame,
    songs: pd.DataFrame,
    group_song: int = -1,
    day_of_week: int = -1,
    hour: int = -1
) -> pd.DataFrame:
    if hour < 0 or hour > 23:
        hour = datetime.datetime.utcnow().hour
    if day_of_week < 0 or day_of_week > 6:
        day_of_week = datetime.datetime.utcnow().weekday()
    hours: list[int] = [
        (hour - 1) % 24,
        hour,
        (hour + 1) % 24
    ]
    days: list[int] = [
        (day_of_week + (hour - 1) // 24) % 7,
        day_of_week,
        (day_of_week + (hour + 1) // 24) % 7,
    ]
    hourly_songs = indexlist.copy()
    hourly_songs["Hour"] = hourly_songs["Time added"].dt.hour
    hourly_songs["Weekday"] = hourly_songs["Time added"].dt.dayofweek
    for (h, d) in zip(hours, days):
        if hour == h:
            hour_point = 50
        else:
            hour_point = 20
        hourly_songs.loc[
            (hourly_songs["Hour"]==h) &
            (hourly_songs["Weekday"]!=d),
            "Hour point"
        ] = hour_point/200
        hourly_songs.loc[
            (hourly_songs["Hour"]==h) &
            (hourly_songs["Weekday"]==d),
            "Hour point"
        ] = hour_point
    hourly_songs["Hour point"] = (
        hourly_songs["Hour point"] /
        np.square(
            (datetime.datetime.utcnow() - hourly_songs["Time added"])
            .dt.days // 365 + 1
        )
    )
    hourly_songs = hourly_songs.groupby(["song_id"]).agg({"Hour point": "sum"})
    hourly_songs["Hour point"] = (
        hourly_songs["Hour point"] / hourly_songs["Hour point"].max()
    )
    group_points: pd.DataFrame = pd.DataFrame()
    if group_song == -1:
        group_points = (
            pd.merge(
                hourly_songs, songs, how="left",
                left_index=True, right_index=True
            ).groupby("Group").agg({"Hour point": "sum"})
        )
        group_points["Hour point"] = (
            group_points["Hour point"] / group_points["Hour point"].max()
        )
        group_points = group_points[
            (group_points["Hour point"] >= group_points["Hour point"].mean())
        ]
    choose_from = (
        pd.merge(
            songs, hourly_songs, how="left",
            left_index=True, right_index=True
        )
    )
    if group_song != -1 and group_song % 100 == 0:
        choose_from = choose_from[
            (choose_from["Group"] // 100 == group_song // 100)
        ]
    if group_song != -1 and group_song % 100 != 0:
        choose_from = choose_from[(choose_from["Group"] == group_song)]
    choose_from["Hour point"] = choose_from["Hour point"].fillna(0)
    if not group_points.empty:
        choose_from = pd.merge(
            choose_from,
            group_points.rename({"Hour point": "Group point"}, axis=1),
            how="left",
            left_on="Group",
            right_index=True
        )
        choose_from["Group point"] = choose_from["Group point"].fillna(0.00001)
    else:
        choose_from["Group point"] = 1
    choose_from["Point"] = (
        np.sqrt(choose_from["Played"] / choose_from["Played"].max())
        + choose_from["Group point"] / choose_from["Group point"].max()
        + choose_from["Hour point"]
        - 1
    )
    choose_from = choose_from.sort_values("Point", ascending=False)
    choose_from = choose_from[(choose_from["Point"]) >= 0]
    choose_from["Place"] = range(1, 1 + len(choose_from))
    choose_from["Method"] = f"H{day_of_week}-{hour}_{group_song}"
    return choose_from


def playlist_from_songs(
    songlist: pd.DataFrame, playlist: pd.DataFrame, date: pd.Timestamp
) -> pd.DataFrame:
    """
    Gets the songs from the songlist that have been playd after the last
    entry in playlist, and adds it to the end of playlist.
    """
    merge = (
        songlist[(songlist["Time added"] > pd.Timestamp(date))]
        .sort_values(by=["Time added"], ascending=[True])
    )
    return pd.concat([playlist, merge], sort=False).reset_index(drop=True)


def similars_parallel(
    queue: mp.Queue,
    index: int,
    artist: str,
    title: str,
    album: str = "",
    songs: Optional[pd.DataFrame] = None,
    similarities: Optional[pd.DataFrame] = None,
    timeframe: int = 30,
) -> None:
    """Looks for similar songs using parallel computations"""
    song_id: list[int] = get_song_id(songs, artist, title, album)
    result = find_similar_id(
        songs, song_id, timeframe, similarities=similarities
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
        playlist.at[last + 1, "Place"] = np.nan
        playlist.at[last + 1, "Last"] = np.nan
    return playlist.reset_index(drop=True)


def move_song(playlist: pd.DataFrame, song: int, place: int = 0) -> pd.DataFrame:
    "Moves a songs to an other place."
    song = max(song, 0)
    song = min(song, playlist.index.max())
    place = max(place, -1)
    place = min(place, playlist.index.max())
    playlist.at[song, "Place"] = np.nan
    playlist.at[song, "Last"] = np.nan
    if song + 1 in playlist.index:
        playlist.at[song + 1, "Place"] = np.nan
        playlist.at[song + 1, "Last"] = np.nan
    if place + 1 in playlist.index:
        playlist.at[place + 1, "Place"] = np.nan
        playlist.at[place + 1, "Last"] = np.nan
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
    items_to_remove: pd.DataFrame
) -> pd.DataFrame:
    list_copy = list_to_handle.copy()
    list_copy.index.name = "id"
    list_copy["artist_l"] = list_copy["Artist"].str.lower()
    list_copy["title_l"] = list_copy["Title"].str.lower()
    items_copy = items_to_remove.copy()
    items_copy["artist_l"] = items_copy["Artist"].str.lower()
    items_copy["title_l"] = items_copy["Title"].str.lower()
    items_copy["exists"] = True
    result: pd.DataFrame = pd.merge(
        list_copy[["artist_l", "title_l"]].reset_index(drop=False),
        items_copy[["artist_l", "title_l", "exists"]],
        on=["artist_l", "title_l"],
        how="left"
    )
    result["exists"] = result["exists"].fillna(False)
    result = result[~(result["exists"])]
    return list_to_handle[(list_to_handle.index.isin(result["id"]))]


def summarize_songlist(songlist: pd.DataFrame) -> pd.DataFrame:
    result: pd.DataFrame
    songlist = songlist.copy(deep=False)
    songlist = songlist.sort_values(by="Time added", ascending=True)
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
    result.index.name = "song_id"
    result["Album"] = result["Album"].fillna("")
    return result[[
        "Artist", "Album", "Title", "Played", "Played last",
        "Added first", "artist_l", "album_l", "title_l"
    ]]

#%%
def filter_and_order(
    artist: str = "",
    album: str = "",
    songlist: Optional[pd.DataFrame] = None,
    songs: Optional[pd.DataFrame] = None,
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
    max_play: the max number how many times a song has to have been played to
        appear
    per_album: whether a song that is on multiple albums should appear as
        separate entries per album (True) or not (False)
    """
    if songlist is None and songs is None:
        raise ValueError(
            "Songlist or songs is needed."
        )
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
    if songs is None:
        songs = summarize_songlist(songlist)
    else:
        songs = songs.copy(deep=False)
    if artist != "":
        songs = songs[(songs["artist_l"]==artist)]
    if album != "":
        songs = songs[(songs["album_l"]==album)]
    if songs.empty:
        print("Results are empty")
        return songs
    if not per_album:
        songs = (
            songs
            .groupby(["artist_l", "title_l"])
            .agg({
                "Played": "sum",
                "Played last": "max",
                "Added first": "min",
                "Artist": "min",
                "Title": "min"
            })
            .reset_index(drop=False)
        )
        songs["Album"] = ""
        songs["album_l"] = ""
    songs = songs[(songs["Played"] >= min_play)]
    if max_play > 0:
        songs = songs[(songs["Played"] <= max_play)]
    if sort_by == "recent p":
        songs = songs.sort_values(by="Played last", ascending=False)
    elif sort_by == "old p":
        songs = songs.sort_values(by="Played last", ascending=True)
    elif sort_by == "new add":
        songs = songs.sort_values(by="Added first", ascending=False)
    elif sort_by == "rarely":
        songs = songs.sort_values(by=["Played", "Played last"], ascending=True)
    else:
        songs = songs.sort_values(by=["Played", "Played last"], ascending=False)
    return songs.reset_index(drop=True)


def find_old_song(
    songlist: pd.DataFrame,
    playlist: pd.DataFrame,
    date: pd.Timestamp,
    sort_by_last: bool = False,
) -> pd.DataFrame:
    """
    Finds the songs that haven't been playd since the specified date and
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


def unique(playlist):
    playlist2 = playlist.copy()
    playlist2["artist_low"] = playlist2["Artist"].str.lower()
    playlist2["title_low"] = playlist2["Title"].str.lower()
    return len(
        playlist2.groupby(["artist_low", "title_low"]).
        agg({"Time added": "count"}).index
    )


def save_data(
    songlist: pd.DataFrame,
    songs: pd.DataFrame,
    playlist: pd.DataFrame,
    filename: str = "data",
) -> None:
    """
    Saves the songlist and the playlist in a pickle file.
    """
    songlist.to_csv(filename + "_songlist.csv", index=False, header=True)
    songs.sort_index().to_csv(filename + "_songs.csv", index=True, header=True)
    playlist.to_csv(filename + "_playlist.csv", index=False, header=True)


def revise_summarized_list(
    old: pd.DataFrame,
    new: pd.DataFrame
) -> pd.DataFrame:
    """Merges the new database into the old one overwriting play data, but
    keeps the grouping and the song_id from the old data."""
    old = old[["artist_l", "title_l", "album_l", "Group"]].reset_index(drop=False)
    merged = (
        pd.merge(old, new, on=["artist_l", "title_l", "album_l"], how="outer")
        .sort_values(by="song_id", ascending=True)
    )
    #Some data in the old database might not be in the new one (i.e. album has
    #been added)
    removed = merged[(merged["Title"].isna())]
    merged = merged[(merged["Title"].notna())]
    removed = pd.merge(
        removed[["song_id", "artist_l", "title_l", "Group"]],
        merged[(merged["song_id"].isna())][[
            "artist_l", "album_l", "title_l", "Artist", "Album", "Title",
            "Played", "Played last", "Added first"
        ]],
        on=["artist_l", "title_l"],
        how="right"
    )
    merged = (
        pd.concat([merged[(merged["song_id"].notna())], removed])
        .reset_index(drop=True)
    )
    #we give new song_id to those elements that hasn't been in the old db
    new_entries = merged[(merged["song_id"].isna())].index
    new_ids = list(range(
        int(merged["song_id"].max()) + 1,
        int(merged["song_id"].max() + 1 + len(new_entries))
        ))
    merged.loc[new_entries, "song_id"] = new_ids
    merged["song_id"] = merged["song_id"].astype(int)
    merged["Played"] = merged["Played"].astype(int)
    merged = merged.set_index("song_id")
    return merged[[
        "Artist", "Album", "Title", "Played", "Played last",
        "Added first", "Group", "artist_l", "album_l", "title_l"
    ]]


def load_data(
    filename: str = "data",
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Loads the data from CSV files."""
    songlist = pd.read_csv(
        filename + "_songlist.csv",
        sep=",",
        header=0,
        parse_dates=["Time added"]
    )
    songlist["Time added"] = pd.to_datetime(songlist["Time added"], format="mixed")
    songlist["Album"] = songlist["Album"].fillna("")
    songs = pd.read_csv(
        filename + "_songs.csv",
        sep=",",
        header=0,
        index_col=0,
        parse_dates=["Played last",	"Added first"],
        dtype={"Group": "Int16"},
    )
    songs["Album"] = songs["Album"].fillna("")
    songs["artist_l"] = songs["Artist"].str.lower()
    songs["title_l"] = songs["Title"].str.lower()
    songs["album_l"] = songs["Album"].fillna("").str.lower()
    songs = songs.drop_duplicates(subset=["artist_l", "album_l", "title_l"])
    playlist_old = pd.read_csv(
        filename + "_playlist.csv",
        sep=",",
        header=0,
        parse_dates=["Time added"],
        dtype={"Place": "Int16", "Trial": "Int16", "Last": "Int16"}
    )
    now = datetime.datetime.utcnow()
    playlist = songlist[(
        songlist["Time added"] >
        now - datetime.timedelta(days=21)
    )]
    playlist = playlist.sort_values(by="Time added").reset_index(drop=True)
    playlist["Secs"] = (
        (playlist["Time added"] - pd.Timestamp("1970-01-01")).dt.total_seconds()
    )
    playlist_old["Secs"] = (
        (playlist_old["Time added"] - pd.Timestamp("1970-01-01"))
        .dt.total_seconds()
    )
    playlist = pd.merge(
        playlist,
        playlist_old[["Secs", "Place", "Trial", "Last", "Method"]],
        how="left",
        on="Secs"
    )
    playlist = playlist.drop(labels="Secs", axis=1)
    if unique(playlist) < 800:
        return songlist, songs, playlist
    playlist2 = (
        playlist[(
            playlist["Time added"] > now - datetime.timedelta(days=10)
        )].reset_index(drop=True)
    )
    if unique(playlist2) > 600:
        return songlist, songs, playlist2
    unique_songs: int = unique(playlist[-600:])
    playlist = (
        playlist[-(1200-unique_songs):]
    ).reset_index(drop=True)
    return songlist, songs, playlist


def load_partial(
    songlist: pd.DataFrame,
    songs: pd.DataFrame,
    filename: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Loads the newest plays and adds it to the current songlist.
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
    songs = summarize_songlist(songlist)
    return songlist, songs

