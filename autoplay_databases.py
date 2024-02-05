#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Oct  3 20:35:00 2021

@author: nicemicro
"""

from datetime import datetime
from typing import Union, Optional
import numpy as np
import pandas as pd

import engine as e
from mpd_wrapper import MPD

#%%

class DataBases:
    def __init__(self, filename: str):
        pd.options.display.max_colwidth = 25
        pd.options.display.width = 120
        e.pd.options.display.expand_frame_repr = False

        self.songlist = pd.DataFrame([])
        self.artists = pd.DataFrame([])
        self.albums = pd.DataFrame([])
        self.playlist = pd.DataFrame([])
        self.songs = pd.DataFrame([])
        self.indexlist = pd.DataFrame([])
        self.similarities = pd.DataFrame([])
        self.similars_cache = e.Cache()
        self.load_file(filename)
        self.currentplayed: int
        if self.playlist.empty:
            self.currentplayed = -1
        else:
            self.currentplayed = self.playlist.index[-1]
        self.sugg_cache: dict[int, pd.DataFrame] = {}
        self.playablelist = pd.DataFrame([])
        self.plstartindex = 0
        self.plendindex = 0
        self.music = MPD()               # create music object
        self.music.timeout = 100         # network timeout in seconds
        self.music.idletimeout = None    # for fetching the result of idle command
        self.music.connect("localhost", 6600)

    def search_song(
        self,
        artist: str,
        album: str,
        title: str,
        strict: bool = True
    ):
        """
        Searches for songs with artist, album and title given, in the MPD database
        """
        artist = artist.replace(",", "")
        album = album.replace(",", "")
        title = title.replace(",", "")
        artist = artist.replace("\"", "")
        album = album.replace("\"", "")
        title = title.replace("\"", "")
        s_strings = []

        for word in [w for w in title.split(" ") if len(w) > 2]:
            s_strings.append("title")
            s_strings.append(word.replace(":", ""))
        for word in [w for w in artist.split(" ") if len(w) > 2]:
            s_strings.append("artist")
            s_strings.append(word.replace(":", ""))
        if len(s_strings) == 0:
            return pd.DataFrame()
        s_strings2 = s_strings.copy()
        if len(s_strings) > 30:
            s_strings = s_strings[0:30]
        if len(s_strings2) > 40:
            s_strings = s_strings[0:40]
        for word in [w for w in album.split(" ") if len(w) > 2]:
            s_strings.append("album")
            s_strings.append(word.replace(":", ""))
        if len(s_strings) > 40:
            s_strings = s_strings[0:40]

        result = self.music.search(*s_strings)
        if len(result) == 0:
            result = self.music.search(*s_strings2)
        if len(result) == 0:
            return pd.DataFrame(result)
        if len(result) == 1:
            #print(f"Search: {artist}-{album}-{title}, found {len(result)}")
            #print(result)
            if not "album" in result[0]:
                result[0]["album"] = ""
            result = pd.DataFrame(result)
            if strict:
                if (
                    result["artist"].str.lower().str.replace(",", "").
                    str.replace("\"", "")[0] != artist.lower()
                ):
                    return pd.DataFrame()
                if (
                    album and
                    result["album"].str.lower().str.replace(",", "").
                    str.replace("\"", "")[0] != album.lower()
                ):
                    return pd.DataFrame()
                if (
                    result["title"].str.lower().str.replace(",", "").
                    str.replace("\"", "")[0] != title.lower()
                ):
                    return pd.DataFrame()
            return result
        #print(f"Search: {artist}-{album}, found {len(result)}")
        if not "album" in result[0]:
            result[0]["album"] = ""
        result = pd.DataFrame(result)
        result = result[
            (result["title"].str.replace(",", "").str.replace("\"", "").str.lower()
            == title.lower())
        ]
        result = result[
            (result["artist"].str.replace(",", "").str.replace("\"", "").str.lower()
            == artist.lower())
        ]
        return result.reset_index(drop=True)

    def new_songlist(self, *args, **kwargs):
        kwargs["songs"] = self.songs
        self.playablelist = e.filter_and_order(
            *args, **kwargs
        )
        self.playablelist = (
            e.remove_played(self.playablelist, self.playlist)
            .reset_index(drop=True)
        )
        #self.playablelist["Album"] = self.playablelist["Album"].fillna("")
        #self.playablelist["album_l"] = self.playablelist["album_l"].fillna("")
        self.plstartindex = 0
        self.plendindex = 0

    def mergesongdata(self, music_file_list: pd.DataFrame) -> pd.DataFrame:
        """
        Merges the song list coming from MPD (music_file_list) with the list
        coming from the database (songs).
        """
        if music_file_list.empty:
            return pd.DataFrame([])
        music_file_list = music_file_list.reset_index(drop=True)
        music_file_list = music_file_list.reset_index(drop=False)
        for dictkey in ["artist", "album", "title"]:
            if dictkey not in music_file_list.columns:
                music_file_list[dictkey] = ""
                music_file_list[dictkey+"_l"] = ""
                continue
            music_file_list[dictkey+"_l"] = music_file_list[dictkey].str.lower()
            music_file_list[dictkey+"_l"] = (
                music_file_list[dictkey+"_l"].str.replace(",", "")
                .str.replace("\"", "")
            )
        #music_file_list.to_csv("music_file_list.csv")
        withalbum = self.songs[(self.songs["album_l"]) != ""]
        noalbum = self.songs[(self.songs["album_l"]) == ""]
        withalbum = pd.merge(
            music_file_list, withalbum, how="inner",
            on=["artist_l", "album_l" ,"title_l"]
        )
        withalbum = withalbum.set_index("index")
        noalbum = pd.merge(
            music_file_list, noalbum, how="inner", on=["artist_l", "title_l"]
        )
        noalbum = noalbum.set_index("index")
        all_found = pd.concat([noalbum, withalbum])
        result = pd.merge(
                music_file_list.set_index("index"),
                all_found[["Played last", "Added first", "Played"]],
                how="left",
                left_index=True,
                right_index=True
                )
        result["Artist"] = result["artist"].str.replace(",", "").str.replace("\"", "")
        result["Album"] = result["album"].str.replace(",", "").str.replace("\"", "")
        result["Title"] = result["title"].str.replace(",", "").str.replace("\"", "")
        return result

    def list_songs_fwd(self, num: int):
        index = self.plendindex
        songs = self.playablelist
        new_list = pd.DataFrame([])
        while len(new_list.index) < num and index < len(songs.index):
            song = self.search_song(songs.at[index, "Artist"],
                    songs.at[index, "Album"],
                    songs.at[index, "Title"])
            if len(song.index) > 0:
                new_list = pd.concat([new_list, song[0:1]])
            index += 1
        if len(new_list.index) > 0:
            self.plstartindex = self.plendindex
            self.plendindex = index
        new_list = self.mergesongdata(new_list)
        return new_list

    def list_songs_bck(self, num):
        index = self.plstartindex
        songs = self.playablelist
        new_list = pd.DataFrame([])
        while len(new_list.index) < num and index > 0:
            index -= 1
            song = self.search_song(songs.at[index, "Artist"],
                    songs.at[index, "Album"],
                    songs.at[index, "Title"])
            if len(song.index) > 0:
                new_list = pd.concat([song[0:1], new_list])
        if len(new_list.index) > 0:
            self.plendindex = self.plstartindex
            self.plstartindex = index
        new_list = self.mergesongdata(new_list)
        return new_list

    def sugg_from_artists(self, artist: str) -> pd.DataFrame:
        s_artists: pd.DataFrame = e.find_similar_artist(
            artist,
            songs=self.songs,
            similarities=self.similarities
        )
        s_artists = s_artists[(s_artists["Point"] > s_artists["Point"].sum()/50)]
        suggestion: pd.DataFrame = e.pd.merge(
            s_artists.drop("Artist", axis=1),
            self.songs.drop("Played last", axis=1).reset_index(drop=False),
            how="left", on="artist_l"
        )
        #suggestion["Album"] = suggestion["Album"].fillna("")
        suggestion["Point"] = suggestion["Point"] * suggestion["Played"]
        suggestion["Place"] = range(0, 0 + len(suggestion))
        suggestion = (
            suggestion.sort_values(by="Point", ascending=False)
            .set_index("song_id")
        )
        return suggestion

    def generate_suggestion(self, index: int = -1) -> pd.DataFrame:
        if index == -1:
            index = max(self.playlist.index)
        song_id_list: list[int] = e.get_song_id(
            self.songs,
            self.playlist.at[index, "Artist"],
            self.playlist.at[index, "Title"],
            self.playlist.at[index, "Album"],
        )
        if len(song_id_list) == 0:
            plays = 0
        else:
            plays = self.songs.loc[song_id_list[0], "Played"]
        suggestion: pd.DataFrame = e.cumul_similar(
            self.playlist[:index+1], self.songs, self.similarities,
        )
        if plays < 15:
            artist_sugg = self.sugg_from_artists(self.playlist.at[index, "Artist"])
            artist_sugg["Point"] = (
                artist_sugg["Point"] / artist_sugg["Point"].sum() * 100
            )
            suggestion = pd.merge(
                suggestion[["Point", "Played last", "Last"]],
                artist_sugg[["Point", "Played last"]],
                left_index=True,
                right_index=True,
                how="outer"
            )
            suggestion["Point_x"] = suggestion["Point_x"].fillna(0)
            suggestion["Point_y"] = suggestion["Point_y"].fillna(0)
            suggestion["Point"] = (
                suggestion["Point_x"] * (0.5 + plays/30) +
                suggestion["Point_y"] * (0.5 - plays/30)
            )
            suggestion["Played last"] = (
                suggestion[["Played last_x", "Played last_y"]].max(axis=1)
            )
            suggestion = e.pd.merge(
                suggestion.drop(
                    ["Point_x", "Point_y", "Played last_x", "Played last_y"],
                    axis=1
                ),
                self.songs.drop("Played last", axis=1),
                left_index=True,
                right_index=True,
                how="left"
            )
            suggestion = suggestion.sort_values("Point", ascending=False)
            suggestion["Place"] = range(1, len(suggestion)+1)
        return suggestion

    def generate_hourly_song(
        self, group_song: int = -1, day_of_week: int = -1, hour_now: int = -1
    ) -> pd.DataFrame:
        if hour_now < 0 or hour_now > 23:
            hour_now = datetime.utcnow().hour
        if day_of_week < 0 or day_of_week > 6:
            day_of_week = datetime.utcnow().weekday()
        hours: list[int] = [
            (hour_now - 1) % 24,
            hour_now,
            (hour_now + 1) % 24
        ]
        days: list[int] = [
            (day_of_week + (hour_now - 1) // 24) % 7,
            day_of_week,
            (day_of_week + (hour_now + 1) // 24) % 7,
        ]
        hourly_songs = self.indexlist.copy()
        hourly_songs["Hour"] = hourly_songs["Time added"].dt.hour
        hourly_songs["Weekday"] = hourly_songs["Time added"].dt.dayofweek
        hourly_songs = pd.concat(
            [
                hourly_songs[
                    (hourly_songs["Hour"]==h) &
                    (hourly_songs["Weekday"]==d
                )] for (h, d) in zip(hours, days)
            ]
        )
        hourly_songs["Hour point"] = 20
        hourly_songs.loc[(hourly_songs["Hour"] == hour_now), "Hour point"] = 50
        hourly_songs["Hour point"] = (
            hourly_songs["Hour point"] /
            np.square(
                (datetime.utcnow() - hourly_songs["Time added"]).dt.days // 365
                + 1
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
                    hourly_songs, self.songs, how="left",
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
                self.songs, hourly_songs, how="left",
                left_index=True, right_index=True
            )
        )
        if group_song >= 1 and group_song <= 9:
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
        return choose_from

    def select_hourly_song(self, group_song: int = -1):
        choose_from = self.generate_hourly_song(group_song=group_song)
        choose_from = e.remove_played(choose_from, self.playlist)
        choose_from = choose_from.iloc[
            0:min(len(choose_from), max(25, int(len(choose_from)/3)))
        ]
        song: pd.DataFrame = pd.DataFrame()
        last_index: int = max(self.playlist.index)
        avoid_artist: str = ""
        if (
            not self.playlist.empty and (
                datetime.utcnow() - self.playlist.loc[last_index, "Time added"] <
                pd.Timedelta(minutes=30)
            )
        ):
            avoid_artist = self.playlist["Artist"].values[-1]
        while song.empty and not choose_from.empty:
            song_index, trial = e.choose_song(choose_from, avoid_artist)
            print(f" -- Len: {len(choose_from)}: --")
            print(choose_from.head()[[
                "Place", "Artist", "Album", "Title", "Group"]])
            print(
                f"  selected from list:  {song_index},",
                f"place {choose_from.at[song_index, 'Place']}"
            )
            song = self.search_song(
                choose_from.at[song_index, "Artist"],
                choose_from.at[song_index, "Album"],
                choose_from.at[song_index, "Title"]
            )
            if not song.empty:
                song["Place"] = choose_from.at[song_index, "Place"]
                song["Last"] = np.NaN
                song["Trial"] = trial
            choose_from = choose_from.drop(song_index)
        return song

    def suggest_song(self, group_song: int = -1) -> pd.DataFrame:
        song = pd.DataFrame()
        last_index = max(self.playlist.index)
        print()
        print(f" -- Looking for similars after {last_index}, group {group_song}--")
        print(self.playlist[-5:])
        index = last_index
        artist: str = ""
        avoid_artist: str = ""
        if not self.playlist.empty:
            avoid_artist = self.playlist["Artist"].values[-1]
        album: Union[str, float] = ""
        title: str = ""
        choose_from: pd.DataFrame
        while (
            song.empty and index >= 0 and (
                datetime.utcnow() - self.playlist.loc[index, "Time added"] <
                pd.Timedelta(minutes=30)
            )
        ):
            if index not in self.sugg_cache:
                print(f"      >> Generating list for {index}")
                self.sugg_cache[index] = self.generate_suggestion(index)
            choose_from = e.remove_played(self.sugg_cache[index], self.playlist)
            choose_from = choose_from.iloc[
                0:min(len(choose_from), max(25, int(len(choose_from)/3)))
            ]
            if group_song >= 1 and group_song <= 9:
                choose_from = choose_from[(choose_from["Group"] == group_song)]
            if choose_from.empty:
                index -= 1
                while index not in self.playlist.index:
                    index -= 1
                continue
            song_index, trial = e.choose_song(choose_from, avoid_artist)
            print(f" -- From a list {index}, len: {len(self.sugg_cache[index])}: --")
            print(choose_from.head()[["Place", "Artist", "Album", "Title", "Group"]])
            print(
                f"  selected from list:  {song_index},",
                f"place {choose_from.at[song_index, 'Place']}"
            )
            artist = self.sugg_cache[index].at[song_index, "Artist"]
            album = self.sugg_cache[index].at[song_index, "Album"]
            if pd.isnull(album):
                album = ""
            assert isinstance(album, str)
            title = self.sugg_cache[index].at[song_index, "Title"]
            song = self.search_song(
                artist, album, title
            )
            if not song.empty:
                song["Place"] = self.sugg_cache[index].at[song_index, "Place"]
                song["Last"] = self.sugg_cache[index].at[song_index, "Last"]
                song["Trial"] = trial
            self.sugg_cache[index] = self.sugg_cache[index].drop(song_index)
        if song.empty:
            song = self.select_hourly_song(group_song)
        if song.empty:
            print("giving up")
            return song
        song = self.mergesongdata(song)
        newline = (song[0:1][["Artist", "Album", "Title", "Place", "Last", "Trial"]])
        newline = newline.rename(
            {0: last_index+1},
            axis="index"
        )
        newline["Time added"] = datetime.utcnow()
        self.playlist = pd.concat([self.playlist, newline])
        return song

    def search_string(self, string: str, hide_played: bool) -> pd.DataFrame:
        keys: list[str] = string.split(" ")
        result_list: list[dict[str, str]]
        if len(keys) == 1 and len(keys[0]) <= 2:
            result_list = self.music.find("any", keys[0])
        else:
            search_list: list[str] = []
            for key in keys:
                search_list += ["any", key]
            result_list = self.music.search(*search_list)
        if not result_list:
            return pd.DataFrame([])
        result = self.mergesongdata(pd.DataFrame(result_list))
        if hide_played:
            #result.to_csv("result.csv")
            #e.remove_played(result, self.playlist).to_csv("result_cut.csv")
            return e.remove_played(result, self.playlist)
        return result

    def search_artist(self, search_string):
        result = pd.DataFrame([])
        if not search_string:
            return result
        search_string = search_string.lower()
        artists = self.artists.copy()
        artists["Artist_l"] = artists["Artist"].str.lower()
        artist_match = artists[(artists["Artist_l"].str.\
                                contains(search_string))]
        if len(artist_match.index) == 0:
            return result 
        full = pd.DataFrame([])
        for index in artist_match.index:
            partial = e.filter_and_order(
                songs=self.songlist,
                artist=artist_match.at[index, "Artist"],
                sort_by="rarely"
            )
            full = pd.concat([full, partial])
            for index2 in partial.index:
                song = self.search_song(partial.at[index2, "Artist"],
                                        partial.at[index2, "Album"],
                                        partial.at[index2, "Title"])
                result = pd.concat([result, song[0:1]])
        new_list = self.mergesongdata(result)
        new_list = new_list.reset_index(drop=True)
        return new_list
    
    def songlist_append(self, artist, album, title):
        if (
            (self.songlist.at[0, "Artist"].lower() == artist.lower()) and
            (self.songlist.at[0, "Title"].lower() == title.lower())
        ):
            return
        new_line = pd.DataFrame([[artist, album, title, datetime.utcnow()]],
                                columns=["Artist", "Album", "Title",
                                         "Time added"])
        self.songlist = pd.concat([new_line, self.songlist]).reset_index(drop=True)
        index: int = self.currentplayed
        while index <= max(self.playlist.index):
            if index not in self.playlist.index:
                continue
            self.playlist.at[index, "Time added"] = datetime.utcnow()
            index += 1
        self.save_file()
    
    def playlist_append(self, artist, album, title):
        last_index = self.playlist.index[-1]
        if ((self.playlist.at[last_index, "Artist"].lower() == artist.lower()) and \
                (self.playlist.at[last_index, "Title"].lower() == title.lower())):
            return
        if len(self.playlist.index) == 0:
            index = 0
        else:
            index = max(self.playlist.index)+1
        new_line = pd.DataFrame(
            [[artist, album, title, datetime.utcnow()]],
            columns=["Artist", "Album", "Title", "Time added"],
            index=[index]
        )
        self.playlist = pd.concat([self.playlist, new_line])

    def add_song(self, position, filedata, jump=False):
        #print(f"add_song position={position}, jump={jump}")
        if position != -1:
            ret_data = self.delete_song(position, -1, jump)
        else:
            ret_data = pd.DataFrame([{"delfrom": -1, "delto": -1,
                                      "jump": jump}])
        #print("current suggestion list:")
        #for i, a, t in zip([line["index"] for line in self.suggestion],
        #                [line["artist"] for line in self.suggestion],
        #                [line["title"] for line in self.suggestion]):
        #    print(f"    {i}. {a} - {t}")
        filename, artist, album, title = (
            filedata[0], filedata[1], filedata[2], filedata[3]
        )
        if len(self.playlist.index) == 0:
            index = 0
        else:
            index = max(self.playlist.index)+1
        new_line = pd.DataFrame(
            [[artist, album, title, datetime.utcnow(), np.NaN, np.NaN]],
            columns=["Artist", "Album", "Title", "Time added", "Place", "Trial"],
            index=[index]
        )
        self.playlist = pd.concat([self.playlist, new_line])
        ret_data["file"] = filename
        return ret_data

    def delete_song(self, delfrom, delto, jump=False):
        print(f"delete_song delfrom={delfrom}, delto={delto}, jump={jump}")
        if self.playlist.empty:
            return
        self.db_maintain()
        indeces: list[int] = list(
            self.playlist[self.playlist.index >= self.currentplayed].index
        )
        if delto == -1:
            dellist = list(
                self.playlist[
                    self.playlist.index >= indeces[delfrom]
                ].index
            )
        else:
            dellist = list(
                self.playlist[
                    (self.playlist.index >= indeces[delfrom]) &
                    (self.playlist.index < indeces[delto])
                ].index
            )
        print(f"  Deleting indexes {dellist}")
        for todel in dellist:
            if todel not in self.sugg_cache.keys():
                continue
            self.sugg_cache.pop(todel)
        self.playlist = self.playlist.drop(dellist)
        if delfrom == 0:
            if len(self.playlist[self.playlist.index < self.currentplayed].index) == 0:
                self.currentplayed = -1
            else:
                self.currentplayed = (
                    max(self.playlist[self.playlist.index < self.currentplayed].index)
                )
        return pd.DataFrame([{"delfrom": delfrom, "delto": delto,
                              "jump": jump}])

    def db_maintain(self):
        status = self.music.status()
        if status["state"] != "play":
            return
        #print("Start DB maintain...")
        currentsong = self.music.currentsong()
        duration = float(status["duration"])
        elapsed = float(status["elapsed"])
        c_artist = currentsong["artist"].replace(",", "").replace("\"", "")
        if "album" in currentsong:
            c_album = currentsong["album"].replace(",", "").replace("\"", "")
        else:
            c_album = ""
        c_title = currentsong["title"].replace(",", "").replace("\"", "")
        line = self.currentplayed
        while line < max(self.playlist.index) and line > -1:
            if line not in self.playlist.index:
                line += 1
                continue
            if not isinstance(self.playlist.at[line, "Album"], str):
               self.playlist.at[line, "Album"] = ""
            if (
                c_artist.lower() == self.playlist.at[line, "Artist"].lower() and
                c_title.lower() == self.playlist.at[line, "Title"].lower() and
                c_album.lower() == self.playlist.at[line, "Album"].lower()
            ):
                self.currentplayed = line
                break
            line += 1
        # Registering the currently playing song on the songs list
        if elapsed > 180 or elapsed > duration / 2:
            self.songlist_append(c_artist, c_album, c_title)
        #print("End DB maintain...")

    def load_file(self, fname=""):
        if fname == "":
            fname = "data"
        self.songlist, saved_songs, self.playlist = e.load_data(fname)
        self.songs = e.summarize_songlist(self.songlist)
        self.songs = e.revise_summarized_list(saved_songs, self.songs)
        self.indexlist = e.make_indexlist(self.songlist, self.songs)
        self.similarities = e.summarize_similars(
            self.songs, indexlist=self.indexlist
        )

    def save_file(self, fname=""):
        if fname == "":
            fname= "data"
        #print("Start file save")
        e.save_data(
            self.songlist, self.songs, self.playlist, fname
        )
        #print("Save complete")

if __name__ == "__main__":
    db = DataBases("data")
