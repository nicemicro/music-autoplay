#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 15 08:18:36 2021

@author: nicemicro
"""

import threading
import time
from queue import Queue
import autoplay_databases as apdb
import pandas as pd

FWDLIST = 4
#%%
class DataBaseWrapper(threading.Thread):
    def __init__(self, filename, command, response):
        threading.Thread.__init__(self)
        self.comm = command
        self.resp = response
        self.db = apdb.DataBases(filename)
        self.cmds = {
            "new_songlist": self.db.new_songlist,
            "list_songs_fwd": self.db.list_songs_fwd,
            "list_songs_bck": self.db.list_songs_bck,
            "suggest_song": self.db.suggest_song,
            "search_artist": self.db.search_artist,
            "delete_song": self.db.delete_song,
            "add_song": self.db.add_song,
            "db_maintain": self.db.db_maintain,
            "load_file": self.db.load_file,
            "save_file": self.db.save_file
            }
    
    def run(self):
        exitFlag = False
        wait = False
        while not exitFlag:
            if wait:
                time.sleep(0.2)
            if self.comm.empty():
                wait = True
                continue
            funct, arguments = self.comm.get()
            #print("Received order to run: ", funct)
            if funct in self.cmds:
                wait = False
                if type(arguments) is list:
                    ret = self.cmds[funct](*arguments)
                elif type(arguments) is dict:
                    ret = self.cmds[funct](**arguments)
                else:
                    assert False, "Unknown command have been passed"
                if not ret is None:
                    #print("Function ", funct, " returned something.")
                    self.resp.put([funct, ret])
            elif funct == "quit":
                exitFlag=True

#%%
class MusicHandler():
    def __init__(self, music):
        self.music = music
        self.comm_que = Queue()
        self.resp_que = Queue()
        self.db_wrap = DataBaseWrapper("data", self.comm_que, self.resp_que)
        self.db_wrap.start()
        #self.db = apdb.DataBases("data")
        self.clear_playlist()
        self.result_storage = {}
            
    def clear_playlist(self):
        status = self.music.status()
        if status["state"] == "stop":
            self.music.clear()
            return
        mpdlistlen = int(status["playlistlength"])
        if "song" not in status:
            return
        mpdlistpos = int(status["song"])
        if mpdlistpos <= mpdlistlen - 2:
            self.music.delete((mpdlistpos + 1, mpdlistlen))
        if mpdlistpos > 0:
            self.music.delete((0, mpdlistpos))
            
    def play_pause(self):
        if self.music.status()["state"] != "play":
            self.music.play()
        else:
            self.music.pause()
            
    def get_results_from_queue(self):
        while not self.resp_que.empty():
            response = self.resp_que.get()
            if response[0][0:10] == "list_songs":
                self.result_storage["list_songs"] = response[1]
            else:
                self.result_storage[response[0]] = response[1]
    
    def add_suggested_song(self):
        assert "suggest_song" in self.result_storage, \
            "This should only be called if we started a search for suggested songs"
        self.get_results_from_queue()
        #print("  - checking for returned suggestions")
        if self.result_storage["suggest_song"].empty:
            #print("    no suggestion returned")
            return
        suggestion = self.result_storage.pop("suggest_song")
        self.music.add(suggestion.at[0, "file"])
        #print("    suggestion returned: ", suggestion.at[0, "file"])
        self.music.random(0)
        # If the music is stopped, but we get a new song in, that means that
        # we got a command to look for (and play) this song before
        status = self.music.status()
        if status["state"] == "stop":
            #print(status)
            mpdlistlen = int(status["playlistlength"])
            self.music.play(mpdlistlen - 1)
    
    def find_suggested_song(self):
        #currentsong = self.music.currentsong()
        #if len(currentsong) == 0:
        #    return
        #c_artist, c_album, c_title = self.current_song_data(currentsong)
        #self.comm_que.put(["playlist_append", [c_artist, c_album, c_title]])
        #self.db.playlist_append(c_artist, c_album, c_title)
        assert not "suggest_song" in self.result_storage, \
            "This shouldn't be called if we already have a search going!"
        #print("> Initiating search for new suggestion")
        self.comm_que.put(["suggest_song", []])
        #suggestion = self.db.suggest_song(self.music, c_artist, c_album,
        #                                  c_title)
        self.result_storage["suggest_song"] = pd.DataFrame([])
    
    def execute_play_file(self):
        assert "add_song" in self.result_storage, \
            "This should only be called if we initiated adding"
        self.get_results_from_queue()
        #print("  - checking for returned suggestions")
        if self.result_storage["add_song"].empty:
            #print("    no suggestion returned")
            return
        status = self.music.status()
        song_data = self.result_storage.pop("add_song")
        if song_data.at[0, "delfrom"] != -1 and "song" in status:
            mpdlistpos = int(status["song"])
            mpdlistlen = int(status["playlistlength"])
            delfrom = song_data.at[0, "delfrom"] + mpdlistpos
            if song_data.at[0, "delto"] == -1:
                delto = mpdlistlen
            else:
                delto = song_data.at[0, "delto"] + mpdlistpos
            #print(f"trying to execute mpd deletion {delfrom}-{delto} ({mpdlistlen})")
            self.music.delete((delfrom, delto))
        self.music.add(song_data.at[0, "file"])
        if song_data.at[0, "jump"]:
            self.music.next()
        #print("    suggestion returned: ", suggestion.at[0, "file"])
        self.music.random(0)
        # If the music is stopped, but we get a new song in, that means that
        # we got a command to look for (and play) this song before
        status = self.music.status()
        if status["state"] == "stop":
            #print(status)
            mpdlistlen = int(status["playlistlength"])
            self.music.play(mpdlistlen - 1)
        self.comm_que.put(["db_maintain", []])

    def play_file(self, position, filedata):
        filename, artist, album, title = \
            filedata[0], filedata[1], filedata[2], filedata[3]
        #print(f"> play_file {position}, {filename}, {artist}, {album}, {title}")
        if "add song" in self.result_storage:
            return
        status = self.music.status()
        jumpnext = False
        if not "song" in status:
            position = -1
        else:
            mpdlistpos = int(status["song"])
            if position == -2:
                position = 0
            else:
                position -= mpdlistpos
            assert position >= 0, "Unreachable"
            duration = float(status["duration"])
            elapsed = float(status["elapsed"])
            if position == 0 and (elapsed > 180 or elapsed > duration / 2):
                jumpnext = True
                position = 1
        self.comm_que.put(["add_song", [position, filedata, jumpnext]])
        self.result_storage["add_song"] = pd.DataFrame([])
    
    def delete_mpd(self):
        assert "delete_song" in self.result_storage, \
            "This should only be called if we started the deletion process"
        self.get_results_from_queue()
        #print("  - checking for returned suggestions")
        if self.result_storage["delete_song"].empty:
            #print("    no suggestion returned")
            return
        status = self.music.status()
        assert ("song" in status), "I have no idea how are we deleting anything"
        delete_this = self.result_storage.pop("delete_song")
        mpdlistpos = int(status["song"])
        mpdlistlen = int(status["playlistlength"])
        delfrom = delete_this.at[0, "delfrom"] + mpdlistpos
        if delete_this.at[0, "delto"] == -1:
            delto = mpdlistlen
        else:
            delto = delete_this.at[0, "delto"] + mpdlistpos
        #print(f"  trying to execute mpd deletion {delfrom}-{delto} ({mpdlistlen})")
        self.music.delete((delfrom, delto))
        if delete_this.at[0, "jump"] and delto - delfrom < FWDLIST:
            #print("  jumped to next song")
            self.music.next()
        elif delete_this.at[0, "jump"]:
            #print("  stopped playing")
            self.music.next()
            self.music.stop()
        self.comm_que.put(["db_maintain", []])
        if delto > delfrom and not "suggest_song" in self.result_storage:
            #print("  search suggested song because something was deleted")
            self.find_suggested_song()
        #print(f"executed mpd deletion {delfrom}-{delto} ({mpdlistlen})")
    
    def delete_command(self, delfrom, delto, jumpnext=False):
        #print(f"delete_command {delfrom}-{delto}")
        self.comm_que.put(["delete_song", [delfrom, delto, jumpnext]])
        self.result_storage["delete_song"] = pd.DataFrame([])
    
    def change_song(self, recalc):
        #print(f"> change_song called with recalc={recalc}")
        status = self.music.status()
        mpdlistlen = int(status["playlistlength"])
        if "song" in status:
            mpdlistpos = int(status["song"])
        elif not "suggest_song" in self.result_storage:
            #print("change_song: find suggested song")
            self.find_suggested_song()
            return
        else:
            return
        duration = float(status["duration"])
        elapsed = float(status["elapsed"])
        jumpnext = False
        recalc = min([recalc, mpdlistlen - 1, mpdlistpos + FWDLIST])
        if elapsed <= 180 and elapsed <= duration / 2:
            recalc = max(recalc, mpdlistpos)
        else:
            jumpnext = (recalc <= mpdlistpos)
            recalc = max(recalc, mpdlistpos + 1)
        self.delete_command(recalc - mpdlistpos, -1, jumpnext)
        
    def play_next(self, jumpto):
        #print(f"> play_next called with jumpto={jumpto}")
        status = self.music.status()
        mpdlistlen = int(status["playlistlength"])
        if "song" in status:
            mpdlistpos = int(status["song"])
        elif not "suggest_song" in self.result_storage:
            #print("play_next: find suggested song")
            self.find_suggested_song()
            return
        else:
            return
        jumpto = max(jumpto, mpdlistpos + 1)
        jumpto = min([jumpto, mpdlistlen - 1, mpdlistpos + FWDLIST])
        duration = float(status["duration"])
        elapsed = float(status["elapsed"])
        #print(pd.DataFrame(self.music.playlistinfo())[["artist", "title"]])
        if elapsed <= 180 and elapsed <= duration / 2:
            self.delete_command(0, jumpto - mpdlistpos, False)
        else:
            self.delete_command(1, jumpto - mpdlistpos,
                                (jumpto <= mpdlistpos + 1))
    
    def current_song_data(self, currentsong):
        c_artist = currentsong["artist"].replace(",", "")
        if "album" in currentsong:
            c_album = currentsong["album"].replace(",", "")
        else:
            c_album = ""
        c_title = currentsong["title"].replace(",", "")
        return c_artist, c_album, c_title
    
    def volume(self, change):
        volumelevel = int(self.music.status()["volume"])
        self.music.volume(volumelevel + change)
    
    def songlist_page_switch(self, listsize, forward=True):
        if forward:
            self.comm_que.put(["list_songs_fwd", [listsize]])
            #self.songlist_page = self.db.list_songs_fwd(self.music, listsize)
        else:
            self.comm_que.put(["list_songs_bck", [listsize]])
            #self.songlist_page = self.db.list_songs_bck(self.music, listsize)
        self.result_storage["list_songs"] = pd.DataFrame([])
    
    def ret_songl_pg_sw(self):
        assert "list_songs" in self.result_storage, \
            "This should only be called if we started listing a song page"
        self.get_results_from_queue()
        if self.result_storage["list_songs"].empty:
            return pd.DataFrame([])
        response = self.result_storage.pop("list_songs")
        return response
    
    def new_songlist(self, sort_by, min_play, max_play):
        self.comm_que.put(["new_songlist", {"sort_by": sort_by,
                                            "min_play": min_play,
                                            "max_play": max_play}])
        #self.db.new_songlist(sort_by=sort_by, min_play=min_play,
        #                     max_play=max_play)
        
    def search_artist(self, artist_string):
        self.comm_que.put(["search_artist", [artist_string]])
        #self.found_songs = self.db.search_artist(self.music, artist_string)
        self.result_storage["search_artist"] = pd.DataFrame([])
    
    def ret_search_artists(self):
        assert "search_artist" in self.result_storage, \
            "This should only be called if we started a search for artists."
        self.get_results_from_queue()
        if self.result_storage["search_artist"].empty:
            return pd.DataFrame([])
        response = self.result_storage.pop("search_artist")
        return response
    
    def song_on_playlist(self, status):
        if not "song" in status:
            return {}
        current = int(status["song"])
        pllength = int(status["playlistlength"])
        if current >= pllength:
            return {}
        return self.music.playlistinfo()[current:]
    
    def song_played(self):
        if "delete_song" in self.result_storage:
            self.delete_mpd()
        if "add_song" in self.result_storage:
            self.execute_play_file()
        if "suggest_song" in self.result_storage:
            # we are looking for a song to suggest, so let's add it if ready
            self.add_suggested_song()
        status = self.music.status()
        mpdlistlen = int(status["playlistlength"])
        if "song" in status:
            mpdlistpos = int(status["song"])
        else:
            mpdlistpos = -1
        if mpdlistpos >= mpdlistlen - FWDLIST:
            # need to add a song to the list automatically
            if status["state"] != "stop" and \
                not "suggest_song" in self.result_storage:
                # if the music is stopped, we don't look for stuff automatically                
                self.find_suggested_song()
        playlistend = self.song_on_playlist(status)        
        for songdata in playlistend:
            songdata["display"] = (songdata["pos"] + ". " + songdata["artist"] + " - " + \
                                   songdata["title"])
            if not "album" in songdata:
                songdata["album"] = ""
            songdata["status"] = ""
        if len(playlistend) == 0:
            playlistend = [{"display": "STOPPED", "album": "", "status": "",
                             "pos": "-1", "id": "-1"}]
        else:
            playlistend[0]["status"] = "(" + status["state"].upper() + ")"
        return playlistend
    
    def db_maintain(self):
        self.comm_que.put(["db_maintain", []])
        #self.db.db_maintain(self.music)
    
    def save_db(self):
        self.comm_que.put(["save_file", []])
        #self.db.save_file()
        
    def destroy(self):
        status = self.music.status()
        duration = float(status["duration"])
        elapsed = float(status["elapsed"])
        if elapsed <= 180 and elapsed <= duration / 2:
            self.delete_command(0, -1, False)
        else:
            self.delete_command(1, -1, False)
        self.save_db()
        self.comm_que.put(["quit", []])
        self.music.clear()
        self.db_wrap.join()

