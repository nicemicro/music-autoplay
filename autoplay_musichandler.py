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
            "renew_suggestion": self.db.renew_suggestion,
            "suggest_song": self.db.suggest_song,
            "search_artist": self.db.search_artist,
            "delete_song": self.db.delete_song,
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
    
    def change_song_now(self):
        assert "renew_suggestion" in self.result_storage, \
            "This should only be called if we started a search for suggested songs"
        self.get_results_from_queue()
        if self.result_storage["renew_suggestion"].empty:
            return False
        suggestion = self.result_storage.pop("renew_suggestion")
        self.music.add(suggestion.at[0, "file"])
        self.music.random(0)
        status = self.music.status()
        if status["state"] != "play":
            self.music.play()
        mpdlistpos = int(status["song"])
        self.music.next()
        self.music.delete(mpdlistpos)
        return True
    
    def find_suggested_song(self):
        #currentsong = self.music.currentsong()
        #if len(currentsong) == 0:
        #    return
        #c_artist, c_album, c_title = self.current_song_data(currentsong)
        #self.comm_que.put(["playlist_append", [c_artist, c_album, c_title]])
        #self.db.playlist_append(c_artist, c_album, c_title)
        assert not "suggest_song" in self.result_storage, \
            "This shouldn't be called if we already have a search going!"
        #print("Initiating search for new suggestion")
        self.comm_que.put(["suggest_song", []])
        #suggestion = self.db.suggest_song(self.music, c_artist, c_album,
        #                                  c_title)
        self.result_storage["suggest_song"] = pd.DataFrame([])
    
    def change_current_song(self):
        currentsong = self.music.currentsong()
        if len(currentsong) == 0:
            return
        c_artist, c_album, c_title = self.current_song_data(currentsong)
        self.comm_que.put(["remove_pl_current", [c_artist, c_album, c_title]])
        #self.db.remove_pl_current(c_artist, c_album, c_title)
        status = self.music.status()
        mpdlistlen = int(status["playlistlength"])
        if "song" in status:
            mpdlistpos = int(status["song"])
        else:
            mpdlistpos = -1
        if mpdlistpos <= mpdlistlen - 2:
            self.music.delete((mpdlistpos + 1, mpdlistlen))
        self.comm_que.put(["renew_suggestion", [c_artist, c_album, c_title]])
        #suggestion = self.db.renew_suggestion(self.music, c_artist, c_album,
        #                                      c_title)
        self.result_storage["renew_suggestion"] = pd.DataFrame([])
    
    def play_file(self, playnow, filename):
        # Clearing everything else from MPD's playlist
        status = self.music.status()
        pllength = int(status["playlistlength"])
        if "song" in status:
            place = int(status["song"])
        else:
            place = -1
        if place < pllength  - 1 and place != -1:
            self.music.delete((place + 1, pllength ))
        # Add the next song
        self.music.add(filename)
        self.music.random(0)
        if playnow:            
            self.play_next()
        
    def play_next(self, jumpto):
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
            self.comm_que.put(["delete_song", [0, jumpto - mpdlistpos]])
            self.music.delete((mpdlistpos, jumpto))
            #print(f"deleted {mpdlistpos} - {jumpto}")
        elif jumpto > mpdlistpos + 1:
            self.comm_que.put(["delete_song", [1, jumpto - mpdlistpos]])
            self.music.delete((mpdlistpos+1, jumpto))
            #print(f"deleted {mpdlistpos+1} - {jumpto}")
            self.music.next()
        #print(pd.DataFrame(self.music.playlistinfo())[["artist", "title"]])
        self.music.play()
    
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
        status = self.music.status()
        mpdlistlen = int(status["playlistlength"])
        if "song" in status:
            mpdlistpos = int(status["song"])
        else:
            mpdlistpos = -1
        if mpdlistpos >= mpdlistlen - FWDLIST:
            # need to add a song to the list automatically
            if "suggest_song" in self.result_storage:
                # we are looking for a song to suggest, so let's add it if ready
                self.add_suggested_song()
            elif status["state"] != "stop":
                # if the music is stopped, we don't look for stuff automatically                
                self.find_suggested_song()
        playlistend = self.song_on_playlist(status)        
        for songdata in playlistend:
            songdata["display"] = (songdata["artist"] + " - " + \
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
        #status = self.music.status()
        # TODO: remove superfluos elements from list
        #self.remove_not_played(status)
        self.save_db()
        self.comm_que.put(["quit", []])
        self.db_wrap.join()

