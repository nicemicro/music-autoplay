#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sud Dec 24 22:11:00 2023

@author: nicemicro
"""

from typing import Optional, Union

import pandas as pd

import engine as e

def get_points(
    songs: pd.DataFrame,
    similarities: pd.DataFrame,
    song_id: int
) -> pd.DataFrame:
    similars_after = e.find_similar_id(
        songs,
        song_id,
        timeframe=30,
        similarities=similarities)[["id_after", "Point"]]
    similars_before = e.find_similar_id(
        songs,
        song_id,
        timeframe=-30,
        similarities=similarities)[["song_id", "Point"]]
    similars = pd.merge(
        similars_after,
        similars_before,
        how="outer",
        left_on="id_after",
        right_on="song_id")
    similars["Point_x"] = similars["Point_x"].fillna(0)
    similars["Point_y"] = similars["Point_y"].fillna(0)
    similars["Point"] = similars["Point_x"] + similars["Point_y"]
    #similars["Point"] = similars["Point"] / similars["Point"].sum()
    similars["song_id"] = similars["song_id"].fillna(similars["id_after"])
    return similars.sort_values(by="Point", ascending=False)[["song_id", "Point"]]

def initial_similars(
    songs: pd.DataFrame,
    similarities: pd.DataFrame,
    number: int = 20
) -> dict[str, pd.DataFrame]:
    counter = 0
    similars_dict: dict[str, pd.DataFrame] = {}
    while counter < number:
        similars = get_points(songs, similarities, int(songs.index[counter]))
        similars_dict[str(int(songs.index[counter]))] = similars
        counter += 1
    return similars_dict

def compare_songs(a: pd.DataFrame, b: pd.DataFrame) -> float:
    merged = pd.merge(a, b, on="song_id")
    #merged["multip"] = merged["Point_x"] * merged["Point_y"]
    merged["point_norm_x"] = merged["Point_x"] / a["Point"].sum()
    merged["point_norm_y"] = merged["Point_y"] / b["Point"].sum()
    merged["multip"] = merged["point_norm_x"] * merged["point_norm_y"]
    return merged["multip"].sum()

def matrix_songs(points: pd.DataFrame) -> pd.DataFrame:
    matrix = pd.DataFrame()
    for a in points.keys():
        new_row = {
            b: compare_songs(points[a], points[b])
            for b in points.keys()
            if b not in matrix.index and a != b
        }
        new_row[a] = 0
        matrix = pd.concat([matrix, pd.DataFrame([new_row], index=[str(a)])])
    return matrix

def find_max_matrix(matrix: pd.DataFrame) -> tuple[str, str]:
    row = matrix.idxmax()[matrix.max().idxmax()]
    column = matrix.max().idxmax()
    return (str(row), str(column))

def merge_group_data(
    a: pd.DataFrame,
    b: pd.DataFrame,
    #weight_a: int,
    #weight_b: int
) -> pd.DataFrame:
    merged = pd.merge(a, b, on="song_id", how="outer")
    merged["Point_x"] = merged["Point_x"].fillna(0)
    merged["Point_y"] = merged["Point_y"].fillna(0)
    merged["Point"] = (
    #    (merged["Point_x"] * weight_a +
    #    merged["Point_y"] * weight_b)
    #    / (weight_a + weight_b)
    #)
        merged["Point_x"] + merged["Point_y"]
    )
    return merged[["song_id", "Point"]].sort_values(by="Point", ascending=False)

def merge_group(
    points: dict[str, pd.DataFrame],
    groups: dict[str, list[str]],
    label_a: str,
    label_b: str
) -> str:
    #weight_a: int = 1
    #weight_b: int = 1
    new_label: str = f"g{label_a}-{label_b}"
    members: list[str] = []
    if label_a in groups.keys():
        #weight_a = len(groups[label_a])
        members += groups.pop(label_a)
        new_label = label_a
    else:
        members.append(label_a)
    if label_b in groups.keys():
        #weight_b = len(groups[label_b])
        members += groups.pop(label_b)
        new_label = label_b
    else:
        members.append(label_b)
    new_points: pd.DataFrame = merge_group_data(
        points.pop(label_a),
        points.pop(label_b),
        #weight_a,
        #weight_b
    )
    points[new_label] = new_points
    groups[new_label] = members
    #print(f"points: {points.keys()}")
    #print(f"groups: {groups}")
    return new_label

def replace_matrix_lines(
    points: dict[str, pd.DataFrame],
    groups: dict[str, list[str]],
    matrix: pd.DataFrame,
    label_a: str,
    label_b: str,
) -> pd.DataFrame:
    matrix = (
        matrix.drop([label_a, label_b], axis=1)
        .drop([label_a, label_b], axis=0)
    )
    new_label: str = merge_group(points, groups, label_a, label_b)
    matrix[new_label] = pd.Series(
        [compare_songs(points[new_label], points[i]) for i in matrix.index],
        index=matrix.index
    )
    matrix = pd.concat([matrix, pd.DataFrame([{}], index=[new_label])])
    return matrix

def add_song_to_matrix(
    points: dict[str, pd.DataFrame],
    matrix: pd.DataFrame,
    songs: pd.DataFrame,
    similarities: pd.DataFrame,
    song_id: int
) -> pd.DataFrame:
    points[str(song_id)] = get_points(songs, similarities, song_id)
    matrix[str(song_id)] = pd.Series(
        [compare_songs(points[str(song_id)], points[i]) for i in matrix.index],
        index=matrix.index
    )
    matrix = pd.concat([matrix, pd.DataFrame([{}], index=[str(song_id)])])
    return matrix

def find_max_restricted(
    matrix: pd.DataFrame,
    a: list[str],
    b: list[str]
) -> tuple[str, str]:
    matrix_one = matrix[a].loc[b]
    matrix_two = matrix[b].loc[a]
    max_one = matrix_one.max().max()
    if pd.isna(max_one) or matrix_two.max().max() > max_one:
        matrix = matrix_two
    else:
        matrix = matrix_one
    row = matrix.idxmax()[matrix.max().idxmax()]
    column = matrix.max().idxmax()
    return (str(row), str(column))

def split_list(
    points: dict[str, pd.DataFrame],
    groups: dict[str, list[str]],
    split_num: int = 9,
) -> tuple[list[str], list[str]]:
    a: list[str] = []
    b: list[str] = []
    for name in points.keys():
        if name not in groups.keys():
            if len(b) < len(points.keys()) - split_num:
                b.append(name)
            else:
                a.append(name)
    groups_names = list(groups.keys())
    groups_len = [len(groups[a]) for a in groups_names]
    while len(a) < split_num:
        index = groups_len.index(max(groups_len))
        a.append(groups_names.pop(index))
        groups_len.pop(index)
    while len(groups_names) > 0:
        b.append(groups_names.pop())
    return a, b

def print_groupings(groups, points, songs) -> None:
    for gid in groups.keys():
        print("grouped:")
        for sid in groups[gid]:
            print(f"    {songs.at[int(sid), 'Artist']} - {songs.at[int(sid), 'Title']}")
    print("no group:")
    for sid in points.keys():
        if sid not in groups.keys():
            print(f"    {songs.at[int(sid), 'Artist']} - {songs.at[int(sid), 'Title']}")

def create_groupings(
    songs: pd.DataFrame,
    matrix: pd.DataFrame,
    similarities: pd.DataFrame,
    points: dict[str, pd.DataFrame],
    groups: dict[str, list[str]],
    counter: int = 0,
    min_play: int = 15,
    final_groups: int = 9
) -> pd.DataFrame:
    while songs.at[int(songs.index[counter]), "Played"] >= min_play:
        row, col = find_max_matrix(matrix)
        matrix = replace_matrix_lines(points, groups, matrix, row, col)
        matrix = add_song_to_matrix(
            points, matrix, songs, similarities, int(songs.index[counter])
        )
        #print(f"{songs.at[int(songs.index[counter]), 'Artist']} - {songs.at[int(songs.index[counter]), 'Title']}")
        counter += 1
        if counter % 50 == 0:
            print(f"{counter} / {len(songs.index)}", end=" ")
            print(f"({songs.at[int(songs.index[counter]), 'Played']})")
    while len(matrix.index) > final_groups:
        a, b = split_list(points, groups, final_groups)
        row, col = find_max_restricted(matrix, a, b)
        matrix = replace_matrix_lines(points, groups, matrix, row, col)
    while len(matrix.index) < final_groups * 3 and counter < len(songs.index):
        matrix = add_song_to_matrix(
            points, matrix, songs, similarities, int(songs.index[counter])
        )
        counter += 1
    while counter < len(songs.index):
        matrix = add_song_to_matrix(
            points, matrix, songs, similarities, int(songs.index[counter])
        )
        a, b = split_list(points, groups, final_groups)
        row, col = find_max_restricted(matrix, a, b)
        matrix = replace_matrix_lines(points, groups, matrix, row, col)
        counter += 1
        if counter % 50 == 0:
            print(f"{counter} / {len(songs.index)}", end=" ")
            print(f"({songs.at[int(songs.index[counter]), 'Played']})")
    while len(matrix.index) > final_groups:
        a, b = split_list(points, groups, final_groups)
        row, col = find_max_restricted(matrix, a, b)
        if matrix[col].loc[row] == 0:
            break
        matrix = replace_matrix_lines(points, groups, matrix, row, col)
    #print_groupings(groups, points, songs)
    return matrix

if __name__ == "__main__":
    songlist, artists, albums, playlist = e.load_data()
    songs = e.summarize_songlist(songlist).sort_values(by="Played", ascending=False)
    indexlist = e.make_indexlist(songlist, songs)
    similarities = e.summarize_similars(songs=songs, indexlist=indexlist)
    groups: dict[str, list[str]] = {}
    initial_search: int = 20
    points: dict[str, pd.DataFrame] = initial_similars(
        songs, similarities, initial_search
    )
    matrix: pd.DataFrame = matrix_songs(points)
    matrix = create_groupings(
        songs, matrix, similarities, points, groups, initial_search, 15, 9)
    group_index: int = 1
    for songselection in groups.values():
        songselect_int = list(int(x) for x in songselection)
        songs.loc[songselect_int, "Group"] = group_index
        group_index += 1
