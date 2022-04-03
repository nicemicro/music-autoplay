#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 15 23:25:46 2020

@author: nicemicro
"""

import engine as e
import pandas as pd
import numpy as np
import scipy.special as ss
import matplotlib.pyplot as plt
import multiprocessing as mp

timeframe = 30*60
points=[1]

def artist_pop(songlist, artists):
    """
    Calculates the number of hits a certain each artist has in the songlist.
    
    Parameters
    ----------
    songlist : Pandas DataFrame
        Contains the list of all scrobbled songs (Artist, Album, Title, Date
        added).
    artists : Pandas DataFrame
        Contains the list of all artists.

    Returns
    -------
    artist_hit : Pandas DataFrame
        A list of artists and the number of songs played from the artists.

    """
    songlist = songlist.copy()
    songlist["artist_low"] = songlist["Artist"].str.lower()
    artist_hit = songlist.groupby(["artist_low"]).agg({
        "Artist": ["max"], "Scrobble time": ["count"]}).reset_index(drop=True)
    artist_hit = artist_hit.set_axis(["Artist", "Hit"], axis=1)
    artist_hit_return = pd.merge(artists, artist_hit, how="left", on="Artist")
    return artist_hit_return[(artist_hit_return["Hit"]).notna()]

def artists_reorder(artist_hit):
    """
    Reorders the Artists DataFrame in based on the number of hits.
    
    Parameters
    ----------
    artist_hit : Pandas DataFrame
        Contains a list of artists in the column "Artist" and their hits in
        the column "Hit"

    Returns
    -------
    artists_ordered : Pandas DataFrame
        The artists and their hits in ordered from most to least hits.
    artists : pandas dataframe
        The list of artists without the hits.

    """
    artists_ordered = artist_hit.sort_values("Hit", axis=0, ascending=False
                                             ).reset_index(drop=True)
    artists_ordered = artists_ordered[(artists_ordered["Hit"].notna())]
    return artists_ordered, artists_ordered[["Artist"]]
#%%
def related_artists(songlist, artists, artist_name, points=points,
                  timeframe=timeframe):
    """
    Calculates the preferences of artists being played after the current 
    artist and orders them in the same order as the artists dataframe.

    Parameters
    ----------
    songlist : Pandas DataFrame
        Contains the list of all scrobbled songs (Artist, Album, Title, Date
        added).
    artists : Pandas DataFrame
        Contains the list of all artists.
    artist_name : string
        Name of the current artist.

    Returns
    -------
    pointlist : Pandas DataFrame
        Similarity points in matching order with the artists DataFrame.

    """
    similars = e.find_similar_artist(songlist, artist_name, timeframe, points
                                     )[["Artist", "Point"]]
    pointlist = pd.merge(artists, similars, how="left", on="Artist")[["Point"]]
    pointlist["Point"] = pointlist["Point"].fillna(0)
    return pointlist

def rel_art_parallel(queue, index, songlist, artists,
                     points=points, timeframe=timeframe):
    """
    Wrapper function for parallel execution of the search for similar artists

    Parameters
    ----------
    queue : Multiprocessing queue
        The queue used to collect the similar artists' points
    index : number
        The row number of the artist from the artists column
    songlist, artists, points, timeframe:
        same as the function related_artists

    Returns
    -------
    None.

    """
    artist_name=artists["Artist"][index]
    result = related_artists(songlist, artists, artist_name, points, timeframe)
    result = result.rename(columns={"Point": index})
    queue.put({"index": index, "res": result})

def artist_matrix(songlist, artists, index_from=0, index_to=-1, points=points,
                  timeframe=timeframe):
    """
    Calculates the preference matrix for the artists.

    Parameters
    ----------
    songlist : Pandas DataFrame
        Contains the list of all scrobbled songs (Artist, Album, Title, Date
        added).
    artists : Pandas DataFrame
        Contains the list of all artists.
    index_from : int, optional
        The starting index to calculate the preference matrix. The default is 0.
    index_to : int, optional
        The end index to calculate the preference matrix. The default is -1.

    Returns
    -------
    matrix : Pandas DataFrame 
        DESCRIPTION.

    """
    matrix = pd.DataFrame()
    indexes = artists.index[index_from:index_to]
    resqueue = mp.Queue()
    processes = {}
    for number in indexes:
        print("(", number + 1, ") ", artists.at[number, "Artist"], "...")
        processes[number] = mp.Process(target=rel_art_parallel, args=
                                       (resqueue, number, songlist, artists,
                                        points, timeframe))
        processes[number].start()
    while True:
        msg = resqueue.get()
        similars = msg["res"]
        index = msg["index"]
        print("(", index + 1, ") " + artists.at[index, "Artist"] + " done.")
        if len(matrix.index) > 0:
            matrix = matrix.join(similars)
        else:
            matrix = similars
        if matrix.shape[1] == index_to - index_from:
            break
    for proc in processes:
        processes[proc].join()
    matrix = matrix.reindex(columns=range(index_to - index_from))
    return matrix
#%%
def normailzed_matrix(sym_matrix, n_vect, n_all):
    """
    Calculates the symmetric and normalized artist martix based on the
    preference matrix

    Parameters
    ----------
    sym_matrix : Pandas DataFrame
        Contains the matrix of artist preferences with the column names and
        indexes representing the numerical value that corresponds the artists
        in the Artists dataframe. Each value is the sum of the x artist after
        y and y artist after x (hence symmetric)
    artist_hit : Pandas DataFrame
        Contains the number hits for each artist.
    cols : List
        Contains the list of artists numbers where the data comes from

    Returns
    -------
    matrix_norm : NumPy array
        The normalized and symmetric matrix for the available artists. The
        points generally mean something like this: ((likelihood of artist X
        played after artist Y) + (likelihood of artist Y played after artist
        X)) / (random chance of artist X played) / 2
    """
    hit_matrix = n_vect[:, np.newaxis] @ n_vect[:, np.newaxis].T
    matrix_norm = sym_matrix / hit_matrix * n_all
    return matrix_norm
#%%
def beta_dist(a, b, mew):
    e1 = ss.betaln(a, b)
    e2 = (a - 1) * np.log(mew)
    e3 = (b - 1) * np.log(1 - mew)
    return np.exp(e2 + e3 - e1)

#%%
def random_coordinates(artist_num, dimensions = 10):
    return np.random.normal(0, 0.5, size=(artist_num, dimensions))

def distance_calc(coordinates):
    """
    Calculates the distance matrix between points

    Parameters
    ----------
    coordinates : numpy.array
        Contains the list of coordinates.

    Returns
    -------
    numpy.array
        The distance square between all points.

    """
    coord_diff = coordinates[:,:,np.newaxis].transpose((0,2,1)) - \
        coordinates[:,:,np.newaxis].transpose((2,0,1))
    return (coord_diff ** 2).sum(axis=2)

def initialize(coords, artist_hit, n_mat, shape=10):
    n_vect = artist_hit["Hit"][n_mat.columns].to_numpy()
    n_all = artist_hit["Hit"].sum()
    n_mat_sym = (n_mat.to_numpy()[n_mat.columns, ...] + \
        (n_mat.to_numpy()[n_mat.columns, ...]).T) / \
        sum(points) / 2
    score_mat = normailzed_matrix(n_mat_sym, n_vect, n_all)
    test_ratio = score_mat * (n_vect[:,np.newaxis] / n_all).T
    # Alpha and Beta constants for each artist based on their ratio amongst all
    # played songs
    alpha_b = (1 + (n_vect / n_all) * shape)
    beta_b = (1 + (1 - n_vect / n_all) * shape)
    # Alpha and Beta constants for each artist (row) played as a function of
    # the other artist (column) being played
    alpha = alpha_b[:,np.newaxis] + n_mat_sym
    beta = (beta_b + n_vect)[:,np.newaxis] - n_mat_sym
    # Y value is the actual chance (ratio) of the artist in the row being
    # played if the song in the column was played
    y_target = (alpha + 1) / (alpha + beta - 2)
    # beta distribution needs to be converted to the distance base to be able
    # to calculate the weights properlyNiceMicro
    
    # Weight is the height of the peak of the beta distribution
    weight = beta_dist(alpha, beta, y_target)
    # Calculated based on the coordinates
    y_real = (n_vect[:np.newaxis] / n_all).T / (distance_calc(coords) + 
                                                np.identity(len(n_vect)))
    y_delta = (y_target - y_real) ** 2 * weight * \
        (1 - np.identity(len(n_vect)))

#%%
def pca(coordinates):
    n, m = coordinates.shape
    Z = coordinates - coordinates.mean()
    Z = Z / Z.std()
    C = np.dot(Z.T, Z) / (n-1)
    eigenvalues, P = np.linalg.eig(C)
    P = np.real(P)
    eigenvalues = np.real(eigenvalues)
    Z_new = np.dot(Z, P) * coordinates.std()
    return Z_new, eigenvalues, P

def scree_plot(eigenvalues):
    #1. Calculate the proportion of variance explained by each feature
    sum_eigenvalues = np.sum(eigenvalues)
    
    prop_var = [i/sum_eigenvalues for i in eigenvalues]
    
    #2. Calculate the cumulative variance
    cum_var = [np.sum(prop_var[:i+1]) for i in range(len(prop_var))]
        
    # Plot scree plot from PCA 
    x_labels = ['PC{}'.format(i+1) for i in range(len(prop_var))]
    
    plt.plot(x_labels, prop_var, marker='o', markersize=6, color='skyblue', linewidth=2, label='Proportion of variance')
    plt.plot(x_labels, cum_var, marker='o', color='orange', linewidth=2, label="Cumulative variance")
    plt.legend()
    plt.title('Scree plot')
    plt.xlabel('Principal components')
    plt.ylabel('Proportion of variance')
    plt.show()


#%%
if __name__ == '__main__':
    print("Recommendation engine for the playlist generator program.")
    print("L: load files")
    print("I: initialize")
    cmd = input("Enter command. ")
    if cmd in ("l", "L"):
        fname = input("Loading files. Enter file name (default: data.pckl): ")
        if fname == '':
            songlist, artists, albums, playlist = e.load_data()
        else:
            songlist, artists, albums, playlist = e.load_data(fname)
        print("Loaded.")
        artist_hit = artist_pop(songlist, artists)
        artist_num = 20
        n_mat = artist_matrix(songlist, artists, 0, artist_num)
        coords = random_coordinates(artist_num, artist_num)
    elif cmd in ("i", "I"):
        initialize(coords, artist_hit, n_mat)
    print("")
    print("Bye")
