import csv
import json
import pandas as pd
import seaborn as sns
import urllib
import urllib.request
import matplotlib.pyplot as plt
import logging
import urllib.request
# import chimpy
import shutil
import os
import hashlib
from datetime import datetime
import mailchimp_marketing as MailchimpMarketing
from mailchimp_marketing.api_client import ApiClientError
from environs import Env


def load_file(fname, fpath='./', delimiter=','):
    """Helper function to load files

    Args:
        fpath : Path to the csv-files.
        delimiter (str, optional): Delimiter for csv file. for Defaults to ';'.
    """

    dest = fpath + fname
    print(f"Loading file {dest} ...")
    df_file = pd.read_csv(dest, delimiter=delimiter)

    return df_file


def write_file(df_input, fname, fpath='./', delimiter=',', index=False, ftype='csv'):
    """Helper function to write files.

    Args:
        modus ([str]): Format in which the output shall be written. Possible values: csv, pkl.
        fname ([str]): Name of the output file.
        fpath (str, optional): Path of the output file. Defaults to './'.
        delimiter (str, optional): Seperator to be used in csv file. Defaults to ','.
        index (bool, optional): Whether ot not the index shall be exported in a seperated cxolu. 
            Defaults to False.
    """

    dest = fpath + fname
    if ftype == 'csv':
        print(f"Writing file {dest} ...")
        df_input.to_csv(dest, sep=delimiter, index=index)
    elif ftype == 'pkl':
        dest = dest.replace("csv", "pkl")
        print(f"Writing file {dest} ...")
        df_input.to_pickle(dest)


def out_for_debug(df_input, fname, modus=''):
    """Wrapper for function "write_file". Will only be executed if mode=DEBUG and writes a pkl file to the debug folder.

    Args:
        df_input ([dateframe]): Dataframe that shall be written into a file for debugging purposes.
        fname ([str]): Name of the dataframe for debugging.
        modus ([str], optional): Indicates in what mode this function shall be executed. Defaults to MODE from the .env file.
            If MODE is set to DEBUG in the .env file, outputs for debugging are written.
    """
    
    print(f"modus is: {modus}")
    if modus == 'DEBUG':
        fname = fname+'.pkl'
        print(f"DEBUG: Writing file {fname} to the debug folder ...")
        write_file(df_input, fname, 'debug/', ftype='pkl')


def get_mailchimp_lists(mc_api_key, server):
    """Helper Functino to determine, what the right id for the supposed list is.

    Args:
        mc_api_key ([str]): Api Key from mailchimp.com. Needed to interact with the API.
        server ([str]): Shorthand of the used server. The first part of the URL visible in the browser once logged in.
    """
    try:
        client = MailchimpMarketing.Client()
        client.set_config({
            "api_key": mc_api_key,
            "server": server
        })

        response = client.lists.get_all_lists()
        print(response)
    except ApiClientError as error:
        print("Error: {}".format(error.text))


def get_filenames_containing(substr, path='.'):
    """Helper functions to get a list of files in a specific path containing a specific substring.

    Args:
        substr ([str]): Substring that the files must contain.
        path ([str]): Path where filenames are analyzed.

    Returns:
        [list]: A list of filenames containing a given substring.
    """
    files = []
    for file_name in os.listdir(path):
        if substr in file_name: files.append(file_name)

    return files


def delete_files_containing(substr, path):
    """Deletes files containing the given substr within the given path.

    Args:
        substr ([str]): Substring used to idendify the files that shall be deleted.
        path ([str]): Path where files shall be deleted.
    """
    to_be_deleted = get_filenames_containing(substr, path)
    for delete_me_fname in to_be_deleted:
        os.remove(path + delete_me_fname)
        print(f"File {delete_me_fname} was deleted.")


def create_new_entry(client, list_id, mail_addr, merge_fields, tags):
    """Tries to create a new entry in the given list in mailchimp.

    Args:
        client: Client object from mailchimp. Needed for communication with the service.
        list_id ([str]): ID of the list where the entry should be added.
        mail_addr ([str]): mail adress of the entry. Used as the primary identifier.
        merge_fields ([dict]): A dictionary contain information for additional fields.
        tags ([list]): Contains a list of tags that will be added.
    """
    try:
        response = client.lists.add_list_member(list_id,
                                                {"email_address": mail_addr, "status": "subscribed", "tags": tags,
                                                 "merge_fields": merge_fields})
        print(response)
    except ApiClientError as error:
        print("Error on mail address {}: {}".format(mail_addr, error.text))


def hash_string(input_str):
    """Helper function to hash a string.

    Args:
        input_str ([str]): Input string that shall be hashed.

    Returns:
        [str]: MD5 hash of the input string.
    """
    input_b = str.encode(input_str)
    input_hash = hashlib.md5(input_b.lower())
    input_hash_str = input_hash.hexdigest()

    return input_hash_str


def update_existing_entry(client, list_id, mail_addr, merge_fields, l_tags):
    """Tries to update an existing entry.

    Args:
        client: Client object from mailchimp. Needed for communication with the service.
        list_id ([str]): ID of the list where the entry should be added.
        mail_addr ([str]): mail adress of the entry. Used as the primary identifier.
        merge_fields ([dict]): A dictionary contain information for additional fields.
        tags ([list]): Contains a list of tags that will be added.
    """
    # hash mail address        
    mail_h = hash_string(mail_addr)
    # send entry
    try:
        response = client.lists.set_list_member(list_id, mail_h,
                                                {"email_address": mail_addr, "status_if_new": "subscribed",
                                                 "status": "subscribed", "merge_fields": merge_fields})
        print(response)
    except ApiClientError as error:
        print("Error on mail address {}: {}".format(mail_addr, error.text))
    for tag in l_tags:
        try:
            response = client.lists.update_list_member_tags(list_id, mail_h, 
                                                    {"tags": [{"name": tag, "status": "active"}]})
            print(response)
        except ApiClientError as error:
            print("Error on updating tag '{}' for mail address {}: {}".format(tag, mail_addr, error.text))




def get_timestamp(format_str="%Y%m%d_%H-%M-%S"):
    """Helper function that generates a timestamp string in the default format "%Y%M%d_%H-%M-%S".

    Args:
        format_str (str, optional): Can be given to get timestamp in certain format. Defaults to "%Y%M%d_%H-%M-%S".

    Returns:
        [str]: String containing the timestamp in the given format.
    """
    now = datetime.now()
    current_timestamp = now.strftime(format_str)

    return current_timestamp
