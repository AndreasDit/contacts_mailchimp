import csv
import json
import pandas as pd
import seaborn as sns
import urllib
import urllib.request
import matplotlib.pyplot as plt
import logging
import urllib.request
import chimpy
import shutil
import os
import hashlib
from datetime import datetime
import mailchimp_marketing as MailchimpMarketing
from mailchimp_marketing.api_client import ApiClientError

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


def write_file(df_input, fname, fpath='./', delimiter=',', index=False):
    """Helper function to write files.

    Args:
        fname ([str]): Name of the output file.
        fpath (str, optional): Path of the output file. Defaults to './'.
        delimiter (str, optional): Seperator to be used in csv file. Defaults to ','.
        index (bool, optional): Whether ot not the index shall be exported in a seperated cxolu. 
            Defaults to False.
    """
    
    dest = fpath + fname
    print(f"Writing file {dest} ...")
    df_input.to_csv(dest,sep=delimiter, index=index)



def from_fundraisingbox(df_input):
    """Function to extract all relevant data from the FundraisingBox file.

    Args:
        df_input ([dataframe]): Dataframe containing the info from the raw csv file.

    Returns:
        [dataframe]: Returns a dataframe with all relevant data extracted from the raw csv FundraisingBox file.
    """
    # Get Input Dataframe
    df_fundraising_box = df_input.copy()
    
    # Get rid of entries with no donation meta info
    df_fund_trans = df_fundraising_box.dropna(subset=['donation_meta_info']).copy()
    
    # Fill missing values with an empty string
    df_fund_trans = df_fund_trans.fillna('')
    
    # Extract information about if the user wants a newsletter
    df_fund_trans['donation_meta_info_dict'] = df_fund_trans.apply(lambda x: json.loads(x['donation_meta_info']), axis=1)
    df_fund_trans['wants_nl'] = df_fund_trans.apply(lambda x: json.loads(x['donation_meta_info'])['wants_newsletter'], axis=1)

    # Select all donators who want a newsletter
    df_fund_want_nl = df_fund_trans[df_fund_trans['wants_nl'] == '1'].copy()
    
    # Cast some datetypes
    df_fund_want_nl['post_code'] = df_fund_want_nl['post_code'].astype(int)
    df_fund_want_nl['post_code'] = df_fund_want_nl['post_code'].astype(str)
    df_fund_want_nl['state'] = df_fund_want_nl['state'].astype(str)
    
    # Transform columns from FundraisingBox to a Mailchimp compatible format
    df_fund_want_nl['address_for_chimp'] = df_fund_want_nl.apply(
                lambda x: x['address']+'  '
                +x['city']+'  '
                +x['state']+'  '
                +x['post_code']+'  '
                +x['country']
                , axis=1)
                
    df_fund_want_nl['address_for_chimp_dict'] = df_fund_want_nl.apply(
                lambda x: 
                {
                "addr1" : x['address'],
                "addr2" : "",
                "city" : x['city'],
                "state": x['state'],
                "zip": x['post_code'],
                "country": x['country']
                }
                , axis=1)

    # Find out if donator is recurring
    df_fund_want_nl['ist_dauerspender'] = df_fund_want_nl.apply(lambda x: 1 if x['by_recurring']==1 else 0, axis=1)
    df_fund_want_nl['ist_einzelspender'] = df_fund_want_nl.apply(lambda x: 0 if x['ist_dauerspender']==1 else 1, axis=1)
    
    # prepare outputs
    df_output = df_fund_want_nl
    
    return df_output


def process_to_one_mailadress(df_input, cols_for_chimp):
    """ETL function that aggregates all given entries and makes them unique per mail adress.
        Also adds tags with 'Einzelspender/in' or 'Dauerspender/in'.

    Args:
        df_input ([dataframe]): Cleaned dataframe from csv files.
        cols_for_chimp ([dict]): List of columns that shall be used for mailchimp export.

    Returns:
        [dataframe]: Returns the transmorfed dataframe.
    """
    # Get Input Dataframe 
    df_for_chimp = df_input[cols_for_chimp].copy()
    
    # Reduziere auf einen Eintrag pro e-mail Adresse
    df_for_chimp_agg = df_for_chimp.groupby('email_address').agg({
    'donation_id': ['min','max'], 'ist_dauerspender': 'sum', 'ist_einzelspender':'sum'})
    df_for_chimp_agg.columns = ["_".join(x) for x in df_for_chimp_agg.columns.ravel()]
    df_for_chimp_agg = df_for_chimp_agg.reset_index()
    
    # Fuege Tag hinzu, ob Dauerspender, Einzelspender oder beides 
    df_for_chimp_agg['spender_tag'] = df_for_chimp_agg.apply(lambda x: 
            ['Einzelspender/in'] if ((x['ist_dauerspender_sum'] == 0) & (x['ist_einzelspender_sum'] > 0)) else
                (['Dauerspender/in'] if ((x['ist_dauerspender_sum'] > 0) & (x['ist_einzelspender_sum'] == 0)) else
                    (['Einzelspender/in', 'Dauerspender/in'] if ((x['ist_dauerspender_sum'] > 0) & (x['ist_einzelspender_sum'] > 0)) else '' ) )
            , axis=1)
    
    # Combine Data
    df_for_chimp_out = pd.merge(df_for_chimp_agg, df_for_chimp
                            ,  how='left'
                            , left_on=['email_address','donation_id_max']
                            , right_on = ['email_address','donation_id'])
    
    # perpare outputs
    df_output = df_for_chimp_out
    
    return df_output


def process_to_mailchimp(df_input, col_map, out_fname='import_into_mailchimp.csv'):
    # Get Input Dataframe 
    df_mailchimp_wip = df_input.copy()

    # get output columns
    output_cols = []
    for mapping in col_map:
        col_fund = mapping[0]
        col_chimp = mapping[1]
        output_cols.append(col_chimp)
        df_mailchimp_wip[col_chimp] = df_mailchimp_wip[col_fund]
    
    # Use df with output cols
    df_mailchimp_output = df_mailchimp_wip[output_cols].copy()
    
    # write results
    write_file(df_mailchimp_output, out_fname, 'data/')

    # prepare outputs
    df_output = df_mailchimp_output
    
    return df_output


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


def send_entries_to_mailchimp(df_to_mc, list_id, mc_api_key, server):
    """Function that sends all entries within a dataframe to mailchimp.

    Args:
        df_to_mc ([dataframe]): Dataframe with entries that shall be sent to mailchimp.
            Schema: Email Address,	First Name,	Last Name,	Address,	Phone,	Tags
        mc_api_key ([str]): [description]
        server ([str]): [description]
    """
    
    for index, row in df_to_mc.iterrows():
        # Get info from entry
        mail_adress = row[0]
        first_name = row[1]
        last_name = row[2]
        address = row[3]
        address_dict = row[4]
        phone = row[5]
        tags = row[6]

        # configure mailchimp client
        client = MailchimpMarketing.Client()
        client.set_config({
            "api_key": mc_api_key,
            "server": server
        })
        
        # mapping object for api to parse input data from the entry
        merged_fields = {"FNAME":first_name, "LNAME":last_name, "ADDRESS":address_dict, "PHONE":phone}
        print(f"Infos zur Mail adresse: {merged_fields}")

        # send data to mailchimp
        create_new_entry(client, list_id, mail_adress, merged_fields, tags)
        update_existing_entry(client, list_id, mail_adress, merged_fields)


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
        response = client.lists.add_list_member(list_id, {"email_address": mail_addr, "status": "subscribed", "tags":tags, "merge_fields":merge_fields})
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


def update_existing_entry(client, list_id, mail_addr, merge_fields):
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
        response = client.lists.set_list_member(list_id, mail_h, {"email_address": mail_addr, "status_if_new": "subscribed", "merge_fields":merge_fields})
        print(response)
    except ApiClientError as error:
        print("Erroron mail address {}: {}".format(mail_addr, error.text))


def get_timestamp(format_str="%Y%M%d_%H-%M-%S"):
    """Helper function that generates a timestamp string in the default format "%Y%M%d_%H-%M-%S".

    Args:
        format_str (str, optional): Can be given to get timestamp in certain format. Defaults to "%Y%M%d_%H-%M-%S".

    Returns:
        [str]: String containing the timestamp in the given format.
    """
    now = datetime.now()
    current_timestamp = now.strftime(format_str)
    
    return current_timestamp


def clean_up(fname, fname_processed, data_path = './data/', folder_name='processed'):
    """Helper Function to clean up after successfully processing input csv files.
        Copies the input file and the written processed version to the folder 'processed' into a time folder.

    Args:
        fname ([str]): Input file that was processed.
        fname_processed ([str]): Input file post processing, is named like input file but with a '_processed' at the end.
        data_path (str, optional): Path to the data folder. Defaults to './data/'.
        folder_name (str, optional): Name of the subfolder within the data folder that is used for archiving. 
            Defaults to 'processed'.
    """
    ts = get_timestamp()
    path = data_path+folder_name+'/'+ts +'/'
    print(path)
    print(os.listdir())
    print(os.listdir())
    os.mkdir(path)
    
    shutil.move(data_path+fname, path+fname)
    shutil.move(data_path+fname_processed, path+fname_processed)