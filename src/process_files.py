import json
import pandas as pd
import shutil
import os
import mailchimp_marketing as MailchimpMarketing
from . import helper_functions as hf

def from_fundraisingbox(df_input, mode=''):
    """Function to extract all relevant data from the FundraisingBox file.

    Args:
        df_input ([dataframe]): Dataframe containing the info from the raw csv file.

    Returns:
        [dataframe]: Returns a dataframe with all relevant data extracted from the raw csv FundraisingBox file.
    """
    print(f"Start from_fundraisingbox() ...")
    # Get Input Dataframe
    df_fundraising_box = df_input.copy()
    
    # Get rid of entries with no donation meta info
    df_fund_trans = df_fundraising_box.dropna(subset=['donation_meta_info']).copy()
            
    # Fill missing values with an empty string
    df_fund_trans = df_fund_trans.fillna('')
    
    # Extract information about if the user wants a newsletter
    df_fund_trans['donation_meta_info_dict'] = df_fund_trans.apply(lambda x: json.loads(x['donation_meta_info']), axis=1)
    df_fund_trans['wants_nl'] = df_fund_trans.apply(lambda x: 
        json.loads(x['donation_meta_info'])['wants_newsletter'] 
            if 'wants_newsletter' in json.loads(x['donation_meta_info'])
            else ''
        , axis=1)

    # Select all donators who want a newsletter
    df_fund_want_nl = df_fund_trans[df_fund_trans['wants_nl'] == '1'].copy()
    
    # Cast some datetypes
    # df_fund_want_nl['post_code'] = df_fund_want_nl['post_code'].astype(int)
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
    
    # DEBUG
    hf.out_for_debug(df_output, 'from_fundraisingbox', mode)
    
    return df_output


def from_twingle(df_input, mode=''):
    """Function to extract all relevant data from the twingle file.

    Args:
        df_input ([dataframe]): Dataframe containing the info from the raw csv file.

    Returns:
        [dataframe]: Returns a dataframe with all relevant data extracted from the raw csv FundraisingBox file.
    """
    print(f"Start from_twingle() ...")
    # Get Input Dataframe
    df_twingle = df_input.copy()
    
    # Get rid of entries with no donation meta info
    print(df_twingle)
    df_twingle_transf = df_twingle.dropna(subset=['newsletter']).copy()
    print(df_twingle_transf)
    
    # Fill missing values with an empty string
    df_twingle_transf = df_twingle_transf.fillna(' ')
    print(f"df after fillna{df_twingle_transf}")
    
    # rename columns
    col_map_rename = [
        ('user_email', 'email_address'),
        ('user_firstname', 'first_name'),
        ('user_lastname', 'last_name'),
        ('user_telephone', 'phone'),
        ('newsletter','wants_nl'),
        ('trx_id','donation_id'),
    ]
    for mapping in col_map_rename:
        col_twing = mapping[0]
        col_chimp = mapping[1]
        df_twingle_transf[col_chimp] = df_twingle_transf[col_twing]
    print(f"df after rename col mapping{df_twingle_transf}")

    # Select all donators who want a newsletter
    df_twingle_transf_nl = df_twingle_transf[df_twingle_transf['wants_nl'] == 1].copy()
    print(f"df after nl == 1 {df_twingle_transf_nl}")
    
    # Cast some datetypes
    df_twingle_transf_nl['user_postal_code'] = df_twingle_transf_nl['user_postal_code'].astype(str)
    df_twingle_transf_nl['user_street'] = df_twingle_transf_nl['user_street'].astype(str)
    
    # Transform columns from FundraisingBox to a Mailchimp compatible format
    print(df_twingle_transf_nl.columns)
    print(df_twingle_transf_nl)
    df_twingle_transf_nl['address_for_chimp'] = df_twingle_transf_nl.apply(
                lambda x:
                    x['user_street']+'  '
                    +x['user_city']+'  '
                    +''+'  '
                    +x['user_postal_code']+'  '
                    +x['user_country']
                , axis=1)
                
    df_twingle_transf_nl['address_for_chimp_dict'] = df_twingle_transf_nl.apply(
                lambda x: 
                    {
                    "addr1" : x['user_street'],
                    "addr2" : "",
                    "city" : x['user_city'],
                    "state": "",
                    "zip": x['user_postal_code'],
                    "country": x['user_country']
                    }
                , axis=1)

    # Find out if donator is recurring
    df_twingle_transf_nl['ist_dauerspender'] = df_twingle_transf_nl.apply(lambda x: 1 if x['recurring']==1 else 0, axis=1)
    df_twingle_transf_nl['ist_einzelspender'] = df_twingle_transf_nl.apply(lambda x: 0 if x['ist_dauerspender']==1 else 1, axis=1)
    
    # prepare outputs
    df_output = df_twingle_transf_nl
    
    # DEBUG
    hf.out_for_debug(df_output, 'from_twingle', mode)
    
    return df_output


def process_to_one_mailadress(df_input, cols_for_chimp, mode=''):
    """ETL function that aggregates all given entries and makes them unique per mail adress.
        Also adds tags with 'Einzelspender/in' or 'Dauerspender/in'.

    Args:
        df_input ([dataframe]): Cleaned dataframe from csv files.
        cols_for_chimp ([dict]): List of columns that shall be used for mailchimp export.

    Returns:
        [dataframe]: Returns the transmorfed dataframe.
    """
    print(f"Start process_to_one_mailadress() ...")
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
    
    # DEBUG
    hf.out_for_debug(df_output, 'process_to_one_mailadress', mode)

    
    return df_output


def process_to_mailchimp(df_input, col_map, out_fname='import_into_mailchimp.csv', mode=''):
    """Processed the data into a format that can be manually imported into mailchimp.
        Writes this data into a csv dile.

    Args:
        df_input ([dataframe]): The input dataframe.
        col_map ([dict]): Column mapping. Defines which columns from the input dataframe shall be shall be used
            and how they should be renamed.
        out_fname (str, optional): Name of the output file. Defaults to 'import_into_mailchimp.csv'.

    Returns:
        [dataframe]: Dataframe with mailchimp compatible form and all relevant information for creating mailchimp contacts.
    """
    print(f"Start process_to_mailchimp() ...")
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
    hf.write_file(df_mailchimp_output, out_fname, 'data/')

    # prepare outputs
    df_output = df_mailchimp_output
    
    # DEBUG
    hf.out_for_debug(df_output, 'process_to_mailchimp', mode)

    return df_output


def send_entries_to_mailchimp(df_to_mc, list_id, mc_api_key, server):
    """Function that sends all entries within a dataframe to mailchimp.

    Args:
        df_to_mc ([dataframe]): Dataframe with entries that shall be sent to mailchimp.
            Schema: Email Address,	First Name,	Last Name,	Address,	Phone,	Tags
        mc_api_key ([str]): [description]
        server ([str]): [description]
    """
    print(f"Start send_entries_to_mailchimp() ...")
    
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
        hf.create_new_entry(client, list_id, mail_adress, merged_fields, tags)
        hf.update_existing_entry(client, list_id, mail_adress, merged_fields)


def clean_up(fname, fname_processed, timest, data_path = './data/', folder_name='processed'):
    """Function to clean up after successfully processing input csv files.
        Copies the input file and the written processed version to the folder 'processed' into a time folder.

    Args:
        fname ([str]): Input file that was processed.
        fname_processed ([str]): Input file post processing, is named like input file but with a '_processed' at the end.
        data_path (str, optional): Path to the data folder. Defaults to './data/'.
        folder_name (str, optional): Name of the subfolder within the data folder that is used for archiving. 
            Defaults to 'processed'.
    """
    print(f"Start send_entries_to_maiclean_uplchimp() ...")
    path = data_path+folder_name+'/'+timest +'/'
    if not os.path.exists(path):
        os.mkdir(path)
    
    shutil.move(data_path+fname, path+fname)
    shutil.move(data_path+fname_processed, path+fname_processed)