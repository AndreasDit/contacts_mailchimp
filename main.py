import os
from environs import Env
import src.process_files as pf
import src.helper_functions as hf

def process_file(col_map_for_chimp, cols_for_chimp, data_path, file, ts, list_id, mc_api_key, mc_server, 
                mode, processed_suffix, what_file=''):
    """Function to combine ETL steps into one procedure.

    Args:
        col_map_for_chimp ([type]): Mapping of column renames for a mailchimp-readable output file.
        cols_for_chimp ([type]): Columns names of cols to be used from the transformded input file.
        data_path ([type]): Paht to the csv files.
        file ([type]): File that shall be processed.
        list_id ([type]): List_id of the list within mailchimp where new entries shall be made.
        mc_api_key ([type]): Mailchimp API Key. Needed for communication.
        mc_server ([type]): Mailchimp Server, first part of the URL one logged in. Needed for communication
        mode ([type]): Shall debugging take place or not? For debugging enter "DEBUG" in the .env file as MODE.
        processed_suffix ([type]): Suffix of the output file which can be manually read in by mailchimp.
        what_file (str, optional): Is this a file from FundraisingBox or from Twingle? Defaults to ''.
    """
    # define processed filename
    file_processed = file[:-4] + processed_suffix + '.csv'

    # ETL steps
    df_file = hf.load_file(file, data_path, ';')
    if what_file == 'is_FundraisingBox':
        df_clean = pf.from_fundraisingbox(df_file, mode)
    if what_file == 'is_twingle':    
        df_clean = pf.from_twingle(df_file, mode)
    df_agg = pf.process_to_one_mailadress(df_clean, cols_for_chimp, mode)
    df_final = pf.process_to_mailchimp(df_agg, col_map_for_chimp, file_processed, mode)

    # send contacts to mailchimp
    pf.send_entries_to_mailchimp(df_final, list_id, mc_api_key, mc_server)

    # Clean up
    pf.clean_up(file, file_processed, ts, data_path)



def main():
    # read parameters
    env = Env()
    env.read_env()

    # export parameters
    mc_api_key = env("MAILCHIMP_API_KEY") 
    mc_server = env("SERVER") 
    list_id = env("LIST_ID")
    mode = env("MODE")
    parse_fund=env("PARSE_FUND")
    parse_twing=env("PARSE_TWNIG")

    # set defaults
    data_path = './data/'
    cols_for_chimp = ['email_address', 'first_name', 'last_name', 'address_for_chimp', 'address_for_chimp_dict'
                    , 'phone', 'donation_id', 'ist_dauerspender', 'ist_einzelspender']
    col_map_for_chimp = [
        ('email_address', 'Email Address'),
        ('first_name', 'First Name'),
        ('last_name', 'Last Name'),
        ('address_for_chimp', 'Address'),
        ('address_for_chimp_dict', 'Address_dict'),
        ('phone', 'Phone'),
        ('spender_tag', 'Tags'),
    #     ('', 'Birthday'),
    ]
    fundraising_substr = 'FundraisingBox'
    twingle_substr = 'twingle'
    processed_suffix = '_processed'
    ts = hf.get_timestamp()

    # delete processed files
    """ If a run failes, files with the suffix _processed can remain. They cause errors in reruns.
        This functions deletes these files so that reruns can run smoothly."""
    hf.delete_files_containing(processed_suffix, data_path)
    
    # get filenames
    fundraising_files = hf.get_filenames_containing(fundraising_substr, data_path)
    twingle_files = hf.get_filenames_containing(twingle_substr, data_path)
    
    # process FundraisingBox files
    if parse_fund == "True":
        print(f"Process csv files from FundraisingBox ...")
        for fundraising_file in fundraising_files:
            print(f"Processing file {fundraising_file} ...")
            process_file(col_map_for_chimp, cols_for_chimp, data_path, fundraising_file, ts, list_id, mc_api_key,
                        mc_server,mode, processed_suffix, 'is_FundraisingBox')

    # process twingle files
    if parse_twing == "True":
        print(f"Process csv files from twingle ...")
        for twingle_file in twingle_files:
            print(f"Processing file {twingle_file} ...")
            process_file(col_map_for_chimp, cols_for_chimp, data_path, twingle_file, ts, list_id, mc_api_key,
                        mc_server,mode, processed_suffix, 'is_twingle')

    print('Done')



if __name__ == "__main__":
    # execute only if run as a script
    main()