import os
from environs import Env
import src.process_files as pf
import src.helper_functions as hf

def main():
    # read parameters
    env = Env()
    env.read_env()  

    # export parameters
    mc_api_key = env("MAILCHIMP_API_KEY") 
    mc_server = env("SERVER") 
    list_id = env("LIST_ID") 

    # set defaults
    path = './data/'
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

    # get filenames
    fundraising_files = []
    for file_name in os.listdir(path):
        if 'FundraisingBox' in file_name: fundraising_files.append(file_name)
    
    # process files
    for fundraising_file in fundraising_files:
        fundraising_file_processed = fundraising_file[:-4]+'_processed.csv'
        df_file = hf.load_file(fundraising_file, path, ';')
        df_clean = pf.from_fundraisingbox(df_file)
        df_agg = pf.process_to_one_mailadress(df_clean, cols_for_chimp)
        df_final = pf.process_to_mailchimp(df_agg, col_map_for_chimp, fundraising_file_processed)
        pf.send_entries_to_mailchimp(df_final, list_id, mc_api_key, mc_server)
        pf.clean_up(fundraising_file, fundraising_file_processed, path)
        
    print('Done')
    
if __name__ == "__main__":
    # execute only if run as a script
    main()