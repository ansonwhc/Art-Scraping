import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
import lxml
import re
import os


def data_selection(df, col, *selections):
    if (selections == ()) | (selections == ('prompt',)):
        if len(set(df[col])) > 20:
            print(f"\nPossible selections in {col} include {sorted(list(set(df[col])))[:10]}\n",
                  "For all possible selections, please refer to https://www.wga.hu/cgi-bin/artist.cgi")

        else:    
            print(f"\nAll possible selections in {col}:", sorted(list(set(df[col]))), '\n')

        selections = input("Selction(s):")
        if selections == '':
            print("\nNo selection specified, returning original dataframe.")
            return df
    else:
        selections = str(tuple(selections))
        
    selected_df = [df[df[col].str.lower()==selection.lower()] 
                   for selection in [re.sub("[ '()]", "", s) for s in re.split(',', selections)]]
    return pd.concat(selected_df, axis=0)


def build_artists_dataset(save_dir=None, export_to_dir=True, 
                          period='all', school='all',
                          base='all', nationality='all',
                          custom_name=None):
    """
    For period, school, base, and nationality, (artists not implemented)
    'all': return all types, 
    str: return one group, 
    list: return multiple groups, 
    'prompt': if for selection prompts
    """
    if save_dir is None: 
        assert (not export_to_dir), print('No directory specified to export the file to.')
    
    if not save_dir is None: 
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)
        
    artists_url = 'https://www.wga.hu/cgi-bin/artist.cgi?Profession=any&School=any&Period=any&Time-line=any&from=0&max=9999999&Sort=Name'
    artists_page = requests.get(artists_url)

    print(f"Status code to www.wga.hu: {artists_page.status_code}")
    
    artists_soup = bs(artists_page.content, 'html.parser')
    artists_table_soup = artists_soup.find('div', 'PAGENUM').table

    # first column is the First letter of the last names
    artists_df = pd.read_html(str(artists_table_soup), header=0)[0].iloc[:, 1:] 

    # add and process dataframe information
    artists_df['URL'] = [a['href'] for a in artists_table_soup.find_all(name='a', href=True)]

    schools = [str(s).split(' (') for s in artists_df.SCHOOL]

    base_ = [re.sub("[ ()]", '', s.pop(-1)) if len(s)>1 else re.sub("[ ()]", '', s[0].split(' ')[0]) for s in schools]

    # convert demonyms to places
    country_url = 'https://github.com/knowitall/chunkedextractor/blob/master/src/main/resources/edu/knowitall/chunkedextractor/demonyms.csv'
    country_page = requests.get(country_url)
    print(f"Status code to github.com/knowitall: {country_page.status_code}")
    country_soup = bs(country_page.content, 'html.parser')
    country_df = pd.read_html(str(country_soup))[0].iloc[:,1:]
    first_row = pd.DataFrame(country_df.columns, index=['Demonym', 'Place']).T
    country_df.columns = ['Demonym', 'Place']
    country_df = pd.concat([first_row, country_df], axis=0)
    base_ = [country_df.Place[country_df['Demonym'].str.lower() == b.lower()].iloc[0] 
            if any(country_df['Demonym'].str.lower() == b.lower()) 
            else b for b in base_]

    schools = [str(s[0]).split(' ') for s in schools]
    nationality_ = [s.pop(0) for s in schools]
    schools = [s[0] for s in schools]

    artists_df['BASE'] = base_
    artists_df['NATIONALITY'] = nationality_
    artists_df['SCHOOL'] = schools

    # to keep the ARTIST column clean
    artists_df = artists_df.drop(index = artists_df[['(see ' in s for s in artists_df.ARTIST]].index) 

    # only keep first of the rest of the duplicates (LOIR, Luigi) & (BURNE-JONES, Edward)
    artists_df = artists_df.drop(index=artists_df.URL[artists_df.URL.duplicated(keep='first')].index)
    assert len(set(artists_df.URL)) == len(artists_df), print("urls and artists length mismatch")

    # data selection
    for col, requirement in zip(['PERIOD','SCHOOL','BASE','NATIONALITY'], 
                                 [period, school, base, nationality]):
        if isinstance(requirement, str):
            requirement = [requirement]
        if requirement != ['all']:
            artists_df = data_selection(artists_df, col, *requirement)
    
    artists_df = artists_df.sort_values(by=['ARTIST'])
    artists_df = artists_df.reset_index(drop=True)
    artists_df.columns = artists_df.columns.str.lower()
    
    if export_to_dir:
        if custom_name is not None:
            add = f"{custom_name}_"
        else:
            add = ''
        artists_df.to_csv(f"{save_dir}/{add}info_dataset.csv", index=False)

    return artists_df