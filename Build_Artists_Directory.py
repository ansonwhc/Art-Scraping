from multiprocessing import Pool, set_start_method, \
freeze_support, get_context, cpu_count, TimeoutError

from bs4 import BeautifulSoup as bs
from PIL import ImageFile, Image

import pandas as pd
import argparse
import requests
import logging
import lxml
import time
import re
import os
        
class ArtistExtract:
    def __init__(self, save_dir, artist_name, artist_url, 
                 return_df=True, export=True,
                 verbose=1):
        self.save_dir = save_dir
        self.artist_name = artist_name
        self.artist_url = artist_url
        self.base_url = 'https://www.wga.hu'
        
        self.return_df = return_df
        self.export = export
        
        self.verbose = verbose
        
        
    def check_save_dir(self):
        if not os.path.exists(self.save_dir):
            try:
                os.mkdir(self.save_dir)
            except:
                print('Could not create directory for saving.')
                pass
        
        
    def flatten(self, A):
        rt = []
        for i in A:
            if isinstance(i,list): 
                rt.extend(self.flatten(i))
            else: 
                rt.append(i)
        return rt


    def get_children(self, url):
        page = requests.get(url)
        soup = bs(page.content, 'html.parser')
        try:
            all_as = soup.find_all('ul')[0]
            hrefs = [a['href'] for a in all_as.find_all('a')]
            children = [self.get_children(f"{'/'.join(url.split('/')[:-1])}/{href}") for href in hrefs]
            return self.flatten(children)

        except:
            return [url] # no children


    def get_direct(self, url):
        page = requests.get(url)
        soup = bs(page.content, 'html.parser')
        child_url = None
        table = None
        right_table = [table for i, table in enumerate(soup.find_all('table')) 
                       if 'Preview' in table.find('tr').text]

        # turning line-skips into commas and taking out the titles, which are bolded
        table = pd.read_html(
            re.sub("<br>|<br/>", ", ", 
                   re.sub("</b>", "_PICTITLE_", 
                          str(right_table[0]).lower())))[0].iloc[:, 1:-1]
        
        table['title'] = 0
        table['artist'] = self.artist_name
        for col in table:
            for idx, row in enumerate(table[col]):
                if "_PICTITLE_" in row:
                    table.loc[idx, 'title'] = row.split("_PICTITLE_")[0]
                    table.loc[idx, col] = row.split("_PICTITLE_")[1]
                table.loc[idx, col] = re.sub("^, |^,|,$", "", table.loc[idx, col]).strip()
        table['jpg url'] = [f"{self.base_url}{a['href']}" for a in right_table[0].find_all('a') if a['href'].endswith(".jpg")]
        table = table[['artist', 'title', 'picture data', 'file info', 'jpg url']]
        
        return table


    def get_artist(self):
        children_url = self.get_children(self.artist_url)
        artist_name = '_'.join(re.sub("https://", "", self.artist_url).split('/')[3:-1])
        if self.verbose:
            print(f"Getting {artist_name}")
        
        df = pd.concat([self.get_direct(child_url) for child_url in children_url], axis=0)
        df = df.reset_index(drop=True)
        df = df.rename_axis('ID')

        if self.export:
            self.check_save_dir()
            name = f"{self.save_dir}/{artist_name}.csv"
            df.to_csv(name)

        if self.return_df:
            return df


class BuildArtistsDirectory:
    def __init__(self, save_dir, period='all', school='all', base='all', 
                 nationality='all', custom_name=None, n_processes=cpu_count()-1,
                 verbose=1):
        
        self.custom_name = custom_name
        if self.custom_name is not None:
            self.add = f"{self.custom_name}_"
        else:
            self.add = ''
            
        self.save_dir = save_dir
        self.save_art_dir = f"{save_dir}/{self.add}artwork"
        self.period = period
        self.school = school
        self.base = base
        self.nationality = nationality
        
        self.n_processes = n_processes
        self.verbose = verbose
        
        if not os.path.exists(self.save_art_dir):
            os.makedirs(self.save_art_dir)
        
        logging.basicConfig(filename=f"{self.save_dir}/{self.add}download_log.log", 
                            filemode='w', level=logging.INFO)
    
    
    def get_jpg(self, url, _id):
        if self.verbose:
                print(f"Getting picture ID {_id}")
        img = Image.open(requests.get(url, stream=True).raw)
        img.save(f"{self.save_art_dir}/{_id}.jpg")
        
        
    def get_artists(self):
        print('Building artists dataset...')
#         extract_instances = [
#             ArtistExtract(self.save_dir, row['artist'], row['url'], 
#                           return_df=True, export=False)
#             for index, row in self.artists_df.iterrows()]
#         with get_context('spawn').Pool(processes=cpu_count()-1) as pool:
#             artwork_df = pool.map(self.get_artist_job, extract_instances)

        with get_context('spawn').Pool(self.n_processes) as pool:
            artists_work_func = [pool.apply_async(ArtistExtract(self.save_dir, 
                                                                row['artist'], 
                                                                row['url'], 
                                                                return_df=True, 
                                                                export=False,
                                                                verbose=self.verbose).get_artist) 
                                 for index, row in self.artists_df.iterrows()]
            TIMEOUT = 20
            artwork_df = []
            for i, result in enumerate(artists_work_func):
                try:
                    return_value = result.get(TIMEOUT)
                    artwork_df.append(return_value)
                except TimeoutError:
                    name = self.artists_df['artist'].iloc[i]
                    print(f"Timeout for {name}")
                    logging.exception(f"Download unsuccessful for {name}")
                    self.artists_df = self.artists_df.drop(index=i)
                    print(f"Entry deleted from the dataset")
        
        self.artists_df.to_csv(f"{self.save_dir}/{self.add}info_dataset.csv", index=False)
        
        artwork_df = pd.concat(artwork_df, axis=0)
        artwork_df = artwork_df.sort_values(by=['artist'])
        artwork_df = artwork_df.reset_index(drop=True)
        artwork_df = artwork_df.rename_axis('ID')
        
        print('Building artwork dataset...')
        
#         with get_context('spawn').Pool(processes=cpu_count()-1) as pool:
#             pool.starmap(self.get_jpg, [(row['jpg url'], index) for index, row in artwork_df.iterrows()])

        with get_context('spawn').Pool(self.n_processes) as pool:
            get_jpg_func = [pool.apply_async(self.get_jpg, args=(row['jpg url'], index)) for index, row in artwork_df.iterrows()]
            for i, result in enumerate(get_jpg_func):
                try:
                    return_value = result.get(TIMEOUT)
                except TimeoutError:
                    print(f"Timeout for ID {i}")
                    logging.exception(f"Download unsuccessful for ID {i}")
                    artwork_df = artwork_df.drop(index=i)
                    print(f"Entry for ID {i} deleted from the dataset")
                except OSError:
                    logging.exception(f"Download unsuccessful for ID {i}")
                    artwork_df = artwork_df.drop(index=i)
                    print(f"Entry for ID {i} deleted from the dataset")
        
        artwork_df = artwork_df
        name = f"{self.save_dir}/{self.add}artwork_dataset.csv"
        artwork_df.to_csv(name)
        
        return artwork_df
        
        
    def run(self):
        start = time.time()
        self.artists_df = build_artists_dataset(period=self.period, 
                                                school=self.school,
                                                base=self.base, 
                                                nationality=self.nationality,
                                                custom_name=self.custom_name,
                                                export_to_dir=False)
        self.artwork_df = self.get_artists()
        time_used = time.time() - start
        print(f"All downloaded artworks are stored in {self.save_art_dir}")
        print(f"Time used: {int((time_used)/60)}m{time_used/60)%1*60:.0f}s")

        return self.artists_df, self.artwork_df
        
        
def get_artist_job(instance):
    artist_work = instance.get_artist()
    return artist_work


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

    
def build(save_dir, period='all', school='all', base='all', nationality='all', 
        custom_name=None, n_processes=cpu_count()-1, verbose=1):
    ImageFile.LOAD_TRUNCATED_IMAGES = True
    Build = BuildArtistsDirectory(save_dir=save_dir, period=period, school=school,
                                  base=base, nationality=nationality, 
                                  custom_name=custom_name,
                                  n_processes=n_processes,
                                  verbose=verbose)
    
    artists_df, artwork_df = Build.run()
    return artists_df, artwork_df, Build.save_art_dir
    
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description ='Scraping artworks from https://www.wga.hu')
    parser.add_argument('savedir', help='Directory to download files to.')
    parser.add_argument('--period', default='all', help="Artist's period")
    parser.add_argument('--school', default='all', help="Artist's school")
    parser.add_argument('--base', default='all', help="Artist's base")
    parser.add_argument('--nationality', default='all', help="Artist's nationality")
    parser.add_argument('--customname', default=None, help='Custom prefix for the files')
    parser.add_argument('--nprocesses', default=cpu_count()-1, 
                        help='The script uses the multiprocessing module, the argument specifies the number of processes')
    parser.add_argument('--verbose', default=1, help='Whether to print the progress or not')
    
    args = parser.parse_args()
    
    freeze_support()
    build(save_dir=args.savedir, period=args.period, school=args.school, 
          base=args.base, nationality=args.nationality, 
          custom_name=args.customname,
          n_processes=args.nprocesses,
          verbose=args.verbose)
        
