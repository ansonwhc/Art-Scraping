import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
import lxml
import re
import os

class ArtistExtract:
    def __init__(self, save_dir, artist_name, artist_url, 
                 return_df=True, export=True):
        self.save_dir = save_dir
        self.artist_name = artist_name
        self.artist_url = artist_url
        self.base_url = 'https://www.wga.hu'
        
        self.return_df = return_df
        self.export = export
        
        
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