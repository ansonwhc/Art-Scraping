from multiprocessing import Pool, set_start_method, \
freeze_support, get_context, cpu_count, TimeoutError
from Build_Artists_Dataset import build_artists_dataset
from Artist_Extract import ArtistExtract
from bs4 import BeautifulSoup as bs
from PIL import ImageFile
from PIL import Image
import pandas as pd
import argparse
import requests
import logging
import lxml
import time
import os
        
        
class BuildArtistsDirectory:
    def __init__(self, save_dir, period='all', school='all', base='all', 
                 nationality='all', custom_name=None, n_processes=cpu_count()-1):
        
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
        
        if not os.path.exists(self.save_art_dir):
            os.makedirs(self.save_art_dir)
        
        logging.basicConfig(filename=f"{self.save_dir}/{self.add}download_log.log", 
                            filemode='w', level=logging.INFO)
    
    
    def get_jpg(self, url, _id):
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
                                                           export=False).get_artist) 
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
        end = time.time()
        print(f"All downloaded artworks are stored in {self.save_art_dir}")
        print(f"Time used: {(end-start)/60:.2f}m")

        return self.artists_df, self.artwork_df
        
        
def get_artist_job(instance):
    artist_work = instance.get_artist()
    return artist_work

    
def main(save_dir, period='all', school='all', base='all', nationality='all', 
         custom_name=None, n_processes=cpu_count()-1):
    ImageFile.LOAD_TRUNCATED_IMAGES = True
    Build = BuildArtistsDirectory(save_dir=save_dir, period=period, school=school,
                                  base=base, nationality=nationality, 
                                  custom_name=custom_name,
                                  n_processes=n_processes)
    
    artists_df, artwork_df = Build.run()
    return artists_df, artwork_df, Build.save_art_dir
    
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description ='Scraping artworks from https://www.wga.hu')
    parser.add_argument('savedir', help='Directory to download files to.')
    parser.add_argument('--period', default='all', help='')
    parser.add_argument('--school', default='all', help='')
    parser.add_argument('--base', default='all', help='')
    parser.add_argument('--nationality', default='all', help='')
    parser.add_argument('--customname', default=None, help='')
    parser.add_argument('--nprocesses', default=cpu_count()-1, help='')
    
    args = parser.parse_args()
    
    freeze_support()
    main(save_dir=args.savedir, period=args.period, school=args.school, 
         base=args.base, nationality=args.nationality, 
         custom_name=args.customname,
         n_processes=args.nprocesses)
        