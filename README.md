# Art_Scraping
Scripts for scraping artworks and artists info from the Web Gallery of Art (https://www.wga.hu)  
The extracted dataset along with a notebook introduction is available on https://www.kaggle.com/ansonnnnn


## Quick start
To extract all artworks within a notebook
```
!git clone https://github.com/ansonwhc/Art_Scraping.git

from Art_Scraping.Build_Artists_Directory import build
artists_df, artwork_df, artwork_dir = build(save_dir="Art_folder")
```

Or simply run the python script
```
python Build_Artists_Directory Art_folder
```
All the extracted data will be stored in the ```Art_folder```


## Data structure
The created directory will look like below
```
Art_folder/ 
|--- artwork/              # Contains all .jpg files
|    |--- pic_ID0.jpg
|    |--- pic_ID1.jpg
|    |--- ...
|--- artwork_dataset.csv   # Contains picture details and IDs, i.e. names of the .jpg within the artwork folder
|--- info_dataset.csv      # Contains information regarding the artists
|--- download_log.log      # Contains error messages in case any unsuccessful download attempts, 
                           # e.g. Timeout Error, Corrupted Iamges
```
