from setuptools import setup, find_packages

setup(
    name='artscraping',
    version='0.1.0',
    packages=find_packages(include=['Art_Scraping', 'Art_Scraping.*'])
    install_requires=[
        'bs4',
        'PIL',
        'pandas',
        'requests',
        'lxml']
)
