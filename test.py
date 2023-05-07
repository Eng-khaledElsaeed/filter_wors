
import pandas as pd
import re
import rarfile


def read_csv_chunks(filename):
        chunks = pd.read_csv(filename.open(filename.namelist()[0]), usecols=[0,2], iterator=True)
        for chunk in chunks:
            print(chunk)

read_csv_chunks(rarfile.RarFile(r"C:\Users\Khale\Downloads\filter_words\40k.rar"))