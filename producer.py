import pandas as pd
from threading import Thread
import time
import re
import rarfile
 
#i/o bounds
class ProducerThread(Thread):
    def __init__(self, filename, chunksize, input_queue,timedict,badwords_filename ,badWordsQueue,specify_cols):
        super().__init__()
        self.filename = rarfile.RarFile(filename)
        self.chunksize = chunksize
        self.input_queue = input_queue
        self.timedict=timedict
        self.patterns = pd.read_csv(badwords_filename).values.tolist()
        self.badWords =('|'.join(re.escape(x[0]) for x in self.patterns))
        self.badWordsQueue=badWordsQueue
        self.badWordsQueue.put(self.badWords)
        self.specify_cols=specify_cols
        self.num_chunks =0
        #assign the chunksize value to timedict 
        self.timedict['chunksize']=self.chunksize
    
        
        #using yiled

    def read_csv_chunks(self,filename, chunksize):
        chunks = pd.read_csv(filename.open(filename.namelist()[0]), usecols=self.specify_cols , chunksize=chunksize, iterator=True)
        for chunk in chunks:
            yield chunk

    def run(self):

        # Create generator for chunks of data
        chunks = self.read_csv_chunks(self.filename,self.chunksize)
        
        start_time = time.time()
        # Process chunks of data until there are no more chunks
        for chunk in chunks:
            
            end_time=time.time()
           
            self.input_queue.put(chunk)
            self.timedict['reading'].append(end_time-start_time)
            self.num_chunks += 1
            start_time = time.time()
    
        #send number of chunks in dict that has time statistics as well
        self.timedict['number of chunks']=self.num_chunks
        
        # Signal end of input and compute read time statistics
        self.input_queue.put(None)















