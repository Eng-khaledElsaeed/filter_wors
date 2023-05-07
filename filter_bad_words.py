from producer import ProducerThread
from consumer import ConsumerThread
from queue import Queue

class filter_bad_words(object):
    def filter_script_from_bad_words(dataFile,badWordsFileName,specify_cols,head,rows_at_chunk):
        # Set up the producer-consumer model
        input_queue = Queue()
        success_queue = Queue()
        fail_queue = Queue()
        badWordsQueue=Queue()
        timedict={
                'chunksize':int(), 
                'number of chunks':int(),
                'reading':[],
                'filtering':[],
                'writing':[]
                }
            
        producer = ProducerThread(dataFile,rows_at_chunk, input_queue,timedict,badWordsFileName,badWordsQueue,specify_cols)
        consumer = ConsumerThread(input_queue,success_queue,fail_queue,badWordsQueue,timedict,head)
        
        ## Start the threads
        producer.start()
        consumer.start()

        producer.join()
        consumer.join()







# if __name__=="__main__":
#     #specified cols in data file that will be filterd
#     specify_cols=[0, 2, 6]
#     #rar data file
#     dataFile=r"C:\Users\Khale\Downloads\filter_words\badWords_3000 record of bad words\40k.rar"
#     #bad word that will be matched with 
#     badWordsFileName=r"C:\Users\Khale\Downloads\filter_words\badWords_3000 record of bad words\badWordss.csv"
#     #indexing the number of cols 
#     head=[0,1,2]
#     #number of rows at chunk 
#     rows_at_chunk=10000

#     while(rows_at_chunk<=150000):
#         filter_script_from_badwords(dataFile,badWordsFileName,specify_cols,head,rows_at_chunk)
#         rows_at_chunk+=10000