import re
from threading import Thread
import time
import openpyxl
from functools import reduce
import os



#cpu bounds
class ConsumerThread(Thread):
    def __init__(self, input_queue, success_queue,fail_queue,badwordsQueue,timedict,head):
        super().__init__()
        self.input_queue = input_queue
        self.success_queue = success_queue
        self.fail_queue=fail_queue
        self.badWordsQueue=badwordsQueue
        self.timedict=timedict
        self.Head=head
        self.badWords=self.badWordsQueue.get()
    


    
    def run(self):
        while True:
            if not self.input_queue.empty():
              
                chunk = self.input_queue.get()

                if chunk is None:
                    self.success_queue.put(None)
                    self.fail_queue.put(None)
                    break
            
                #filtering
                start_time=time.time()
            
                
                boolList = [ ~chunk.iloc[:,head].str.contains(self.badWords, regex = True, flags = re.I,na=False) for head in self.Head ]
                bool_checker = self.check_bool(boolList)

                healthy_df = chunk[bool_checker]
                unhealthy_df = chunk[~bool_checker]
               
                end_time=time.time()
        

                self.success_queue.put(healthy_df)
                self.fail_queue.put(unhealthy_df)
                
                self.timedict['filtering'].append(end_time-start_time)
                boolList.clear()
                
                start_time=time.time()
                self.write_csv(self.success_queue,"Healty Record")
                self.write_csv(self.fail_queue,"UnHealty Record")
                end_time=time.time()

                self.timedict['writing'].append(end_time-start_time)
             
        self.ExceLwriter()   



    #pythoinc for loop instead of 
    #x = BoolList[0]
    #   for i in BoolList:
    #      x = x & i
    def check_bool(self, BoolList):
       return reduce(lambda x, y: x & y, BoolList)
    





    def write_csv(self, queue, fileName):
        if not os.path.exists("output"):
            os.makedirs("output")
        
        
        #create folder store healty and unhealty files
        if not os.path.exists("output/FrameSize"+str(self.timedict['chunksize'])):
            os.makedirs("output/FrameSize"+str(self.timedict['chunksize']))

        fileName="output/FrameSize"+str(self.timedict['chunksize'])+"/"+fileName
        
        while True:
            if queue.empty():
                break

            record = queue.get()  
            # Check if the file already exists
            if not os.path.exists(fileName):
                # write the first chunk with header
                record.to_csv(fileName, mode='w', header=True, index=False) 

            else:    
                # append chunk without header
                record.to_csv(fileName, mode='a', header=False, index=False) 
                        

   

        
    def ExceLwriter(self):

        #measure reading total time and avg time
        total_time_of_read=sum(self.timedict['reading'])
        avg_time_of_read=total_time_of_read/self.timedict['number of chunks']

        #measure filtering total time and avg time
        total_time_of_filter=sum(self.timedict['filtering'])
        avg_time_of_filter=total_time_of_filter/self.timedict['number of chunks']
        #measure writing total time and avg time
        total_time_of_writing=sum(self.timedict['writing'])
        avg_time_of_writing=total_time_of_writing/self.timedict['number of chunks']

        #measure processing total time and avg time
        total_time_of_processing=sum(self.timedict['reading'])+sum(self.timedict['filtering'])+sum(self.timedict['writing'])
        avg_time_of_processing=total_time_of_processing/self.timedict['number of chunks']
        
        
        data = {
            "D.frame size":self.timedict['chunksize'] ,
            'avg_Reading Time': avg_time_of_read,
            'avg_filtering Time': avg_time_of_filter,
            'Total_processing_Time': total_time_of_processing,
            'avg_processing_Time': avg_time_of_processing

            }
        
        headers = list(data.keys())
           # check if the file exists
        try:
            workbook = openpyxl.load_workbook("./output/output.xlsx")
            worksheet = workbook.active
        except FileNotFoundError:
        #create a new workbook and worksheet if the file doesn't exist
            workbook = openpyxl.Workbook()
            worksheet = workbook.active
            worksheet.append(headers)
        
        # append data to the worksheet
        values = [data[header] for header in headers]
        worksheet.append(values)
            # save the workbook
        workbook.save('./output/output.xlsx')


