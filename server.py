# to satrt use remote invocation (RPC RMI)
import Pyro4

from filter_bad_words import filter_bad_words


#get opject of filter class code 
filter_script=filter_bad_words()

# define daemon of pyro4
daemon = Pyro4.Daemon()                # make a Pyro daemon
uri = daemon.register(filter_script)   # register the filter_script object



if __name__=="__main__":
    
    #specified cols in data file that will be filterd
    specify_cols=[0, 2, 6]
    #rar data file
    dataFile=r"C:\Users\Khale\Downloads\filter_words\badWords_3000 record of bad words\40k.rar"
    #bad word that will be matched with 
    badWordsFileName=r"C:\Users\Khale\Downloads\filter_words\badWords_3000 record of bad words\badWordss.csv"
    #indexing the number of cols 
    head=[0,1,2]
    #number of rows at chunk 
    rows_at_chunk=10000

    option=input("do you want use RMI? y/n")
    if(option=="y" or option.capitalize()=="Y"):
        print("Use RPC-RMI")
        print('Filter script uri: ', uri)    # print the uri
        while(rows_at_chunk<=150000):
            daemon.requestLoop()  # start the event loop of the server to wait for calls
            rows_at_chunk+=10000
    else:
        print("Use all server resource")
        while(rows_at_chunk<=150000):
            filter_script.filter_script_from_bad_words(dataFile,badWordsFileName,specify_cols,head,rows_at_chunk)
            rows_at_chunk+=10000 

    