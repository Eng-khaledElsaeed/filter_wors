# uri A variable is Pyro4 In your own way filter_bad_words Object generated uri , 
# this id Equivalent to the name of the service, but you can also specify a more understandable service name.
# Here is the client side client.py :
 
import Pyro4


uri=input(" Pyro uri : ").strip()
name=input("Your name: ").strip()
filter_bad_words=Pyro4.Proxy(uri)        
filter_bad_words.ilter_script_from_bad_words()