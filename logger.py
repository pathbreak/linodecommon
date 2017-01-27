from __future__ import print_function
import colorama as clr

clr.init()
    
def msg(msg):
    print(msg)

def success_msg(msg):
    print(clr.Style.BRIGHT + clr.Fore.GREEN + msg + clr.Style.RESET_ALL)

def warn_msg(msg):
    print(clr.Style.BRIGHT + clr.Fore.YELLOW + msg + clr.Style.RESET_ALL)

def error_msg(msg):
    print(clr.Style.BRIGHT + clr.Fore.RED + msg + clr.Style.RESET_ALL)

def heading(msg):
    success_msg(msg)
    success_msg('=' * len(msg))

    
