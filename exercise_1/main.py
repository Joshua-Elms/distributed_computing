import subprocess
import sys
import time

def main(interactive=False, default_timeout="60"):
    """
    Start a server and at least one client process.
    
    interactive: bool - if True, start one client in interactive mode (doesn't use subprocess). Multi-client mode requires interactive=False.
    default_timeout: int - default timeout (s) for server to wait for client connection, must be non-negative.
    """
    try: 
        str_timeout = sys.argv[1]
    
    except IndexError as e:
        str_timeout = default_timeout

    try:
        float(str_timeout)
        
    except TypeError as e:
        print(f"Timeout (s) must be numeric, you passed: '{str_timeout}'")
        quit()
        
    print(repr(str_timeout))
    subprocess.Popen(["python", "server.py", str_timeout])
    time.sleep(0.5)
    
    if interactive:
        import client
        
    else:
        subprocess.Popen(["python", "client.py", "0"]) # 0 for non-interactive mode
    
    
    
    
if __name__ == "__main__":
    main(
        interactive=False,
        default_timeout="15"
    )