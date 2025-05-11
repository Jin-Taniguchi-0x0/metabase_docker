import urllib.request
import time

def check_metabase_health():
    url = 'http://metabase:3000/api/health'
    max_retries = 10
    delay = 5

    for i in range(max_retries):
        try:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req) as response:
                status_code = response.getcode()
                print(f"Status code: {status_code}")
                if status_code == 200:
                    print("Metabase health check successful.")
                    return True
                else:
                    print("Metabase health check failed with status code:", status_code)
                    return False
        except urllib.error.URLError as e:
            print(f"Attempt {i+1}/{max_retries} - Failed to reach the server: {e.reason}")
        except urllib.error.HTTPError as e:
            print(f"Attempt {i+1}/{max_retries} - Server couldn't fulfill the request. Error code: {e.code}")
        
        time.sleep(delay)
    
    print("Metabase health check failed after multiple attempts.")
    return False

if __name__ == "__main__":
    check_metabase_health()
