import requests

url_list = dict()
def unshorten_url(url):
    if url in url_list.keys():
        print('returning URL')
        print(url_list)
        return url_list[url]
    else:
        try:
            if url != ' ' and url:
                if not url.startswith('http'):
                    url = 'http://' + url
                r = requests.get(url, timeout = 5)
                if r.status_code == 200:
                    return r.url
            return None
        except:
            return None
