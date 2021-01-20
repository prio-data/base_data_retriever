import os

def hyperlink(request,path,name):
    base = f"{request.url.scheme}://{request.url.hostname}:{request.url.port}"
    return os.path.join(base,path,name)+"/"
