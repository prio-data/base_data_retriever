
from datetime import date
import os
import requests

import settings

def cast_date_to_mid(date:date)->int:
    r = requests.get(os.path.join(settings.TIMECASTER_URL,"vmid",str(date)))
    if r.status_code == 200:
        return int(r.content)
    else:
        raise requests.exceptions.HTTPError(r.status_code)
