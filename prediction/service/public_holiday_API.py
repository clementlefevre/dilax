def get_public_holidays(country,year):
    BASE_URL = "https://api.xmltime.com/holidays?"
    accesskey= "accesskey=rtdGNBuF1E"    
    secretkey = "secretkey=K6WiKnRcziiz5H2Kcgw9"
    version = "version=2"    
    types = "types=federal"
    country = "country="+country
    year = "year="+year
    lang ="lang=fr"

    url = BASE_URL + accesskey +"&"+ secretkey +"&"+version + "&"+ types+ "&"+country + "&"+year+ "&"+lang
    print url
    request = urllib2.urlopen(url)
    data = json.loads(request.read())

    return data