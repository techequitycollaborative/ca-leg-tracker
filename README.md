# ca-leg-tracker
## Credentials
Locally configure a `credentials.ini` file. For example:
```
[openstates]
api_key = **********

[postgresql]
user = *********
password = *********
host = *********.db.ondigitalocean.com
port = *********
dbname = *********
sslmode = require
```

This will be parsed by `config.py` which enables the pSQL and OpenStates API.  
