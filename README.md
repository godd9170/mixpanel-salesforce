# mixpanel-salesforce

###Sensitive Information
For API Keys, Secrets, Passwords and Tokens we rely on the user's environment variables. To open up your `~/.bashrc` and add the following lines:

```
# Mixpanel Credentials
export MX_API_KEY=<INSERT_KEY>
export MX_API_SECRET=<INSERT_SECRET>
export MX_TOKEN=<INSERT_TOKEN>

# Salesforce Credentials (For Mixpanel -> Salesforce)
export SFDC_USER=<SALESFORCE_USERNAME>
export SFDC_PASSWORD=<SALESFORCE_PASSWORD>
export SFDC_TOKEN=<SALESFORCE_SECURITY_TOKEN>
```

Make sure you refresh your bash so they're usable:

`source ~/.bashrc`

Pip install some goodies

`pip install -r requirements.txt`