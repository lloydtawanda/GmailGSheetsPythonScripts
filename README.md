# GmailGSheetsPythonScripts
This repository contains python scripts to perform Gmail and Google Sheets operations.

## Getting Started

### Prerequisites
Install Google API python library, simply use `pip` or `easy_install`:

```bash
pip install --upgrade google-api-python-client
```

or

```bash
easy_install --upgrade google-api-python-client
```
### Running Scripts
How to send email with and attachment:
```bash
python gmail.py --action "send" --from-addr "From Address" --to-addr "To Address" \
--credentials "location of credentials file" --attachment "Location of attachment file"
```
How to read a google sheet:
```bash
python gsheets.py --action "read" --spreadsheet-id "Spreadsheet ID" --range-name "Range Name" \
--credentials "location of credentials file" --subject "Owner gmail account for the spreadsheet"
```







