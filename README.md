# billing_parser

It parses billing details from data/ folder(*.zip archives) a d store aggregated data into sqlite.
For faster reading of huge files(few GB size) reading was implemented by chunks in threads

TO start:
    python create_db.py
    python main.py
All the configurations could be found in configs.conf file.

TODO:
- investigate issue with sharing queues/ play with using safethread generator
- insert prepared data into SQLite (added initial version. Need to be tested)
- Need  more testing and investigation
- Fix queries(avoid duplicates, insert by chunks, execution in a single connection)

