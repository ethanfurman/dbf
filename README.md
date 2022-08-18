# Python DBF Reader

Forked from user [Ethanfurman](https://github.com/ethanfurman/dbf), with a small patch to increase usability of the program. The patch applies to error handling of certain fields not starting
with the expected character. Instead of throwing the error and stopping, it now throws the error, logs it and continues on with operation. 

This patch allowed me to read in files and get the needed information that I could not before, due to the breaking error handling. 
