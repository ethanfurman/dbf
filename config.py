# configuration flags

## Flag for behavior if bad data is encountered in a logical field
## Return None if True, else raise BadDataError
LOGICAL_BAD_IS_NONE = True

## treat non-unicode data as ...
input_decoding = 'utf-8'

## if no codepage specified on dbf creation, use this
default_codepage = 'ascii'

## default format if none specified
default_type = 'db3'

temp_dir = os.environ.get("DBF_TEMP") or os.environ.get("TMP") or os.environ.get("TEMP") or ""


