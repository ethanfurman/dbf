dbf
===

dbf (also known as python dbase) is a module for reading/writing
dBase III, FP, VFP, and Clipper .dbf database files.  It's
an ancient format that still finds lots of use (the most common
I'm aware of is retrieving legacy data so it can be stored in a
newer database system; other uses include GIS, stand-alone programs
such as Family History, Personal Finance, etc.).

Highlights
----------

Table -- represents a single .dbf/.dbt (or .fpt) file combination
and provides access to records; suports the sequence access and 'with'
protocols.  Temporary tables can also live entirely in memory.

Record -- repesents a single record/row in the table, with field access
returning native or custom data types; supports the sequence, mapping,
attribute access (with the field names as the attributes), and 'with'
protocols.  Updates to a record object are reflected on disk either
immediately (using gather() or write()), or at the end of a 'with'
statement.

Index -- nonpersistent index for a table.

Fields::

    dBase III (Null not supported)

        Character --> unicode
        Date      --> datetime.date or None
        Logical   --> bool or None
        Memo      --> unicode or None
        Numeric   --> int/float depending on field definition or None

        Float     --> same as numeric

    Clipper (Null not supported)

        Character --> unicode  (character fields can be up to 65,519)

    Foxpro (Null supported)

        General   --> str (treated as binary)
        Picture   --> str (treated as binary)

    Visual Foxpro (Null supported)

        Currency  --> decimal.Decimal
        douBle    --> float
        Integer   --> int
        dateTime  --> datetime.datetime

    If a field is uninitialized (Date, Logical, Numeric, Memo, General,
    Picture) then None is returned for the value.

Custom data types::

    Null     -->  used to support Null values

    Char     -->  unicode type that auto-trims trailing whitespace, and
                  ignores trailing whitespace for comparisons

    Date     -->  date object that allows for no date

    DateTime -->  datetime object that allows for no datetime

    Time     -->  time object that allows for no time

    Logical  -->  adds Unknown state to bool's: instead of True/False/None,
                  values are Truth, Falsth, and Unknown, with appropriate
                  tri-state logic; just as bool(None) is False, bool(Unknown)
                  is also False;  the numerical values of Falsth, Truth, and
                  Unknown is 0, 1, 2

    Quantum  -->  similar to Logical, but implements boolean algebra (I think).
                  Has states of Off, On, and Other.  Other has no boolean nor
                  numerical value, and attempts to use it as such will raise
                  an exception


Whirlwind Tour
--------------

    import datetime
    import dbf

    # create an in-memory table
    table = dbf.Table(
            filename='test',
            field_specs='name C(25); age N(3,0); birth D; qualified L',
            on_disk=False,
            )
    table.open(dbf.READ_WRITE)

    # add some records to it
    for datum in (
            ('Spanky', 7, dbf.Date.fromymd('20010315'), False),
            ('Spunky', 23, dbf.Date(1989, 7, 23), True),
            ('Sparky', 99, dbf.Date(), dbf.Unknown),
            ):
        table.append(datum)

    # iterate over the table, and print the records
    for record in table:
        print(record)
        print('--------')
        print(record[0:3])
        print([record.name, record.age, record.birth])
        print('--------')

    # make a copy of the test table (structure, not data)
    custom = table.new(
            filename='test_on_disk.dbf',
            default_data_types=dict(C=dbf.Char, D=dbf.Date, L=dbf.Logical),
            )

    # automatically opened and closed
    with custom:
        # copy records from test to custom
        for record in table:
            custom.append(record)
        # modify each record in custom (could have done this in prior step)
        for record in custom:
            dbf.write(record, name=record.name.upper())
            # and print the modified record
            print(record)
            print('--------')
            print(record[0:3])
            print([record.name, record.age, record.birth])
            print('--------')

    table.close()
