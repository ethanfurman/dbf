rmdir build /s/q
rmdir dist /s/q
rmdir html /s/q
python \python27\scripts\epydoc.py dbf.py --output=c:\source\dbf\ancient\dbf\html --show-imports --no-frames
python setup.py sdist bdist --format=wininst %1
