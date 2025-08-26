# CONTRIBUTING

## Backwards compatibility

As of August 2025, `dbf` is still officially compatible with Python 2.7  
(EOL: 2020) and Python 3.6+ (EOL: 2021).

The tooling used for building/packaging requires Python >= 3.7.

All tests succeed for all versions succeed, but for some python versions
deprecation warnings are printed.

## Running unit tests

Simply run:

```bash
python -m dbf.test
```

## Running Tox

[Tox](https://tox.wiki/) is a tool for running builds and tests across many
environments, and for encapsulating other automated checks. For `dbf`, tox is
used to test across Python 2.7 and 3.6–3.13, and to run a test build of a
a wheel (target called `packaging`).

### Set up and activate toxrunner (venv) and install Python packages)

Because of backwards compatibility with Python versions older than 3.7, you
cannot run tox with the typical setup. In particular, it is necessary to use
`virtualenv<20.22`. It is recommended to create a dedicated virtualenv just
for running tox:

```bash
python -m venv toxrunner
source toxrunner/bin/activate
pip install -r requirements-toxrunner.txt
```

### Set up pyenv

By default, virtualenv uses its own mechanism for finding a matching
interpreter. With this approach, tox will skip any interpreter versions
that are not available.

[Pyenv](https://github.com/pyenv/pyenv) is a great tool for installing,
managing, and switching between different Python versions. If you install
pyenv and configure virtualenv to use it as an interpreter discovery backend,
then interpreters will automatically be installed and loaded as needed.

Example setup:

```bash
export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PYENV_ROOT/shims:$PATH"
eval "$(pyenv init -)"
```

To set the backend:

```bash
export VIRTUALENV_DISCOVERY=pyenv
```

These environment variable definitions can be added globally
(e.g. in `~/.bashrc`), inserted into the `activate` script,
or run manually inside the virtualenv before running tox.

### Run tox

To run the build and the unit tests in Python 3.6:

```bash
tox -e py36
```

To run the build/packaging test:

```bash
tox -e packaging
```

To run all environments/targets:

```bash
tox
```
